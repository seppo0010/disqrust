#![cfg_attr(feature = "nightly", feature(catch_panic))]
extern crate disque;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread::{spawn, JoinHandle};
#[cfg(feature = "nightly")] use std::thread::catch_panic;

use disque::Disque;

/// Once a job execution finishes, change its status by performing one of this
/// actions.
#[derive(Clone)]
pub enum JobStatus {
    /// Performs a best effort cluster wide deletion of the job.
    FastAck,
    /// Acknowledges the execution of the jobs.
    AckJob,
    /// Puts back the job in the queue ASAP.
    NAck,
}

/// Handles a job task.
pub trait Handler {
    /// Process a job.
    fn process_job(&self, queue_name: &[u8], jobid: &String, body: Vec<u8>) -> JobStatus;
    /// Decides if a job that failed in the past should be re-executed.
    /// `nack` is the count of negatives acknowledges.
    /// `additional_deliveries` is the number of times the job was processed
    /// but it was not acknowledged.
    fn process_error(&self, _: &[u8], _: &String, _: u32, _: u32) -> bool {
        false
    }
}

/// A wrapper to send the handler to each worker thread.
#[derive(Clone)]
struct HandlerWrapper<H: Handler> {
    handler: Arc<H>,
}
unsafe impl<H: Handler> Send for HandlerWrapper<H> {}
unsafe impl<H: Handler> Sync for HandlerWrapper<H> {}

#[cfg(feature = "nightly")]
macro_rules! spawn {
    ($func: expr, $err: expr) => {
        spawn(move || {
            match catch_panic(move || $func) {
                Ok(_) => (),
                Err(e) => ($err)(e),
            }
        })
    }
}

#[cfg(not(feature = "nightly"))]
macro_rules! spawn {
    ($func: expr, $err: expr) => {
        spawn(move || $func)
    }
}

#[allow(dead_code)]
enum JobUpdate {
    Success(usize, String, JobStatus),
    Failure(usize),
}

/// Creates a worker to handle tasks coming from `task_rx`, reporting them back
/// to `completion_tx` using the provided `handler_`. The `position` is the
/// worker id.
#[allow(unused_variables)]
fn create_worker<H: Handler + Clone + 'static>(position: usize,
        task_rx: Receiver<Option<(Vec<u8>, String, Vec<u8>, u32, u32)>>,
        completion_tx: Sender<JobUpdate>,
        handler_: HandlerWrapper<H>,
        ) -> JoinHandle<()> {
    let handlerw = handler_.clone();
    let completion_tx2 = completion_tx.clone();
    spawn!({
        let handler = handlerw.handler;
        loop {
            let (queue, jobid, job, nack,
                additional_deliveries) = match task_rx.recv() {
                Ok(o) => match o {
                    Some(v) => v,
                    None => break,
                },
                Err(e) => {
                    // TODO: better log
                    println!("Error in worker thread {:?}", e);
                    break;
                }
            };

            if nack > 0 || additional_deliveries > 0 {
                if !handler.process_error(&*queue, &jobid, nack,
                    additional_deliveries) {
                    return;
                }
            }
            let status = handler.process_job(&*queue, &jobid, job);

            completion_tx.send(JobUpdate::Success(position, jobid, status)).unwrap();
        }
    }, |e| {
        println!("handle panic {:?}", e);
        completion_tx2.send(JobUpdate::Failure(position)).unwrap();
    })
}

/// Workers manager.
pub struct EventLoop<H: Handler + Clone + 'static> {
    /// The connection to pull the jobs.
    disque: Disque,
    /// The worker threads and their channels to send jobs.
    workers: Vec<(JoinHandle<()>, Sender<Option<(Vec<u8>, String, Vec<u8>, u32, u32)>>)>,
    /// The receiver when tasks are completed.
    completion_rx: Receiver<JobUpdate>,
    /// The sender for when tasks are completed. Keeping a reference just to
    /// provide to workers.
    completion_tx: Sender<JobUpdate>,
    /// Set of available workers.
    free_workers: HashSet<usize>,
    /// Watched queue names.
    queues: HashSet<Vec<u8>>,
    /// Server network layout.
    hello: (u8, String, Vec<(String, String, u16, u32)>),
    /// Counter for where the tasks are coming from. If most tasks are coming
    /// from a server that is not the one the connection is issued, it can be
    /// used to connect directly to the other one.
    node_counter: HashMap<Vec<u8>, usize>,
    /// The task processor.
    ahandler: HandlerWrapper<H>
}

impl<H: Handler + Clone + 'static> EventLoop<H> {
    pub fn new(
            disque: Disque, numworkers: usize,
            handler: H) -> Self {
        let mut workers = Vec::with_capacity(numworkers);
        let mut free_workers = HashSet::with_capacity(numworkers);
        let (completion_tx, completion_rx) = channel();
        let ahandler = HandlerWrapper { handler: Arc::new(handler) };
        for i in 0..numworkers {
            let (task_tx, task_rx) = channel();
            let jg = create_worker(i, task_rx, completion_tx.clone(),
                    ahandler.clone());
            workers.push((jg, task_tx));
            free_workers.insert(i);
        }
        let hello = disque.hello().unwrap();
        EventLoop {
            disque: disque,
            completion_rx: completion_rx,
            workers: workers,
            hello: hello,
            free_workers: free_workers,
            queues: HashSet::new(),
            node_counter: HashMap::new(),
            completion_tx: completion_tx,
            ahandler: ahandler,
        }
    }

    /// Adds a queue to process its jobs.
    pub fn watch_queue(&mut self, queue_name: Vec<u8>) {
        self.queues.insert(queue_name);
    }

    /// Removes a queue from job processing.
    pub fn unwatch_queue(&mut self, queue_name: &Vec<u8>) {
        self.queues.remove(queue_name);
    }

    /// Marks a job as completed
    fn completed(&mut self, worker: usize, jobid: String, status: JobStatus) {
        self.free_workers.insert(worker);
        match status {
            JobStatus::FastAck => self.disque.fastackjob(jobid.as_bytes()),
            JobStatus::AckJob => self.disque.ackjob(jobid.as_bytes()),
            JobStatus::NAck => self.disque.nackjob(jobid.as_bytes()),
        }.unwrap();
    }

    /// Creates a new worker to replace one that has entered into panic.
    fn handle_worker_panic(&mut self, worker: usize) {
        if self.workers.len() == 0 {
            // shutting down
            return;
        }

        let (task_tx, task_rx) = channel();
        let jg = create_worker(worker, task_rx, self.completion_tx.clone(),
                self.ahandler.clone());
        self.workers[worker] = (jg, task_tx);
        self.free_workers.insert(worker);
    }

    /// Checks workers to see if they have completed their jobs.
    /// If `blocking` it will wait until at least one new is available.
    fn mark_completed(&mut self, blocking: bool) {
        macro_rules! recv {
            ($func: ident) => {
                match self.completion_rx.$func() {
                    Ok(c) => match c {
                        JobUpdate::Success(worker, jobid, status) => {
                            self.completed(worker, jobid, status);
                        },
                        JobUpdate::Failure(worker) => self.handle_worker_panic(worker),
                    },
                    Err(_) => return,
                }
            }
        }

        if blocking {
            recv!(recv);
        }
        loop {
            recv!(try_recv);
        }
    }

    /// Connects to the server that is issuing most of the jobs.
    pub fn choose_favorite_node(&self) -> (Vec<u8>, usize) {
        let mut r = (&Vec::new(), &0);
        for n in self.node_counter.iter() {
            if n.1 >= r.1 {
                r = n;
            }
        }
        (r.0.clone(), r.1.clone())
    }

    /// Number of jobs produced by the current server.
    pub fn jobcount_current_node(&self) -> usize {
        let nodeid = self.hello.1.as_bytes()[0..8].to_vec();
        self.node_counter.get(&nodeid).unwrap_or(&0).clone()
    }

    /// Identifier of the current server.
    pub fn current_node_id(&self) -> String {
        self.hello.1.clone()
    }

    /// Fetches a task and sends it to a worker.
    /// Returns true if a job was received and is processing.
    fn run_once(&mut self) -> bool {
        self.mark_completed(false);
        let worker = match self.free_workers.iter().next() {
            Some(w) => w.clone(),
            None => return false,
        };

        let job = match self.disque.getjob_withcounters(false, None,
                &*self.queues.iter().map(|k| &**k).collect::<Vec<_>>()
                ).unwrap() {
            Some(j) => j,
            None => return false,
        };

        let nodeid = job.1.as_bytes()[2..10].to_vec();
        let v = self.node_counter.remove(&nodeid).unwrap_or(0);
        self.node_counter.insert(nodeid, v + 1);

        self.free_workers.remove(&worker);
        self.workers[worker].1.send(Some(job)).unwrap();
        true
    }

    /// Connects to a new server.
    fn connect_to_node(&mut self, new_master: Vec<u8>) -> bool {
        let mut hello = None;
        for node in self.hello.2.iter() {
            if node.0.as_bytes()[..new_master.len()] == *new_master {
                match Disque::open(&*format!("redis://{}:{}/", node.1, node.2)) {
                    Ok(disque) => {
                        hello = Some(match disque.hello() {
                            Ok(hello) => hello,
                            Err(_) => break,
                        });
                        self.disque = disque;
                        break;
                    },
                    Err(_) => (),
                }
                break;
            }
        }
        match hello {
            Some(h) => { self.hello = h; true }
            None => false,
        }
    }

    /// Connects to the server doing most jobs.
    pub fn do_cycle(&mut self) {
        let (fav_node, fav_count) = self.choose_favorite_node();
        let current_count = self.jobcount_current_node();
        // only change if it is at least 20% better than the current node
        if fav_count as f64 / current_count as f64 > 1.2 {
            self.connect_to_node(fav_node);
        }
    }

    /// Runs for ever. Every `cycle` jobs reevaluates which server to use.
    pub fn run(&mut self, cycle: usize) {
        self.run_times_cycle(0, cycle)
    }

    /// Runs until `times` jobs are received.
    pub fn run_times(&mut self, times: usize) {
        self.run_times_cycle(times, 0)
    }

    /// Runs `times` jobs and changes server every `cycle`.
    pub fn run_times_cycle(&mut self, times: usize, cycle: usize) {
        let mut c = 0;
        let mut counter = 0;
        loop {
            let did_run = self.run_once();
            if did_run {
                if times > 0 {
                    counter += 1;
                    if counter == times {
                        break;
                    }
                }
                if cycle > 0 {
                    c += 1;
                    if c == cycle {
                        self.do_cycle();
                        c = 0;
                    }
                }
            } else {
                self.mark_completed(true);
            }
        }
        self.mark_completed(false);
    }

    /// Sends a kill signal to all workers and waits for them to finish their
    /// current job.
    pub fn stop(mut self) {
        for worker in std::mem::replace(&mut self.workers, vec![]).into_iter() {
            worker.1.send(None).unwrap();
            worker.0.join().unwrap();
        }
        self.mark_completed(false);
    }
}

#[test]
fn favorite() {
    #[derive(Clone)]
    struct MyHandler;
    impl Handler for MyHandler {
        fn process_job(&self, _: &[u8], _: &String, _: Vec<u8>) -> JobStatus {
            JobStatus::AckJob
        }
        fn process_error(&self, _: &[u8], _: &String, _: u32, _: u32) -> bool {
            false
        }
    }
    let disque = Disque::open("redis://127.0.0.1:7711/").unwrap();
    let mut el = EventLoop::new(disque, 1, MyHandler);
    el.node_counter.insert(vec![1, 2, 3], 123);
    el.node_counter.insert(vec![4, 5, 6], 456);
    el.node_counter.insert(vec![0, 0, 0], 0);
    assert_eq!(el.choose_favorite_node(), (vec![4, 5, 6], 456));
}

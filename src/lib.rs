#![feature(iter_cmp)]
extern crate disque;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread::{spawn, JoinHandle};

use disque::Disque;

#[derive(Clone)]
pub enum JobStatus {
    FastAck,
    AckJob,
    NAck,
}

pub trait Handler {
    fn process_job(&self, queue_name: &[u8], jobid: &String, body: Vec<u8>) -> JobStatus;
    fn process_error(&self, queue_name: &[u8], jobid: &String, nack: u32, additional_deliveries: u32) -> bool;
}

#[derive(Clone)]
struct HandlerWrapper<H: Handler> {
    handler: Arc<H>,
}
unsafe impl<H: Handler> Send for HandlerWrapper<H> {}
unsafe impl<H: Handler> Sync for HandlerWrapper<H> {}

fn create_worker<H: Handler + Clone + 'static>(position: usize,
        task_rx: Receiver<Option<(Vec<u8>, String, Vec<u8>, u32, u32)>>,
        completion_tx: Sender<(usize, String, JobStatus)>,
        handler_: HandlerWrapper<H>,
        ) -> JoinHandle<()> {
    let handlerw = handler_.clone();
    spawn(move || {
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

            completion_tx.send((position, jobid, status)).unwrap();
        }
    })
}

pub struct EventLoop {
    disque: Disque,
    workers: Vec<(JoinHandle<()>, Sender<Option<(Vec<u8>, String, Vec<u8>, u32, u32)>>)>,
    completion_rx: Receiver<(usize, String, JobStatus)>,
    free_workers: HashSet<usize>,
    queues: HashSet<Vec<u8>>,
    hello: (u8, String, Vec<(String, String, u16, u32)>),
    node_counter: HashMap<Vec<u8>, usize>,
}

impl EventLoop {
    pub fn new<H: Handler + Clone + 'static>(
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
        }
    }

    pub fn watch_queue(&mut self, queue_name: Vec<u8>) {
        self.queues.insert(queue_name);
    }

    pub fn unwatch_queue(&mut self, queue_name: &Vec<u8>) {
        self.queues.remove(queue_name);
    }

    fn completed(&mut self, worker: usize, jobid: String, status: JobStatus) {
        self.free_workers.insert(worker);
        match status {
            JobStatus::FastAck => self.disque.fastackjob(jobid.as_bytes()),
            JobStatus::AckJob => self.disque.ackjob(jobid.as_bytes()),
            JobStatus::NAck => self.disque.nackjob(jobid.as_bytes()),
        }.unwrap();
    }

    fn mark_completed(&mut self, blocking: bool) -> bool {
        if blocking {
            match self.completion_rx.recv() {
                Ok(c) => self.completed(c.0, c.1, c.2),
                Err(_) => return false,
            }
        }
        loop {
            match self.completion_rx.try_recv() {
                Ok(c) => self.completed(c.0, c.1, c.2),
                Err(_) => break,
            }
        }
        true
    }

    pub fn choose_favorite_node(&self) -> (Vec<u8>, usize) {
        let default = (&Vec::new(), &0);
        let r = self.node_counter.iter().max_by(|node| node.1).unwrap_or(default);
        (r.0.clone(), r.1.clone())
    }

    pub fn jobcount_current_node(&self) -> usize {
        let nodeid = self.hello.1.as_bytes()[0..8].to_vec();
        self.node_counter.get(&nodeid).unwrap_or(&0).clone()
    }

    pub fn current_node_id(&self) -> String {
        self.hello.1.clone()
    }

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

    pub fn do_cycle(&mut self) {
        let (fav_node, fav_count) = self.choose_favorite_node();
        let current_count = self.jobcount_current_node();
        // only change if it is at least 20% better than the current node
        if fav_count as f64 / current_count as f64 > 1.2 {
            self.connect_to_node(fav_node);
        }
    }

    pub fn run(&mut self, cycle: usize) {
        self.run_times_cycle(0, cycle)
    }

    pub fn run_times(&mut self, times: usize) {
        self.run_times_cycle(times, 0)
    }

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

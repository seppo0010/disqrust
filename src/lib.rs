extern crate disque;

use std::collections::HashSet;
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
                additional_deliveries) = match task_rx.recv().unwrap() {
                Some(v) => v,
                None => break,
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
        EventLoop {
            disque: disque,
            completion_rx: completion_rx,
            workers: workers,
            free_workers: free_workers,
            queues: HashSet::new(),
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
            JobStatus::FastAck => self.disque.fastack(&[jobid.as_bytes()]),
            JobStatus::AckJob => self.disque.ackjob(&[jobid.as_bytes()]),
            JobStatus::NAck => self.disque.nack(&[jobid.as_bytes()]),
        }.unwrap();
    }

    fn mark_completed(&mut self) {
        loop {
            match self.completion_rx.try_recv() {
                Ok(c) => self.completed(c.0, c.1, c.2),
                Err(_) => break,
            }
        }
    }

    fn run_once(&mut self) -> bool {
        self.mark_completed();
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
        self.free_workers.remove(&worker);
        self.workers[worker].1.send(Some(job)).unwrap();
        true
    }

    pub fn run(&mut self) {
        loop {
            self.run_once();
        }
    }

    pub fn run_times(&mut self, mut times: usize) {
        while times > 0 {
            times -= 1;
            self.run_once();
        }
    }

    pub fn stop(mut self) {
        for worker in std::mem::replace(&mut self.workers, vec![]).into_iter() {
            worker.1.send(None).unwrap();
            worker.0.join().unwrap();
        }
        self.mark_completed();
    }
}

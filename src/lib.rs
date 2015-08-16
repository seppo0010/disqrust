extern crate disque;

use std::collections::HashSet;
use std::thread::JoinHandle;

use disque::Disque;

pub enum JobStatus {
    FastAck,
    AckJob,
    NAck,
}

pub trait Handler {
    fn process_job(&self, queue_name: &[u8], jobid: String, body: Vec<u8>) -> JobStatus;
    fn process_error(&self, queue_name: &[u8], jobid: String, nack_threshold: u32, additional_deliveries_threshold: u32);
}

pub struct EventLoop<H: Handler> {
    disque: Disque,
    workers: Vec<Option<JoinHandle<()>>>,
    handler: H,
    queues: HashSet<Vec<u8>>,
}

impl<H: Handler> EventLoop<H> {
    pub fn new(disque: Disque, numworkers: usize, handler: H) -> Self {
        let mut workers = Vec::with_capacity(numworkers);
        for _ in 0..numworkers { workers.push(None); }
        EventLoop {
            disque: disque,
            workers: workers,
            handler: handler,
            queues: HashSet::new(),
        }
    }

    pub fn watch_queue(&mut self, queue_name: Vec<u8>) {
        self.queues.insert(queue_name);
    }

    pub fn unwatch_queue(&mut self, queue_name: &Vec<u8>) {
        self.queues.remove(queue_name);
    }

    fn run_once(&self) {
        let (queue, jobid, job, nack, additional_deliveries
                ) = self.disque.getjob_withcounters(
                    false, None, &*self.queues.iter().map(|k| &**k).collect::<Vec<_>>()).unwrap().unwrap();
        self.handler.process_job(&*queue, jobid, job);
    }

    pub fn run(&self) {
        loop {
            self.run_once();
        }
    }

    pub fn run_times(&self, mut times: usize) {
        while times > 0 {
            times -= 1;
            self.run_once();
        }
    }
}

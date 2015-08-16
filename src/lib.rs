extern crate disque;

use std::collections::HashMap;
use std::thread::JoinHandle;

use disque::Disque;

pub enum JobStatus {
    FastAck,
    AckJob,
    NAck,
}

pub trait JobHandler {
    fn process_job(&self, queue_name: &[u8], jobid: String, body: Vec<u8>) -> JobStatus;
}

pub trait ErrorHandler {
    fn process_error(&self, queue_name: &[u8], jobid: String, nack_threshold: u32, additional_deliveries_threshold: u32);
}

pub struct EventLoop<J: JobHandler, E: ErrorHandler> {
    disque: Disque,
    workers: Vec<Option<JoinHandle<()>>>,
    job_handler: HashMap<Vec<u8>, J>,
    error_handler: HashMap<Vec<u8>, (u32, u32, E)>,
}

impl<J: JobHandler, E: ErrorHandler> EventLoop<J, E> {
    pub fn new(disque: Disque, numworkers: usize) -> Self {
        let mut workers = Vec::with_capacity(numworkers);
        for _ in 0..numworkers { workers.push(None); }
        EventLoop {
            disque: disque,
            workers: workers,
            job_handler: HashMap::new(),
            error_handler: HashMap::new(),
        }
    }
    pub fn register_job_handler(&mut self, queue_name: Vec<u8>, job_handler: J) {
        self.job_handler.insert(queue_name, job_handler);
    }

    pub fn register_error_handler(&mut self, queue_name: Vec<u8>, nack_threshold: u32, additional_deliveries_threshold: u32, error_handler: E) {
        self.error_handler.insert(queue_name, (nack_threshold, additional_deliveries_threshold, error_handler));
    }

    fn run_once(&self) {
        let (queue, jobid, job, nack, additional_deliveries
                ) = self.disque.getjob_withcounters(
                    false, None, &*self.job_handler.keys().map(|k| &**k).collect::<Vec<_>>()).unwrap().unwrap();
        self.job_handler.get(&queue).unwrap().process_job(&*queue, jobid, job);
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

extern crate disqrust;
extern crate disque;

use std::time::Duration;
use std::sync::mpsc::{channel, Sender};

use disque::Disque;
use disqrust::{EventLoop, Handler, JobStatus};

enum HandlerCall {
    Job(Vec<u8>, String, Vec<u8>),
    Error(Vec<u8>, String, u32, u32),
}

impl HandlerCall {
    fn body(&self) -> Vec<u8> {
        match *self {
            HandlerCall::Job(_, _, ref body) => body.clone(),
            HandlerCall::Error(_, _, _, _) => panic!("getting body for error"),
        }
    }

    fn nack_additional_deliveries(&self) -> (u32, u32) {
        match *self {
            HandlerCall::Job(_, _, _) => panic!("getting nack for job"),
            HandlerCall::Error(_, _, a, b) => (a, b),
        }
    }
}

#[derive(Clone)]
struct MyHandler {
    sender: Sender<HandlerCall>,
    process_job_ret: JobStatus,
    process_error_ret: bool,
}

impl MyHandler {
    fn new(sender: Sender<HandlerCall>, process_job_ret: JobStatus,
            process_error_ret: bool) -> MyHandler {
        MyHandler {
            sender: sender,
            process_job_ret: process_job_ret,
            process_error_ret: process_error_ret,
        }
    }
}

impl Handler for MyHandler {
    fn process_job(&self, queue_name: &[u8], jobid: &String, body: Vec<u8>
            ) -> JobStatus {
        self.sender.send(HandlerCall::Job(queue_name.to_vec(), jobid.clone(),
                    body)).unwrap();
        self.process_job_ret.clone()
    }

    fn process_error(&self, queue_name: &[u8], jobid: &String, nack: u32,
            additional_deliveries: u32) -> bool {
        self.sender.send(HandlerCall::Error(queue_name.to_vec(), jobid.clone(),
                    nack, additional_deliveries)).unwrap();
        self.process_error_ret
    }
}


#[test]
fn basic() {
    let queue = b"basicqueue";
    let disque = Disque::open("redis://127.0.0.1:7711/").unwrap();
    disque.addjob(queue, b"my job", Duration::from_secs(10), None, None, None,
            None, None, false).unwrap();
    let (tx, rx) = channel();
    let mut el = EventLoop::new(disque, 1, MyHandler::new(tx,
                JobStatus::AckJob, true));
    el.watch_queue(queue.to_vec());
    el.run_times(1);
    el.stop();
    assert_eq!(rx.try_recv().unwrap().body(), b"my job".to_vec());
    assert!(rx.try_recv().is_err());
}

#[test]
fn error() {
    let queue = b"errorqueue";
    let disque = Disque::open("redis://127.0.0.1:7711/").unwrap();
    let jobid = disque.addjob(queue, b"my job", Duration::from_secs(10), None,
            None, None, None, None, false).unwrap();
    disque.getjob(true, None, &[queue]).unwrap();
    disque.nack(&[jobid.as_bytes()]).unwrap();

    let (tx, rx) = channel();
    let mut el = EventLoop::new(disque, 1, MyHandler::new(tx,
                JobStatus::AckJob, false));
    el.watch_queue(queue.to_vec());
    el.run_times(1);
    el.stop();
    assert_eq!(rx.try_recv().unwrap().nack_additional_deliveries(), (1, 0));
    assert!(rx.try_recv().is_err());
}

#[test]
fn error_and_job() {
    let queue = b"errorjobqueue";
    let disque = Disque::open("redis://127.0.0.1:7711/").unwrap();
    let jobid = disque.addjob(queue, b"my job", Duration::from_secs(10), None,
            None, None, None, None, false).unwrap();
    disque.getjob(true, None, &[queue]).unwrap();
    disque.nack(&[jobid.as_bytes()]).unwrap();

    let (tx, rx) = channel();
    let mut el = EventLoop::new(disque, 1, MyHandler::new(tx,
                JobStatus::AckJob, true));
    el.watch_queue(queue.to_vec());
    el.run_times(1);
    el.stop();
    assert_eq!(rx.try_recv().unwrap().nack_additional_deliveries(), (1, 0));
    assert_eq!(rx.try_recv().unwrap().body(), b"my job");
    assert!(rx.try_recv().is_err());
}

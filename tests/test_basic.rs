extern crate disqrust;
extern crate disque;
extern crate redis;

use std::time::Duration;
use std::sync::mpsc::{channel, Sender};

use disque::Disque;
use disqrust::{EventLoop, Handler, JobStatus};
use redis::Value;

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

#[cfg(test)]
fn create_job(queue: &[u8], job: &[u8], nack: bool
        ) -> (Disque, Vec<u8>, Vec<u8>, String) {
    let disque = Disque::open("redis://127.0.0.1:7711/").unwrap();
    let jobid = disque.addjob(queue, job, Duration::from_secs(10), None,
            None, None, None, None, false).unwrap();
    if nack {
        disque.getjob(true, None, &[queue]).unwrap();
        disque.nackjob(jobid.as_bytes()).unwrap();
    }
    (disque, queue.to_vec(), job.to_vec(), jobid)
}

#[test]
fn basic() {
    let (disque, queue, job, _) = create_job(b"basic", b"job67", false);

    let (tx, rx) = channel();
    let handler = MyHandler::new(tx, JobStatus::AckJob, true);
    let mut el = EventLoop::new(disque, 1, handler);
    el.watch_queue(queue.to_vec());
    el.run_times(1);
    el.stop();
    assert_eq!(rx.try_recv().unwrap().body(), job);
    assert!(rx.try_recv().is_err());
}

#[test]
fn error() {
    let (disque, queue, _, _) = create_job(b"error", b"job456", true);

    let (tx, rx) = channel();
    let handler = MyHandler::new(tx, JobStatus::AckJob, false);
    let mut el = EventLoop::new(disque, 1, handler);
    el.watch_queue(queue.to_vec());
    el.run_times(1);
    el.stop();
    assert_eq!(rx.try_recv().unwrap().nack_additional_deliveries(), (1, 0));
    assert!(rx.try_recv().is_err());
}

#[test]
fn error_and_job() {
    let (disque, queue, job, _) = create_job(b"errorandjob", b"job123", true);

    let (tx, rx) = channel();
    let handler = MyHandler::new(tx, JobStatus::AckJob, true);
    let mut el = EventLoop::new(disque, 1, handler);
    el.watch_queue(queue.to_vec());
    el.run_times(1);
    el.stop();
    assert_eq!(rx.try_recv().unwrap().nack_additional_deliveries(), (1, 0));
    assert_eq!(rx.try_recv().unwrap().body(), job);
    assert!(rx.try_recv().is_err());
}

#[test]
fn fastack() {
    let (disque, queue, _, jobid) = create_job(b"fastack", b"job123", false);

    assert!(disque.show(jobid.as_bytes()).unwrap().is_some());

    let (tx, rx) = channel();
    let handler = MyHandler::new(tx, JobStatus::FastAck, true);
    let mut el = EventLoop::new(disque, 1, handler);
    el.watch_queue(queue.to_vec());
    el.run_times(1);
    el.stop();
    rx.try_recv().unwrap();

    let disque = Disque::open("redis://127.0.0.1:7711/").unwrap();
    assert!(disque.show(jobid.as_bytes()).unwrap().is_none());
}

#[test]
fn ackjob() {
    let (disque, queue, _, jobid) = create_job(b"ackjob", b"job123", false);

    assert!(disque.show(jobid.as_bytes()).unwrap().is_some());

    let (tx, rx) = channel();
    let handler = MyHandler::new(tx, JobStatus::AckJob, true);
    let mut el = EventLoop::new(disque, 1, handler);
    el.watch_queue(queue.to_vec());
    el.run_times(1);
    el.stop();
    rx.try_recv().unwrap();

    let disque = Disque::open("redis://127.0.0.1:7711/").unwrap();
    assert!(disque.show(jobid.as_bytes()).unwrap().is_none());
}

#[test]
fn nack() {
    let (disque, queue, _, jobid) = create_job(b"nack", b"job000", false);

    let (tx, rx) = channel();
    let handler = MyHandler::new(tx, JobStatus::NAck, true);
    let mut el = EventLoop::new(disque, 1, handler);
    el.watch_queue(queue.to_vec());

    el.run_times(3);
    el.stop();
    rx.try_recv().unwrap();
    rx.try_recv().unwrap();
    rx.try_recv().unwrap();

    let disque = Disque::open("redis://127.0.0.1:7711/").unwrap();
    assert_eq!(*disque.show(jobid.as_bytes()).unwrap().unwrap().get(
                "nacks").unwrap(),
            Value::Int(3));
    disque.ackjob(jobid.as_bytes()).unwrap();
}

#[test]
fn jobcount_current_node() {
    let (disque, queue, _, _) = create_job(b"ackjob", b"job123", false);

    let (tx, rx) = channel();
    let handler = MyHandler::new(tx, JobStatus::AckJob, true);
    let mut el = EventLoop::new(disque, 1, handler);
    el.watch_queue(queue.to_vec());
    el.run_times(1);

    assert!(el.jobcount_current_node() >= 1);
    el.stop();
    rx.try_recv().unwrap();
}

#[test]
fn change_servers() {
    let disque = Disque::open("redis://127.0.0.1:7711/").unwrap();
    // This test does not apply when there is only one server
    let hello = disque.hello().unwrap();
    if hello.2.len() == 1 {
        return;
    }
    let disque2 = Disque::open(&*format!("redis://{}:{}/",
                hello.2[1].1, hello.2[1].2)).unwrap();

    let (tx, rx) = channel();

    let handler = MyHandler::new(tx, JobStatus::AckJob, true);
    let mut el = EventLoop::new(disque2, 1, handler);
    let oldid = el.current_node_id();
    let queue = b"change_servers";
    let job = b"job";

    el.watch_queue(queue.to_vec());

    let att = 20;
    for _ in 0..att {
        disque.addjob(queue, job, Duration::from_secs(10), None,
                None, None, None, None, false).unwrap();
        el.run_times(1);
        el.do_cycle();
        let newid = el.current_node_id();
        if newid != oldid {
            rx.try_recv().unwrap();
            return;
        }
    }

    panic!("After {} attempts it did not change node", att);
}

extern crate disqrust;
extern crate disque;

use std::time::Duration;
use std::sync::mpsc::{channel, Sender};

use disque::Disque;
use disqrust::{EventLoop, Handler, JobStatus};

#[test]
fn basic() {
    struct MyHandler {
        sender: Sender<Vec<u8>>,
    }

    impl MyHandler {
        fn new(sender: Sender<Vec<u8>>) -> MyHandler {
            MyHandler { sender: sender }
        }
    }

    impl Handler for MyHandler {
        fn process_job(&self, _: &[u8], _: &String, body: Vec<u8>) -> JobStatus {
            self.sender.send(body).unwrap();
            JobStatus::AckJob
        }
        fn process_error(&self, _: &[u8], _: &String, _: u32, _: u32) -> bool {
            panic!("Should not be called")
        }
    }

    let disque = Disque::open("redis://127.0.0.1:7711/").unwrap();
    disque.addjob(b"basicqueue", b"my job", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    let (tx, rx) = channel();
    let mut el = EventLoop::new(disque, 1, MyHandler::new(tx));
    el.watch_queue(b"basicqueue".to_vec());
    el.run_times(1);
    assert_eq!(rx.try_recv().unwrap(), b"my job".to_vec());
}

#[test]
fn error() {
    struct MyHandler {
        sender: Sender<(u32, u32)>,
    }

    impl MyHandler {
        fn new(sender: Sender<(u32, u32)>) -> MyHandler {
            MyHandler { sender: sender }
        }
    }

    impl Handler for MyHandler {
        fn process_job(&self, _: &[u8], _: &String, _: Vec<u8>) -> JobStatus {
            panic!("Should not be called")
        }
        fn process_error(&self, _: &[u8], _: &String, nack: u32, additional_deliveries: u32) -> bool {
            self.sender.send((nack, additional_deliveries)).unwrap();
            false
        }
    }

    let disque = Disque::open("redis://127.0.0.1:7711/").unwrap();
    let jobid = disque.addjob(b"errorqueue", b"my job", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    disque.getjob(true, None, &[b"errorqueue"]).unwrap();
    disque.nack(&[jobid.as_bytes()]).unwrap();

    let (tx, rx) = channel();
    let mut el = EventLoop::new(disque, 1, MyHandler::new(tx));
    el.watch_queue(b"errorqueue".to_vec());
    el.run_times(1);
    assert_eq!(rx.try_recv().unwrap(), (1, 0));
}

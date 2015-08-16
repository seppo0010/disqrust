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
        fn process_job(&self, queue_name: &[u8], jobid: String, body: Vec<u8>) -> JobStatus {
            self.sender.send(body);
            JobStatus::AckJob
        }
        fn process_error(&self, queue_name: &[u8], jobid: String, nack_threshold: u32, additional_deliveries_threshold: u32) {
        }
    }

    let disque = Disque::open("redis://127.0.0.1:7711/").unwrap();
    let jobid = disque.addjob(b"basicqueue", b"my job", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    let (tx, rx) = channel();
    let mut el = EventLoop::new(disque, 1, MyHandler::new(tx));
    el.watch_queue(b"basicqueue".to_vec());
    el.run_times(1);
    assert_eq!(rx.recv().unwrap(), b"my job".to_vec());
}

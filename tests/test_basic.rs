extern crate disqrust;
extern crate disque;

use std::time::Duration;
use std::sync::mpsc::{channel, Sender};

use disque::Disque;
use disqrust::{EventLoop, JobHandler, ErrorHandler, JobStatus};

#[test]
fn basic() {
    struct MyJobHandler {
        sender: Sender<Vec<u8>>,
    }
    impl MyJobHandler {
        fn new(sender: Sender<Vec<u8>>) -> MyJobHandler {
            MyJobHandler { sender: sender }
        }
    }
    impl JobHandler for MyJobHandler {
        fn process_job(&self, queue_name: &[u8], jobid: String, body: Vec<u8>) -> JobStatus {
            self.sender.send(body);
            JobStatus::AckJob
        }
    }

    struct MyErrorHandler;
    impl ErrorHandler for MyErrorHandler {
        fn process_error(&self, queue_name: &[u8], jobid: String, nack_threshold: u32, additional_deliveries_threshold: u32) {}
    }

    let disque = Disque::open("redis://127.0.0.1:7711/").unwrap();
    let jobid = disque.addjob(b"basicqueue", b"my job", Duration::from_secs(10), None, None, None, None, None, false).unwrap();
    let mut el = EventLoop::<_, MyErrorHandler>::new(disque, 1);
    let (tx, rx) = channel();
    el.register_job_handler(b"basicqueue".to_vec(), MyJobHandler::new(tx));
    el.run_times(1);
    assert_eq!(rx.recv().unwrap(), b"my job".to_vec());
}

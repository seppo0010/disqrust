# disqrust

[![Build Status](https://travis-ci.org/seppo0010/disqrust.svg?branch=master)](https://travis-ci.org/seppo0010/disqrust)
[![crates.io](http://meritbadge.herokuapp.com/disqrust)](https://crates.io/crates/disqrust)

A high-level library to implement Disque workers.

The crate is called `disqrust` and you can depend on it via cargo:

```toml
[dependencies]
disqrust = "0.1.0"
```

It currently requires Rust Beta or Nightly.

## Basic Operation

```rust,no_run
extern crate disque;
extern crate disqrust;

use disque::Disque;
use disqrust::{EventLoop, Handler, JobStatus};

#[derive(Clone)]
struct MyHandler(u8);

impl Handler for MyHandler {
    fn process_job(&self, queue_name: &[u8], jobid: &String, body: Vec<u8>) -> JobStatus {
        match queue_name {
            b"send email" => { /* send email */; JobStatus::AckJob },
            _ => JobStatus::NAck,
        }
    }
}

fn main() {
    let disque = Disque::open("redis://127.0.0.1:7711/").unwrap();
    let mut el = EventLoop::new(disque, 4, MyHandler(0));
    el.watch_queue(b"my queue".to_vec());
    el.run(1000);
}
```

## Documentation

For a more comprehensive documentation with all the available functions and
parameters go to http://seppo0010.github.io/disqrust/

For a complete reference on Disque, check out https://github.com/antirez/disque

use std::sync::mpsc::{Sender, channel, Receiver};
use std::thread::{self, JoinHandle, Thread};
use std::sync::{mpsc, Arc, Mutex};

pub struct ThreadPoll{
    workers:Vec<Worker>,
    sender:Option<mpsc::Sender<Job>>,
    capacity:usize,
}

impl ThreadPoll {
    pub fn new(size: usize) -> ThreadPoll{
        assert!(size > 0);
        let mut workers = Vec::with_capacity(size);
        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));
        for id in 0..size{
            // 创建一些不执行的线程并且存储在数组中
            let _worker = Worker::new(id, Arc::clone(&receiver));
            workers.push(_worker);
        }
        ThreadPoll{
            workers,
            sender: Some(sender),
            capacity:size,
        }
    }

    pub fn execute<F>(&self, f:F)
        where F:FnOnce() + Send + 'static,
    {
        let job : Job = Box::new(f);
        self.sender.as_ref().unwrap().send(job).unwrap();
    }
}

// 停机用.
impl Drop for ThreadPoll{
    fn drop(&mut self) {
        // 先清理掉threadpoll自己的sender，让worker从recv的等待中退出(此时recv会返回错误.)
        drop(self.sender.take());
        for worker in &mut self.workers{
            //print!("Shutting down worker {}", worker.id);
            if let Some(thread) = worker.joinable.take(){
                thread.join().unwrap();
            }
        }
    }
}

type Job = Box<dyn FnOnce() + Send + 'static>;

pub struct Worker{
    id:usize,
    joinable:Option<JoinHandle<()>>,
}

impl Worker {
    pub fn new(id:usize, receiver:Arc<Mutex<mpsc::Receiver<Job>>>)->Worker{
        let _thread = thread::spawn(move ||loop {
            let message = receiver.lock().unwrap().recv();
            match message {
                Ok(job) => {
                    //print!("Worker {id} gets a job. Executing.");
                    job();
                    //print!("Worker {id} completes the job.");
                }
                Err(_) => {
                    //print!("Worker {id} disconnected. Shutting down.");
                    break;
                }
            }
            
        });
        Worker { id: (id), joinable: Some(_thread) }
    }
}
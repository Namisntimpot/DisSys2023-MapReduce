#![allow(unused)]

pub mod map_reduce_server;
pub mod map_reduce;
mod thread_poll;
mod io_wrapper;
pub mod map_reduce_client;
pub mod error;

use map_reduce_server::MapReduceServer;

/// 运行一个mapreduce server host 为地址号.
pub fn run_server(host : &str, master_num : usize, worker_num : usize) {
    println!("Establish and run a server for mapreduce at {}", host);
    let mut server = MapReduceServer::new(
        host,
        master_num,
        worker_num,
    );
    server.run();
}

#![allow(unused)]
#![allow(non_snake_case)]

mod map_reduce_server;
mod map_reduce;
mod thread_poll;
mod io_wrapper;
mod map_reduce_client;
mod error;

use std::env;

/// 运行它的时候需要通过命令行参数指定hdfs客户端的host，以及用户名称. \
/// cargo run `hdfs_clent_host` `username`
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args : Vec<String> = env::args().collect();
    let hdfs_client_host = &args[1];
    let username = &args[2];
    map_reduce::SETUP_GLOBAL_HDFS_CLIENT(hdfs_client_host, username)?;
    let mut server = map_reduce_server::MapReduceServer::new(
        "127.0.0.1:7878", 1, 3);
    server.run();
    Ok(())
}
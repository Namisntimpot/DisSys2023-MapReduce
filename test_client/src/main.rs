use MapReduce::map_reduce_client;
use MapReduce::map_reduce::SETUP_GLOBAL_HDFS_CLIENT;
use std::env;

/// 在target/debug目录(exe存在的那个目录)执行 `cargo run <hdfs_client_host> <username>`
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args : Vec<String> = env::args().collect();
    let hdfs_client_host = &args[1];
    let username = &args[2];
    SETUP_GLOBAL_HDFS_CLIENT(hdfs_client_host, username)?;
    let mut client = map_reduce_client::Client::new(
        "./input/article.txt",
        "./libusemapreduce.so",
        "127.0.0.1:7878",
        "./ret",
        4,
        2
    )?;
    println!("MapReduce Client established.");
    client.execute()?;
    Ok(())
}

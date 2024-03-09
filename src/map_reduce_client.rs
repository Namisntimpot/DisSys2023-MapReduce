use libloading::{Symbol, Library, Error as LibloadingError};
use thiserror::Error;
use std::fmt::format;
use std::io::{Write, Read};
use std::{
    net::TcpStream,
    collections::HashMap,
};

use crate::map_reduce::MessagePacket;
use crate::io_wrapper::*;
use crate::error::MapReduceError;

pub struct Client{
    origin_input_file : String,  // 原始的输入文件，将要被分块成多个(mapper_num)个.
    result_dir : String,    // 用户指定的要把结果文件放在这个文件夹.
    dll_path : String,   // .dll的绝对路径.
    server_host : String,   // server的地址
    input_dir : Option<String>,   // server返回的放置输入文件的文件夹.
    result_files : Option<String>,  // 用|分隔的多个结果文件的绝对路径.
    task_id : u32,    // server分配的task_id
    m : u32,
    n : u32,
}


impl Client{
    /// 新建一个Client.  \
    /// origin_input_file : 原始输入文件的路径，单个文件；它将被分块成. \
    /// dll_path: dll的路径，一般和用户crate名字和toml里的设置有关 \
    /// result_dir : 制定一个输出文件夹，所有n个输出文件都会被放到result_dir中, 它可以没被创建. \
    /// m: mapper数量 \
    /// n: reducer数量 \
    pub fn new(
        origin_input_file : &str, 
        dll_path : &str, server_host : &str, result_dir : &str, 
        m:u32, n:u32)-> Result<Client, MapReduceError>{
        let ret_dir = result_dir.to_string();
        if !iowrapper_exist(&ret_dir) {
            iowrapper_create_dir(&ret_dir)?;
        }
        Ok(Client {
            origin_input_file: iowrapper_get_absolute_path(&origin_input_file.to_string())?,
            result_dir: iowrapper_get_absolute_path(&ret_dir)?, 
            dll_path: iowrapper_get_absolute_path(&dll_path.to_string())?,
            server_host : server_host.to_string(),
            input_dir: None, 
            result_files: None, 
            task_id: 0, 
            m, n})
    }

    /// 执行这个mapreduce任务
    pub fn execute(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // 提前测试一下是否可以链接.
        self.test_mapper_reducer_loadable()?;
        println!("Testing if mapper and reducer loadable......done.");

        // 申请任务
        let apply_for_task = format!(
            r#"{{
                "message_type":1,
                "mapper_num":{},
                "reducer_num":{}
            }}"#, self.m, self.n
        );
        println!("Connecting to MapReduce server...");
        let mut stream = TcpStream::connect(&self.server_host)?;
        println!("Applying for a MapReduce task...");
        stream.write_all(apply_for_task.as_bytes())?;
        
        
        let mut task_info = [0;1024];
        let length = stream.read(&mut task_info)?;
        let mut task_info : MessagePacket = serde_json::from_slice(&task_info[0..length])?;
        //如果返回的message_type不对，就结束.
        if task_info.message_type != 4 {
            return Err(Box::new(MapReduceError::WrongMessageType));
        }
        // 记录用于识别自己的task_id!!
        self.task_id = task_info.task_id;

        println!("The task id is: {}", task_info.task_id);

        // 接下来，把文件复制到指定的地方，并且结束回复消息称自己已经完成.
        println!("Copying dynamic linked library...");
        iowrapper_copy_file(&self.dll_path, &task_info.dll_file)?;
        println!("Clipping and copying input file...");
        iowrapper_file_blocking(
            &self.origin_input_file, 
            &task_info.data_file, self.m
        )?;
        let prepared_message = String::from(
            format!("{{\"message_type\":2,\"task_id\":{}}}",self.task_id)
        );
        // server中，刚Apply用的(message_type==1)tcpstream会drop掉，所以应该重新连接.
        let mut stream = TcpStream::connect(&self.server_host)?;
        stream.write_all(prepared_message.as_bytes())?;

        // 等候server发来结果通知.
        println!("Waitting for results...");
        let mut result_packet = [0;1024];
        let length = stream.read(&mut result_packet)?;
        let result_packet:MessagePacket = serde_json::from_slice(&result_packet[0..length])?;

        if result_packet.message_type != 5 {
            return Err(Box::new(MapReduceError::WrongMessageType));
        }

        if result_packet.data_file.is_empty() {
            eprintln!("Task Failed. {}", result_packet.dll_file);
            return Err(Box::new(MapReduceError::TaskFailed));
        }

        println!("Task done. Fetching result files.");

        let ret_files:Vec<&str> = result_packet.data_file.split('|').collect();
        // 把结果复制到目标文件夹.
        for file_whole_path in ret_files {
            let file_whole_path = file_whole_path.to_string();
            let filename = iowrapper_get_filename(&file_whole_path)?;
            let target_path = path_join(&self.result_dir, &filename);
            iowrapper_copy_file(&file_whole_path, &target_path)?;
        }

        // 复制完毕，通知server任务结束，可以清除任务, 通信类型是3.
        let copied_message = String::from(
            format!("{{\"message_type\":3,\"task_id\":{}}}", self.task_id)
        );

        // 同样，那边通知完之后直接drop了之前的stream，所以需要重新连接
        let mut stream = TcpStream::connect(&self.server_host)?;
        stream.write_all(copied_message.as_bytes())?;
        // 这里写了之后如果立即退出, 

        println!("All MapReduce task completed.");

        Ok(())
    }

    fn test_mapper_reducer_loadable(&self) -> Result<(),Box<dyn std::error::Error>> {
        unsafe{
            let lib = Library::new(&self.dll_path)?;
            let mapper : Result<Symbol<fn(&String)->HashMap<String,Vec<String>>>, LibloadingError>
                = lib.get(b"mapper");
            match mapper{
                Err(_) => {
                    return Err(Box::new(MapReduceError::DllLoadingError {
                        fntype : String::from("mapper")
                    }));
                }
                _ => {}
            };
            let reducer: Result<Symbol<fn(&String, &Vec<String>)->Vec<String>>, LibloadingError>
                = lib.get(b"reducer");
            match reducer{
                Err(_) => {
                    return Err(Box::new(MapReduceError::DllLoadingError {
                        fntype : String::from("reducer")
                    }));
                }
                _ => {}
            };
        }
        Ok(())
    }
}
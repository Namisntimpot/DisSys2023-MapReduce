mod masters;
mod workers;

enum Status{
    Waiting,
    Executing,
    Completed,
    Error,
}

use std::{
    fs,
    io::{prelude::*, BufReader, read_to_string},
    net::{TcpListener, TcpStream},
    thread::{self, Thread},
    sync::{Arc, Mutex},
    time::Duration,
    collections::HashMap, hash::Hash,
};
use serde_json::map::Entry;

use crate::{thread_poll::ThreadPoll, io_wrapper::{iowrapper_create_dir, iowrapper_get_absolute_path, path_join, iowrapper_exist, iowrapper_remove_dir_all, HdfsSetting, iowrapper_read_dir_into_strings, iowrapper_copy_file, iowrapper_get_filename}};
use crate::map_reduce::MessagePacket;
use crate::map_reduce_server::masters::Master;
use crate::error::MapReduceError;


pub struct MapReduceServer{
    host : String,
    listener : TcpListener,
    master_poll : ThreadPoll,
    worker_poll : Arc<Mutex<ThreadPoll>>,  // 共享所有权..
    task_id_count : u32,     // 累增计数，用来分配task_id.
    task_map : HashMap<u32, TaskEntry>,  // 用Hashmap实现id到task的O(1)访问.
}

struct TaskEntry{
    pub task_id : u32,
    pub hdfs_base_dir : String,
    pub task_base_dir : String,  // 该任务数据文件所在的基本目录
    pub input_dir : String,    // 输入文件所在的dir
    pub dll_path : String,     // 该任务的dllpath所在的路径，这三个都是server分配的.
    pub result_path : Option<String>,  // 所有结果文件的路径, 用|分隔. 最开始可能没有.
    pub mapper_num : u32,
    pub reducer_num : u32,
    pub status : Status,
    pub stream : Option<TcpStream>,  // 用来保存与Client对话用的tcpstream的,可能变更.
    // 在收到master报告任务完毕之后，也会暂存master的stream直到这里.
}

impl MapReduceServer {
    pub fn new(host:&str, master_num:usize, worker_num:usize) -> MapReduceServer{
        let listener = TcpListener::bind(host).unwrap(); // 不处理错误.
        let worker_poll = ThreadPoll::new(worker_num);
        let master_poll = ThreadPoll::new(master_num);
        println!("MapReduce server with {} masters and {} workers at {}",
                    master_num, worker_num, host);
        MapReduceServer { 
            host : String::from(host),
            listener: (listener),
            master_poll,
            worker_poll : Arc::new(Mutex::new(worker_poll)),
            task_id_count : 0,
            task_map : HashMap::new(),
        }
    }

    pub fn run(&mut self) -> Result<(), Box<dyn std::error::Error>>{
        // 注意，下面这个for .. in self.listener借用了self中的listener字段;
        // 而后面的handle_... 借用了整个self，从签名来看，包括里面的listener(虽然实际上没有)
        // 而编译器单独编译各个函数，只看函数签名，所以“发现”了这个重复引用.
        // 所以使用listener.accept避开listener.incomming的对self的长期引用.
        loop {
            let stream:Option<TcpStream> = match self.listener.accept() {
                Ok((_socket, addr)) => Some(_socket),
                Err(e) => {
                    println!("Couldn't get client {e:?}");
                    None
                }
            };
            if let Some(mut stream) = stream{
                // read_to_string 要一直读到eof... 不知道怎么发送EOF，所以用read+定长buf吧...
                let mut buf = [0;1024];
                let length = stream.read(&mut buf);
                if length.is_err(){
                    eprintln!("{}", length.err().unwrap());
                    continue;
                }
                let length = length.unwrap();
                let packet = serde_json::from_slice::<MessagePacket>(&buf[0..length]);
                match packet {
                    Ok(packet) => {
                        // 判断packet类型 用数组的方式存函数可能更聪明..
                        if packet.message_type == 1 {
                            if let Err(e) = self.handle_type_1_client_applying(stream, packet){
                                eprintln!("{}",e);
                            }
                        }
                        else if packet.message_type == 2 {
                            if let Err(e) = self.handle_type_2_client_prepared(stream, packet){
                                eprintln!("{}",e);
                            }
                        }
                        else if packet.message_type == 3 {
                            if let Err(e) = self.handle_type_3_client_copied(stream, packet){
                                eprintln!("{}", e);
                            }
                        }
                        else if packet.message_type == 7 {
                            if let Err(e) = self.handle_type_7_master_completed(stream, packet){
                                eprintln!("{}", e);
                            }
                        }
                        else if packet.message_type == 8 {
                            if let Err(e) = self.handle_type_8_master_report_failed(stream, packet){
                                eprintln!("{}", e);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("{}",e);
                    }
                }
                
            }
        }
    }

    /// 处理第1种：Client申请一个任务.
    fn handle_type_1_client_applying(&mut self, mut stream:TcpStream, packet:MessagePacket) 
        -> Result<(), Box<dyn std::error::Error>>{
        // 这个包里只有m, n有用
        let task_id = self.task_id_count;  // 分配一个任务编号.
        self.task_id_count+=1;

        // hdfs中这个任务的base_dir.如果存在就删了重建；不存在就创建
        let mut hdfs_base_dir = HdfsSetting::path_head().to_string();
        hdfs_base_dir.push_str(MapReduceServer::hdfs_root_dir());
        let hdfs_base_dir = path_join(&hdfs_base_dir, &format!("{}", task_id));
        if iowrapper_exist(&hdfs_base_dir) {
            iowrapper_remove_dir_all(&hdfs_base_dir)?;
        }
        iowrapper_create_dir(&hdfs_base_dir)?;

        // 创建这个任务用的文件夹(base文件夹)
        let base_dir = format!("./{}/", task_id);
        if iowrapper_exist(&base_dir) {
            iowrapper_remove_dir_all(&base_dir)?;
        }
        iowrapper_create_dir(&base_dir)?;
        let base_dir = iowrapper_get_absolute_path(&base_dir)?;  // 变成绝对路径.
        let input_dir = path_join(&base_dir, &String::from("rawinput/"));
        // 记得创建这个rawinput目录..
        iowrapper_create_dir(&input_dir)?;
        let dll_path = path_join(&base_dir, &String::from("uesr_mapreduce.dll"));
        // 创建一个TaskEntry
        let taskentry = TaskEntry{
            task_id,
            hdfs_base_dir,
            task_base_dir : base_dir,
            input_dir : input_dir.clone(),
            dll_path : dll_path.clone(),
            result_path : None,
            mapper_num : packet.mapper_num,
            reducer_num : packet.reducer_num,
            status : Status::Waiting,
            stream : None,
        };
        // 形成发回的数据包.
        let message = MessagePacket{
            message_type : 4,   // 通知，已经分配任务, 编号为4.
            from : 0,   // 无用.
            task_id : task_id,
            data_file : taskentry.hdfs_base_dir.clone(),   // client 把数据文件放在这里, 这是个文件夹.
            dll_file : path_join(&taskentry.hdfs_base_dir, &String::from("uesr_mapreduce.dll")),  // 把dll放在这里, 这是个文件名，直接复制到这个文件名即可.
            mapper_num : packet.mapper_num,  // 无用
            reducer_num : packet.reducer_num,  // 无用.
        };
        // 存储任务表项
        self.task_map.insert(task_id, taskentry);

        stream.write_all(serde_json::to_string(&message)?.as_bytes())?;
        // 发送消息
        // 注意在这里之后，那个stream被drop了!
        Ok(())
    }

    /// 处理第2种，client报告已经准备好了，把任务丢入master_poll中.
    /// 注意要把文件hdfs中的文件取回到"本地"
    fn handle_type_2_client_prepared(&mut self, mut stream:TcpStream, packet:MessagePacket)
        -> Result<(), Box<dyn std::error::Error>> {
        let task_id = packet.task_id;
        let entry = self.task_map.get_mut(&task_id);
        if entry.is_none() {
            eprintln!("received a task id: {task_id} that is not allocated");
            return Err(Box::new(MapReduceError::WrongTaskId));
        }

        let entry = entry.unwrap();
        entry.stream = Some(stream);

        // 将hdfs中的文件复制到“本地”，entry.input_dir中.
        for f_hdfspath in iowrapper_read_dir_into_strings(&entry.hdfs_base_dir)? {
            if f_hdfspath.ends_with(".dll") {
                // 是那个dll, 复制到 entry.dllpath
                iowrapper_copy_file(&f_hdfspath, &entry.dll_path)?;
            }
            else {
                let fname = iowrapper_get_filename(&f_hdfspath)?;
                let to = path_join(&entry.input_dir, &fname);
                iowrapper_copy_file(&f_hdfspath, &to)?;
            }
        }
        // 扔给 master_poll 一个master线程
        let task_id = entry.task_id;
        let m = entry.mapper_num;
        let n = entry.reducer_num;
        let base_dir = entry.task_base_dir.clone();
        let inputpath = entry.input_dir.clone();
        let dllpath = entry.dll_path.clone();
        let server_host = self.host.clone();
        let worker_poll = Arc::clone(&self.worker_poll);
        // 上面这条代码增加一个互斥的共享引用，Arc::clone克隆的是那个引用!
        self.master_poll.execute(move || {
            Master::master_thread(
                task_id, m, n, base_dir, inputpath, dllpath, 
                server_host, worker_poll
            );
        });

        entry.status = Status::Executing;
        Ok(())
    }

    /// 处理第3种情况，client报告已经将结果文件复制到本地，此时server通知master做清理，并且删除这一项task
    /// 并且server需要删除hdfs中的这个目录.
    fn handle_type_3_client_copied(&mut self, mut stream:TcpStream, packet:MessagePacket)
        -> Result<(), Box<dyn std::error::Error>> {
        // client已经把结果复制走了，可以进行清理工作, 本地的清理完全由master完成，所以直接给master的stream发消息就行了。
        // hdfs上的清理由server完成.
        // 只用message_type=6就可以了.
        let json_str = String::from("{\"message_type\":6}");
        let entry = self.task_map.get_mut(&packet.task_id);
        if entry.is_none() {
            eprintln!("received a task id: {} that is not allocated", packet.task_id);
            return Err(Box::new(MapReduceError::WrongTaskId));
        }
        let entry = entry.unwrap();
        if let Some(mut master_stream) = entry.stream.take() {
            // 一定有，不可能没有. 此时这个stream应该是master发来的stream.
            master_stream.write_all(json_str.as_bytes())?;
        }
        // 清理hdfs上的这个任务的文件夹.
        iowrapper_remove_dir_all(&entry.hdfs_base_dir)?;
        // master做完所有文件的清理，server可以直接删除这个task项目了.
        self.task_map.remove(&packet.task_id);   // 删除这个键值对，返回的值没用了.
        Ok(())
    }

    /// 处理第7种情况, master报告任务<id>完成，并包含结果文件的位置(|分隔的多个文件)
    fn handle_type_7_master_completed(&mut self, mut stream:TcpStream, packet:MessagePacket)
        -> Result<(), Box<dyn std::error::Error>> {
        // 这里的stream是master的.同样应该暂存下来，等client发送获取结果完毕的通知.
        let entry = self.task_map.get_mut(&packet.task_id);
        if entry.is_none() {
            eprintln!("received a task id: {} that is not allocated", packet.task_id);
            return Err(Box::new(MapReduceError::WrongTaskId));
        }
        let entry = entry.unwrap();
        entry.result_path = Some(packet.data_file.clone());

        // 把data_file中的文件都复制到hdfs_base_dir上.
        let mut hdfs_ret_filepaths = String::new();
        for local_ret_path in packet.data_file.split('|') {
            let local_ret_fname = iowrapper_get_filename(&local_ret_path.to_string())?;
            let hdfs_ret_path = path_join(&entry.hdfs_base_dir, &local_ret_fname);
            iowrapper_copy_file(&local_ret_path.to_string(), &hdfs_ret_path)?;
            hdfs_ret_filepaths.push_str(&hdfs_ret_path);
            hdfs_ret_filepaths.push('|');
        }
        let hdfs_ret_filepaths = hdfs_ret_filepaths.trim_end_matches('|').to_string();

        let message = MessagePacket {
            message_type : 5,   // 告知client任务已经完成，结果文件已经准备好.
            from : 0,
            task_id : packet.task_id,
            data_file : hdfs_ret_filepaths,  // 这个就是|分隔的所有结果文件. 不过在hdfs上
            dll_file : String::new(),   // 无用
            mapper_num : 0,  // 无用.
            reducer_num : 0,  // 无用
        };
        let json_str = serde_json::to_string(&message)?;
        if let Some(mut client_stream) = entry.stream.take() {
            // 一定有，不可能没有.
            client_stream.write_all(json_str.as_bytes())?;
        }
        entry.stream = Some(stream);
        Ok(())
    }

    /// 处理信号8，master报告任务失败，且不是因为机器原因失败。server发送信号回给client, 
    /// 此时，message_packet中的data_file为空(作为标识), 而dll_file存储错误信息.
    /// 同时master发过来的数据包中，dll_file也是错误信息.
    fn handle_type_8_master_report_failed(&mut self, mut stream:TcpStream, packet:MessagePacket)
        -> Result<(), Box<dyn std::error::Error>>
    {
        let entry = self.task_map.get_mut(&packet.task_id);
        if entry.is_none(){
            eprintln!("received a task id: {} that is not allocated", packet.task_id);
            return Err(Box::new(MapReduceError::WrongTaskId));
        }
        let entry = entry.unwrap();
        let message = MessagePacket{
            message_type : 5,
            from :0,
            task_id : packet.task_id,
            data_file : String::new(),
            dll_file : packet.dll_file,
            mapper_num : 0,
            reducer_num : 0
        };
        let json_str = serde_json::to_string(&message)?;
        // 通知client出错了.
        if let Some(mut client_stream) = entry.stream.take() {
            client_stream.write_all(json_str.as_bytes())?;
        }
        // 直接清除这个任务，结束.
        iowrapper_remove_dir_all(&entry.task_base_dir)?;
        iowrapper_remove_dir_all(&entry.hdfs_base_dir)?;
        self.task_map.remove(&packet.task_id);
        Ok(())
    }

    pub fn hdfs_root_dir() -> &'static str {
        "/DS2023"
    }
}
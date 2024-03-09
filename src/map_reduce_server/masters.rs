use serde::{Deserialize, Serialize};

use crate::map_reduce_server::Status;
use crate::map_reduce::MessagePacket;
use crate::thread_poll::ThreadPoll;
use crate::io_wrapper::*;
use crate::map_reduce_server::workers::{mapper,reducer};
use crate::error::MapReduceError;
use std::{
    sync::mpsc::channel,
    sync::Arc,
    sync::Mutex,
    io::prelude::*,
    net::TcpStream, 
};

/// Master和Worker之间通信(Worker向Master发送包)的格式.
#[derive(Deserialize, Serialize)]
pub struct MasterWorkerInfo{
    pub subtask_id : u32,
    pub successed : bool,
    pub result_path : String,
}

pub struct Master{
    task_id : u32,
    mapper_num : u32,
    reducer_num : u32,
    mapper_completed : u32,
    reducer_completed: u32,
    base_dir : String,   // 暂时的该任务的基本目录.
    inputpath : String,  // 总的input的路径
    dllpath : String,
    mapper_tracking_list : Vec<SubTaskEntry>,
    reducer_tracking_list: Vec<SubTaskEntry>,
}

// 子任务追踪中的表项
struct SubTaskEntry{
    subtask_id : u32,
    status : Status,
    inputpath: String,
    resultpath:String,
}

impl SubTaskEntry{
    pub fn new(subtask_id:u32, status:Status, inputpath:String)->SubTaskEntry{
        SubTaskEntry { subtask_id, status, inputpath, resultpath: String::from("") }
    }
}

// Master的关联函数, 将这个任务分配给thread.  
impl Master{
    pub fn new(task_id:u32, m:u32, n:u32, base_dir:String, inputpath:String, dllpath:String) -> Master{
        Master{
            task_id,
            mapper_num : m,
            reducer_num: n,
            mapper_completed: 0,
            reducer_completed: 0,
            base_dir,
            inputpath,
            dllpath,
            mapper_tracking_list: Vec::new(),
            reducer_tracking_list: Vec::new(),
        }
    }

    /// master函数的入口
    pub fn master_thread(
        task_id:u32,m:u32, n:u32, base_dir:String,
        inputpath:String, dllpath:String,
        server_host:String,
        worker_poll: Arc<Mutex<ThreadPoll>>   // 共享所有权并且互斥.
    ) {
        // TODO: 在这里申请Master，这样之后在失败之后就可以在这里进行清理，否在在do_master中清理.
        if let Err(e) = Master::do_master(
            task_id, m, n, base_dir, inputpath, dllpath, server_host.clone(), worker_poll) {
            eprintln!("Master (task id: {}) failed. {}",task_id, e);
            let mut stream = TcpStream::connect(server_host).expect(
                "Master cannot connect to Server!"
            );
            // 如果server死了，master也没必要活着..
            let message_json_str = format!(
                "{{\"message_type\":8,\"task_id\":{},\"dll_file\":{:?}}}",
                task_id, e
            );
            stream.write_all(message_json_str.as_bytes()).expect(
                "Master cannot send messages to Server..."
            );
            // TODO: 执行可能的清理...?
        }
    }

    /// 创建一个master所用的线程函数!
    fn do_master(
        task_id:u32,m:u32, n:u32, base_dir:String,
        inputpath:String, dllpath:String,
        server_host:String,
        worker_poll: Arc<Mutex<ThreadPoll>>   // 共享所有权并且互斥.
    ) -> Result<(), Box<dyn std::error::Error>>{
        let mut master:Master = Master::new(task_id, m, n, base_dir, inputpath.clone(), dllpath.clone());
        let (sender, receiver) = channel::<String>();

        // 先创建所有mapper任务
        let mut mapper_id : u32 = 0;
        for filepath in iowrapper_read_dir_into_strings(&master.inputpath)?{
            // filepath直接是文件夹子文件的路径.
            let mapper_task = SubTaskEntry::new(
                mapper_id,
                Status::Waiting,
                String::from(filepath)
            );
            master.mapper_tracking_list.push(mapper_task);
            mapper_id+=1;
        }
        // 纠正可能的m的错误，让m变成mapper_tracking_list中的值, mapper_tracking_list是inputpath中文件数量.
        let real_m = master.mapper_tracking_list.len() as u32;
        if real_m != master.mapper_num {
            //---------------------------------
            // TODO: 打一条后续发给client 的 log?
            //---------------------------------
            master.mapper_num = real_m;
        }

        // 接着把所有mapper任务分配出去.
        for mapper_task_item in &mut master.mapper_tracking_list {
            let task_id = master.task_id;
            let subtaskid = mapper_task_item.subtask_id;
            let base_dir = master.base_dir.clone();
            let inputp = mapper_task_item.inputpath.clone();
            let dllp = master.dllpath.clone();
            let n = master.reducer_num;
            let worker_sender = sender.clone();
            // 绕考所有权机制，让下面的闭包获取所有权..
            // 别的线程在拿着worker_poll的时候死掉了，会返回一个error(但同样获取了mutex). ——暂时不管.
            worker_poll.lock().unwrap().execute(move || {
                mapper(
                    task_id,
                    subtaskid, 
                    base_dir,
                    inputp, 
                    dllp, 
                    n, 
                    worker_sender);
            });
            mapper_task_item.status = Status::Executing;  // 修改状态.
        }

        // 接着读取回复结果. 有一个sender在自己这里，一定不会因没有发送端而终止.
        let mut max_mapper_num: u32 = master.mapper_num;
        loop{
            let mapper_result = receiver.recv()?;
            let packet:MasterWorkerInfo = serde_json::from_str(&mapper_result)?;
            let index = packet.subtask_id as usize;
            let mapper_task:&mut SubTaskEntry = &mut master.mapper_tracking_list[index];
            if packet.successed {
                mapper_task.status = Status::Completed;
                mapper_task.resultpath = packet.result_path;  // 一个mapper会准备n个输出文件，在一个文件夹下.
                master.mapper_completed += 1;
            } else {
                //------------TODO--------------
                // 错误处理, 保存一条log..?
                //------------------------------
                mapper_task.status = Status::Error;
                max_mapper_num -= 1;
                
            }
            if master.mapper_completed == max_mapper_num {
                // 所有任务都完成了.
                break;
            }
        }

        // 准备reducer任务. 第 i 个reducer的输入文件是所有mapper的第i个输出文件.
        for i in 0..master.reducer_num {
            // 编号为i的reducer, 输入文件是所有mapper的输出文件i.mid  
            let mut inputfiles = String::new();
            for mapper_task in &master.mapper_tracking_list{
                // 只记录那些成功的.
                match mapper_task.status {
                    Status::Completed => {
                        let inputfile = path_join(
                            &mapper_task.resultpath, 
                            &String::from(format!("{i}.json"))
                        );
                        inputfiles.push_str(&inputfile);
                        inputfiles.push('|');
                    }
                    _ => {
                        continue;
                    }
                }
            }
            let inputfiles = inputfiles.trim_end_matches('|').to_string();  // 去掉末尾的 |
            let reducer_task = SubTaskEntry::new(
                i+mapper_id, 
                Status::Waiting,
                inputfiles
            );
            master.reducer_tracking_list.push(reducer_task);
        }
        
        // 向 workerpoll 中丢入所有 reducer 任务.
        for reducer_task_item in &mut master.reducer_tracking_list{
            // 向Workerpoll 丢入一个 reducer任务. 注意所有权向闭包的转移..
            let task_id = master.task_id;
            let subtask_id = reducer_task_item.subtask_id;
            let base_dir = master.base_dir.clone();
            let inputfilepath = reducer_task_item.inputpath.clone();
            let dllpath = master.dllpath.clone();
            let worker_sender = sender.clone();
            worker_poll.lock().unwrap().execute(move || {
                reducer(
                    task_id, subtask_id - mapper_id, 
                    base_dir, inputfilepath, dllpath, worker_sender);
            });

            reducer_task_item.status = Status::Executing;
        }

        // 接下来等worker回复完成reducer的消息.
        let mut max_reducer_num = master.reducer_num;
        loop{
            let reducer_result = receiver.recv()?;
            let packet:MasterWorkerInfo = serde_json::from_str(&reducer_result)?;
            let index = packet.subtask_id as usize;
            let reducer_task:&mut SubTaskEntry = &mut master.reducer_tracking_list[index];
            if(packet.successed){
                reducer_task.status = Status::Completed;
                reducer_task.resultpath = packet.result_path;
                master.reducer_completed += 1;
            } else {
                // --------TODO----------------
                // 错误处理，打个Log之类的?.
                // ----------------------------
                reducer_task.status = Status::Error;
                max_reducer_num -= 1;
            }

            if master.reducer_completed == max_reducer_num {
                break;
            }
        }

        // 完成，收集结果文件位置.
        let mut resultfiles = String::new();
        for reducer_task in &master.reducer_tracking_list {
            resultfiles.push_str(&reducer_task.resultpath);
            resultfiles.push('|');
        }
        // 注意，自己整除json字符串，\需要显式地有两个：\\ ! 否则非法.
        let resultfiles = resultfiles.trim_end_matches('|').replace('\\', "/");   //去掉末尾的 |

        // 接下来向Server发送消息：完成.
        let mut tcpstream = TcpStream::connect(server_host)?;
        let message_json_str = format!(
            "{{\"message_type\":7,\"task_id\":{},\"data_file\":\"{}\"}}",
            master.task_id, resultfiles
        );  // 一个json格式字符串. 也许有更好的方式统一地组织它...

        tcpstream.write_all(message_json_str.as_bytes())?;
        
        // 之后等待回复, 回复的一定是clear信号(type:6)，所以不用管内容，只是阻塞到等来信号.
        let mut unused = [0; 1024];
        tcpstream.read(&mut unused)?;   // 这里会阻塞. 这个信息用不上，不管.

        // 执行清理：清理原始inputfiles, 清理mapper产生的所有中间文件，清理reducer产生的结果文件.
        // 1. 清除最开始的输入文件位置与dllpath.
        iowrapper_remove_dir_all(&inputpath)?;
        iowrapper_remove_file(&dllpath)?;
        // 2. 清除所有成功的mapper_task的resultpath(是一个文件夹)
        for mapper_task in &master.mapper_tracking_list {
            match mapper_task.status {
                Status::Completed => {
                    iowrapper_remove_dir_all(&mapper_task.resultpath)?;
                }
                _ => {}
            }            
        }
        // 3. 清除所有成功的reducer_task的resultpath(是一个文件)
        for reducer_task in &master.reducer_tracking_list {
            match reducer_task.status {
                Status::Completed => {
                    iowrapper_remove_file(&reducer_task.resultpath)?;
                }
                _ => {}
            }
        }
        // 4. 清理掉这个任务的base_dir.
        iowrapper_remove_dir_all(&master.base_dir)?;

        println!("Master of task {} completed and quited.", master.task_id);
        Ok(())
    }
}
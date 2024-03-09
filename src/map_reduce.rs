use serde::{Deserialize, Serialize};

use crate::io_wrapper::HdfsSetting;
use crate::error::MapReduceError;

/// 初始化HDFS客户端的全局设置；这个函数只应该调用一次.
pub fn SETUP_GLOBAL_HDFS_CLIENT(client_host : &str, user : &str) -> Result<(),MapReduceError> {
    let setting = HdfsSetting::new(client_host, user)?;
    HdfsSetting::init_global(setting)?;
    Ok(())
}

/// 定义通信类型(message_type)：\
/// message_type:   意义\
/// Client:  \
/// 1:              client申请任务号, 开启一个任务 \
/// 2:              client将输入文件与dll准备到指定位置，可以开始任务 \
/// 3:              client将结果文件复制到本地，任务完毕 \
/// Server:  \
/// 4:              向client发送任务号，存放输入文件与dll的位置  \
/// 5:              向client发送结束通知，并附上结果文件的位置  \
/// 6:              向master发送清理中间与结果文件的通知  \
/// Master:  \
/// 7:              向server发送任务处理完毕通知，包括任务id以及结果文件位置 \
/// 8:              向server发送任务失败的通知，这个不是机器的问题，所以server也向client发送失败信息.
///                 暂时把data_file设置为空字符串表示失败....
#[derive(Deserialize, Serialize, Debug)]
pub struct MessagePacket{
    pub message_type:u8,
    #[serde(default="default_packet_int")]
    pub from:u32,    // 可能要用到的id.
    #[serde(default="default_packet_int")]
    pub task_id:u32,
    #[serde(default="default_packet_string")]
    pub data_file:String,   // 可能是输入文件，也可能是结果文件.
    #[serde(default="default_packet_string")]
    pub dll_file:String,
    #[serde(default="default_packet_int")]
    pub mapper_num:u32,
    #[serde(default="default_packet_int")]
    pub reducer_num:u32,
}


// MessagePacket的一些默认值.
fn default_packet_int()->u32{
    0
}
fn default_packet_string()-> String{
    "unused".to_string()
}
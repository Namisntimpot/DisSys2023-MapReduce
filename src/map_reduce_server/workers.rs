// 真正执行map和reduce的workers所使用的线程函数.  
use libloading::{Symbol, Library};
use std::fmt::format;
use std::sync::mpsc::Sender;
use std::{
    sync::mpsc, collections::HashMap, collections::BTreeMap,
    hash::{Hash, Hasher},
    collections::hash_map::DefaultHasher,
};
use serde_json;

use crate::io_wrapper::*;
use crate::map_reduce_server::masters::MasterWorkerInfo;

use super::masters::Master;

/// 链接并且执行mapper函数.
/// 用户mapper定义: pub fn mapper(content:&String)->HashMap<String, Vec<String>>;
fn load_execute_mapper(dllpath : &String, content:&String)->Result<HashMap<String,Vec<String>>,Box<dyn std::error::Error>>{
    unsafe{
        let lib = Library::new(dllpath)?;
        let func : Symbol<fn(&String)->HashMap<String,Vec<String>>> = lib.get(b"mapper")?;
        Ok(func(content))
    }
}

/// 链接并且执行reducer函数.
/// 用户reducer定义：pub fn reducer(k:&String, v:&Vec<String>)->Vec<String>;
/// 直接丢入合并完的BTreeMap(它自己是有序的)的可变引用, 并且直接在它上面修改(in-place)
fn load_execute_reducer(dllpath:&String, btree:&mut BTreeMap<String,Vec<String>>) 
    -> Result<(), Box<dyn std::error::Error>> {
    unsafe{
        let lib = Library::new(dllpath)?;
        let func:Symbol<fn(&String, &Vec<String>)->Vec<String>> = lib.get(b"reducer")?;
        for (k, v) in btree {
            *v = func(&k, &v);
        }
        Ok(())
    }
}

pub fn mapper(
    task_id : u32,      // 创建文件夹用.
    subtask_id : u32,
    base_dir : String,
    inputfilepath : String,    //mapper的输入文件是一个文件.
    dllpath : String,
    reducer_num : u32,
    sender : Sender<String>
) {
    //-----TODO---------
    // 先把inputfile和dllpath复制到本地, 先不实现.
    //------------------
    if let Err(e) = do_mapper(
        task_id, subtask_id, base_dir, inputfilepath, dllpath, reducer_num, &sender) {
        eprintln!("mapper {} of task {} failed : {}",subtask_id, task_id, e);
        let err_info = MasterWorkerInfo{
            subtask_id,
            successed : false,
            result_path : String::from(format!("{}",e))
        };
        sender.send(serde_json::to_string(&err_info).unwrap()).unwrap();          
    }
    //-----------TODO-----------
    // 清理之前复制到“本地”的临时文件
    //--------------------------
    println!("Mapper\t{}\tof task\t{}\tsuccessfully finished and quited.", subtask_id, task_id);
}

pub fn do_mapper(
    task_id : u32,      // 创建文件夹用.
    subtask_id : u32,
    base_dir : String,
    inputfilepath : String,    //mapper的输入文件是一个文件.
    dllpath : String,
    reducer_num : u32,
    sender : &Sender<String>
) -> Result<(), Box<dyn std::error::Error>> {

    let localinputfile = inputfilepath;  // 暂时先用这个替代. 注意是绝对路径.
    let localdllpath = dllpath;

    // 读取这个输入文件的所有内容.
    let content = iowrapper_read_to_string(&localinputfile)?;  // 完整的文件内容.
    
    // 动态链接localdllpath.
    let mapper_ret = load_execute_mapper(&localdllpath, &content)?;

    // 取出 mapper_ret 中的键值对，哈希之后放入不同的文件里. 放置中间文件..
    // 将中间文件放在"./{task_id}/{subtask_id}/XX.mid", 也就是base_dir/subtask_id/XX.mid
    let mid_dir = path_join(&base_dir, &format!("{}/", subtask_id));
    iowrapper_create_dir(&mid_dir)?;
    // 创建n个中间文件，命名为{i}.mid(从0开始)，然后准备n个hashmap，把结果存入n个文件中.
    // 感觉应该流式处理要好一点，但没找到动态地向json文件中加东西的方法.
    let mut hashmaps: Vec<HashMap<String,Vec<String>>> = Vec::with_capacity(reducer_num as usize);
    for i in 0..reducer_num {
        let mid_file_name = String::from(format!("{}.json", i));
        let mid_file_path = path_join(&mid_dir, &mid_file_name);
        // 创建文件.
        iowrapper_create_file(&mid_file_path)?;
        // 一个空的hashmap.
        hashmaps.push(HashMap::new());
    }
    // 把mapper_ret中的键值对决定放入哪个文件后放入相应的hashmap中.
    
    for (k, v) in mapper_ret {
        let mut hasher = DefaultHasher::new();  // 哈希一下来shuffle
        // 通过new or default出来的DefaultHasher都是一样的.
        k.hash(&mut hasher);
        let index = (hasher.finish() % (reducer_num as u64)) as usize;
        // hashmap中不可能有键值重复.
        hashmaps[index].insert(k, v);
    }
    // 保存进文件.
    for i in 0..reducer_num {
        let p = String::from(format!("{}.json", i));
        let p = path_join(&mid_dir, &p);
        let content = serde_json::to_string_pretty(&hashmaps[i as usize])?;
        iowrapper_write_file_all(&p, &content)?;
    }
    // 发消息
    let success_info = MasterWorkerInfo{
        subtask_id,
        successed: true,
        result_path : mid_dir,
    };
    sender.send(serde_json::to_string(&success_info)?)?;
    Ok(())
}

pub fn reducer(
    task_id : u32,
    subtask_id : u32,
    base_dir : String,
    inputfilepath : String,   // 这个是许多用|分隔的许多文件路径.
    dllpath : String,
    sender : Sender<String>
) {
    //------TODO----------
    // 把“不同机器上”的文件(包括dllpath)复制到本机，暂且略.
    //--------------------
    if let Err(e) = do_reducer(
        task_id, subtask_id, base_dir, inputfilepath, dllpath, &sender) {
        eprintln!("Reducer {} of task {} failed : {}",subtask_id, task_id, e);
        let err_info = MasterWorkerInfo{
            subtask_id,
            successed : false,
            result_path : String::from(format!("{}",e))
        };
        sender.send(serde_json::to_string(&err_info).unwrap()).unwrap();
    }
    // ----------TODO---------------------
    // 清理之前“复制到本地”的临时文件；暂时不用管.
    // -----------------------------------
    println!("Reducer\t{}\tof task\t{}\tsuccessfully finished and quited.", subtask_id, task_id);
}

pub fn do_reducer(
    task_id : u32,
    subtask_id : u32,
    base_dir : String,
    inputfilepath : String,   // 这个是许多用|分隔的许多文件路径.
    dllpath : String,
    sender : &Sender<String>
) -> Result<(), Box<dyn std::error::Error>> {
    let inputfiles:Vec<&str> = inputfilepath.split('|').collect();  // 多个输入文件的路径.
    let local_inputfiles = inputfiles;
    let local_dllpath = dllpath;

    // 将所有文件的内容读进来，放进一个hashmap中，同一个键的只是append，然后将各键值对分别丢入reducer.
    // 注意，要根据key来排序！所以使用有序的BTreeMap
    let mut btree: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for file in local_inputfiles {
        // 遍历完之后local_inputfiles没用了, 内部成员所有权被移走.
        let json_str = iowrapper_read_to_string(&String::from(file))?;
        let subhashmap:HashMap<String,Vec<String>> = serde_json::from_str(&json_str[..])?;
        for (k, mut v) in subhashmap {
            // 对其中一个文件，如果btree中有就append到已有项中; 如果没有就插入键值对.
            match btree.get_mut(&k) {
                Some(entry) => {
                    entry.append(&mut v);  // 这个要求 moves all elements in "other" and then append.
                }
                None => {
                    btree.insert(k, v);
                }
            }
        }
    }
    // 执行reducer, 注意下面的直接把结果修改在了btree里，是in-place的
    load_execute_reducer(&local_dllpath, &mut btree)?;
        // Ok(_) => { },   // do nothing
        // Err(_) => {
        //     println!("Failed when loading and executing user's reducer");
        //     // 发失败消息.
        //     let err_info = MasterWorkerInfo{
        //         subtask_id,
        //         successed : false,
        //         result_path : String::from("Failed to load and execute reducer")
        //     };
        //     sender.send(serde_json::to_string(&err_info).unwrap()).unwrap();
        //     //--------TODO------------
        //     // 清理刚才复制过来的临时文件, 因为现在没考虑这个，先不管
        //     //------------------------
        //     return;   // 结束整个reducer过程.
        // }
    // }
    // 现在这个btree就是结果了，把它保存到结果文件中.
    // 文件路径为 ./task_id/ret{subtask_id}.json
    let ret_path = path_join(
        &base_dir, &format!("ret{}.json",subtask_id)
    );
    iowrapper_create_file(&ret_path)?;
    let ret_json_str = serde_json::to_string_pretty(&btree)?;
    iowrapper_write_file_all(&ret_path, &ret_json_str);
    // 发送成功的消息.
    let success_info = MasterWorkerInfo{
        subtask_id,
        successed : true,
        result_path : ret_path,
    };
    sender.send(serde_json::to_string(&success_info).unwrap()).unwrap();

    Ok(())  // Over
    
}
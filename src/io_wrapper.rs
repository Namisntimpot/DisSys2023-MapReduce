/// 因为可能要接入 hdfs，但最开始就直接用本地fs，所以用一个wrapper封装.
/// 已添加错误传递...
/// 应该用些宏定义简化代码...
/// 存在一个平台的差异：Win下以write权限打开文件如果没有会直接创建一个；Linux下如果没有会报错。必须在option上加上.create(true)来保证.
use std::{
    io::{prelude::*, BufReader},
    fs,
    fs::File,
    path::Path, sync::Once, str::FromStr, os::unix::net::SocketAddr, borrow::BorrowMut,
};
use once_cell::sync::OnceCell;
use serde_json::de::IoRead;
use hdrs::{Client as HdfsClient, ClientBuilder as HdfsClientBuilder};

use crate::error::MapReduceError;

type IOResult<T> = Result<T, MapReduceError>;

pub struct HdfsSetting{
    hdfs_client_host : String,
    user : String,
    hdfs_client : HdfsClient,
}

static HDFS_CLIENT_INSTANCE : OnceCell<HdfsSetting> = OnceCell::new();

impl HdfsSetting {
    pub fn new(host : &str, user : &str) -> IOResult<HdfsSetting> {
        let setting = HdfsSetting {
            hdfs_client_host : host.to_string(),
            user : user.to_string(),
            hdfs_client : HdfsClientBuilder::new(host).with_user(user).connect()?
        };
        Ok(setting)
    }

    pub fn init_global(setting : HdfsSetting) -> IOResult<()> {
        let ret = HDFS_CLIENT_INSTANCE.set(setting);
        if ret.is_err() {
            return Err(MapReduceError::FileIOError(std::io::Error::new(std::io::ErrorKind::NotConnected, "Something wrong when initialize global hdfs client")));
        }
        Ok(())
    }

    pub fn get_global_client() -> IOResult<&'static HdfsClient> {
        let r = HDFS_CLIENT_INSTANCE.get();
        if r.is_none() {
            return Err(MapReduceError::FileIOError(std::io::Error::new(std::io::ErrorKind::Other, "HDFS client unset.")));
        }
        let r = r.unwrap();
        Ok(&r.hdfs_client)
    }

    pub fn path_head() -> &'static str {
        "hdfs://"
    }
}

/// 把两个路径结合，path1为父, path2为子
/// 注意，path1可以直接带前缀 hdfs://，并且结果也带着它
pub fn path_join(path1:&String, path2:&String) -> String{
    let par = Path::new(path1);
    let err = String::from("Failed to join paths");
    par.join(path2).into_os_string().into_string().expect(&err)
}

/// 输入一个字符串的文件夹绝对路径，返回文件夹下所有文件路径(string)的迭代器(Vec)
/// 如果目录路径不存在或者其它错误导致read_dir失败，就会返回错误，否则返回 
/// 这个迭代器中只包含文件夹下可读的项目. ReadDir迭代器里是Result<DirEnry>, 他可能出错.
/// DirEntry转为字符串的时候也可能出错... 总之最后返回可用的文件路径列表.  \
/// 这个函数返回的列表中，每个项都直接是绝对路径！并且如果是hdfs，会包含hdfs://
pub fn iowrapper_read_dir_into_strings(dir:&String) -> IOResult<Vec<String>> {
    if dir.starts_with(HdfsSetting::path_head()) {
        let dir = &dir[HdfsSetting::path_head().len()..];
        let client = HdfsSetting::get_global_client()?;   // 它直接是metadata...
        // 没搞明白下面的所有权转移...read_dir的结果Readdir里有一个IntoIter<...>(这是个啥东西？？)，这个东西没有copy trait，所以不能move value...???
        // 暂时先用clone让它运行，虽然感觉不太对劲.
        let ret = client.read_dir(dir)?.clone()
                .map(|x| {
                    let mut p = HdfsSetting::path_head().to_string();
                    p.push_str(x.path());
                    p
                }).collect::<Vec<String>>();
        Ok(ret)
    } else {
        let ret = fs::read_dir(dir)?
                .filter_map(|x| x.ok())
                .map(|x| x.path().into_os_string().into_string())
                .filter_map(|x| x.ok())
                .collect::<Vec<String>>();
        Ok(ret)
    }
}

/// 获取文件名或者文件夹的名字，直接从输入的字符串中获取(即获取路径中的最后一项)，不去文件系统中验证。\
/// 如果是hdfs(以 hdfs:// 开头)，如果以/结尾，删了这个/；然后返回最后一个/之后的内容，如果没有/，就报错，因为hdfs路径一定需要是绝对路径.
pub fn iowrapper_get_filename(path:&String) -> IOResult<String> {
    if path.starts_with(HdfsSetting::path_head()) {
        let path = if path.ends_with('/') {
            &path[HdfsSetting::path_head().len()..path.len()-1]
        } else {
            &path[HdfsSetting::path_head().len()..]
        };
        if let Some(index) = path.rfind('/') {
            Ok(path[index+1..].to_string())
        } else {
            Err(MapReduceError::PathError)
        }
    } else {
        let p =Path::new(path);
        let ret = p.file_name();   // return None if path 以 '..' 结尾..?
        match ret {
            Some(name) => {
                match name.to_os_string().into_string(){
                    Ok(n) => Ok(n),
                    Err(_) => Err(MapReduceError::PathError)
                }
            }
            None => Err(MapReduceError::PathError)
        }
    }
}

/// 返回文件的扩展名。注意返回Err的原因可能是真的出错(比如文件不存在)，也可能是文件没有扩展名(没有'.')
pub fn iowrapper_get_extension(path:&String) -> IOResult<String> {
    if path.starts_with(HdfsSetting::path_head()) {
        let fname = iowrapper_get_filename(path)?;
        match fname.rfind('.') {
            Some(index) => {
                Ok(fname[index+1..].to_string())
            },
            None => {
                Err(MapReduceError::PathError)
            }
        }
    } else {
        let p = Path::new(path);
        let ret = p.extension();
        match ret {
            Some(ext) => {
                match ext.to_os_string().into_string(){
                    Ok(e) => Ok(e),
                    Err(_) => Err(MapReduceError::PathError)
                }
            }
            None => Err(MapReduceError::PathError)
        }
    }
}

/// 获取绝对路径. 注意它只能获取已有文件的绝对路径. \
/// 如果是hdfs，它必须以/开头，如果不是就直接返回错误；如果是就直接返回原样(包括hdfs://)而不检查是否存在(因为hdfs太慢了)
pub fn iowrapper_get_absolute_path(path:&String) -> IOResult<String> {
    if path.starts_with(HdfsSetting::path_head()) {
        let _path = &path[HdfsSetting::path_head().len()..];
        if _path.starts_with('/') {
            Ok(path.to_string())
        } else {
            Err(MapReduceError::PathError)
        }
    } else {
        let abs = fs::canonicalize(path)?;
        match abs.into_os_string().into_string() {
            Ok(x) => Ok(x),
            Err(_) => Err(MapReduceError::PathError)
        }
    }
}

/// 判断一个文件路径是否存在.  \
/// 如果是hdfs，返回false可能是hdfs正常但路径不存在，也可能是hdfs不正常(比如客户端连接失败)
pub fn iowrapper_exist(path:&String) -> bool {
    let hdfs_head = HdfsSetting::path_head();
    if path.starts_with(hdfs_head) {
        let path = &path[hdfs_head.len()..];
        let client = HdfsSetting::get_global_client();
        if client.is_err() {
            return false;
        }
        let client = client.unwrap();
        match client.metadata(&path) {
            Ok(_) => {return true;},
            Err(_) => {return false;}
        };
    }
    let p = Path::new(path);
    p.exists()
}

/// 创建一个文件夹
pub fn iowrapper_create_dir(path:&String) -> IOResult<()> {
    if path.starts_with(HdfsSetting::path_head()) {
        let path = &path[HdfsSetting::path_head().len()..];
        let client = HdfsSetting::get_global_client()?;
        Ok(client.create_dir(path)?)
    }
    else {
        Ok(fs::create_dir(path)?)
    }
}

/// 删除一个文件夹
pub fn iowrapper_remove_dir(path:&String) -> IOResult<()> {
    if path.starts_with(HdfsSetting::path_head()) {
        let path = &path[HdfsSetting::path_head().len()..];
        let client = HdfsSetting::get_global_client()?;
        Ok(client.remove_dir(path)?)
    } else {
        Ok(fs::remove_dir(path)?)
    }
}

/// 删除一个文件夹下的所有文件，以及这个文件夹.
pub fn iowrapper_remove_dir_all(path:&String) -> IOResult<()>{
    if path.starts_with(HdfsSetting::path_head()) {
        let path = &path[HdfsSetting::path_head().len()..];
        let client = HdfsSetting::get_global_client()?;
        Ok(client.remove_dir_all(path)?)
    } else {
        Ok(fs::remove_dir_all(path)?)
    }
}

/// 创建一个文件.
pub fn iowrapper_create_file(path:&String) -> IOResult<()> {
    if path.starts_with(HdfsSetting::path_head()) {
        let path = &path[HdfsSetting::path_head().len()..];
        let client = HdfsSetting::get_global_client()?;
        client.open_file().write(true).create(true).open(path)?;   // 通过这种方式创建文件.
        Ok(())
    } else {
        File::create(path)?;
        Ok(())
    }
}

/// 删除一个文件
pub fn iowrapper_remove_file(path:&String) -> IOResult<()> {
    if path.starts_with(HdfsSetting::path_head()) {
        let path = &path[HdfsSetting::path_head().len()..];
        let client = HdfsSetting::get_global_client()?;
        Ok(client.remove_file(path)?)
    } else {
        Ok(fs::remove_file(path)?)
    }
}

/// 读取一个文件的全部内容并返回一个String
pub fn iowrapper_read_to_string(path:&String) -> IOResult<String> {
    if path.starts_with(HdfsSetting::path_head()) {
        let path = &path[HdfsSetting::path_head().len()..];
        let client = HdfsSetting::get_global_client()?;
        let mut f = client.open_file().read(true).open(path)?;
        let mut ret = String::new();
        f.read_to_string(&mut ret)?;
        Ok(ret)
    } else {
        Ok(fs::read_to_string(path)?)
    }
}

/// 向文件中写入字符串.并覆盖原有内容
pub fn iowrapper_write_file_all(path:&String, content:&String) -> IOResult<()> {
    if path.starts_with(HdfsSetting::path_head()) {
        let path = &path[HdfsSetting::path_head().len()..];
        let client = HdfsSetting::get_global_client()?;
        let mut f = client.open_file().write(true).create(true).open(path)?;
        Ok(f.write_all(content.as_bytes())?)
    } else {
        Ok(fs::write(path, content)?)
    }
}

/// 把文件路径from下的文件复制到文件to.
pub fn iowrapper_copy_file(from:&String, to:&String) -> IOResult<()> {
    // 如果涉及到hdfs，就把内容读过来、然后写出去... from, to有其一就要这么做.
    if from.starts_with(HdfsSetting::path_head()) || to.starts_with(HdfsSetting::path_head()) {
        // let from_content = iowrapper_read_to_string(from)?;
        // iowrapper_write_file_all(to, &from_content)
        let mut f_from = IOWrapperFile::open_read(from)?;
        //println!("in iowrapper_copy_file: from(read) opened");
        let mut f_to = IOWrapperFile::open_empty(to)?;
        //println!("in iowrapper_copy_file: to(write) opened");
        let mut buf = Vec::with_capacity(10240);   // 10KB
        f_from.read_to_end(&mut buf)?;
        Ok(f_to.write(buf.as_slice())?)
    }
    else {
        fs::copy(from, to)?;
        Ok(())
    }
}

/// 文件大小, in bytes
pub fn iowrapper_filesize(path:&String) -> IOResult<u64> {
    if path.starts_with(HdfsSetting::path_head()) {
        let path = &path[HdfsSetting::path_head().len()..];
        let client = HdfsSetting::get_global_client()?;
        Ok(client.metadata(path)?.len())
    } else {
        Ok(fs::metadata(path)?.len())
    }
}

/// 因为reducer存在持续写的需求，所以要有一个类似文件结构的东西. IOWrapperFile是一个封装
/// 里面就是 std::fs::File 或者 hdfs的类似结构.
/// 原本想用Read Write trait做泛型，但是失败.
pub struct IOWrapperFile{
    f_std : Option<std::fs::File>,
    f_hdrs: Option<hdrs::File>,
    // f : T,
}

impl IOWrapperFile{
    /// 打开文件，清空内容
    pub fn open_empty(path:&String) -> IOResult<IOWrapperFile>{
        if path.starts_with(HdfsSetting::path_head()) {
            let path = &path[HdfsSetting::path_head().len()..];
            let client = HdfsSetting::get_global_client()?;
            // let f : T = client.open_file().write(true).open(path)?;
            // Ok(IOWrapperFile { f })
            let f =client.open_file().write(true).create(true).open(path)?;
            Ok(IOWrapperFile { f_std : None, f_hdrs : Some(f)})
        } else {
            // let f : T = File::options().write(true).open(path)?;
            // Ok(IOWrapperFile { f })
            let f = File::options().write(true).create(true).open(path)?;
            Ok(IOWrapperFile { f_std : Some(f), f_hdrs : None})
        }
    }

    /// 打开文件，追加内容
    pub fn open_append(path : &String) -> IOResult<IOWrapperFile> {
        if path.starts_with(HdfsSetting::path_head()) {
            let path = &path[HdfsSetting::path_head().len()..];
            let client = HdfsSetting::get_global_client()?;
            let f = client.open_file().append(true).create(true).open(path)?;
            // Ok(IOWrapperFile { f })
            // let f = client.open_file().append(true).open(path)?;
            Ok(IOWrapperFile { f_std : None, f_hdrs : Some(f)})
        } else {
            let f = File::options().append(true).create(true).open(path)?;
            // Ok(IOWrapperFile { f })
            Ok(IOWrapperFile { f_std : Some(f), f_hdrs : None})
        }
    }

    /// 打开文件，读内容
    pub fn open_read(path : &String) -> IOResult<IOWrapperFile> {
        if path.starts_with(HdfsSetting::path_head()) {
            let path = &path[HdfsSetting::path_head().len()..];
            let client = HdfsSetting::get_global_client()?;
            let f = client.open_file().read(true).open(&path)?;
            // Ok(IOWrapperFile { f })
            Ok(IOWrapperFile { f_std : None, f_hdrs : Some(f)})
        } else {
            let f = File::options().read(true).open(path)?;
            // Ok(IOWrapperFile { f })
            Ok(IOWrapperFile { f_std : Some(f), f_hdrs : None})
        }
    }

    /// 向文件中写入内容.
    pub fn write(&mut self, content : &[u8]) -> IOResult<()> {
        if let Some(ref mut f) = self.f_std {
            f.write(content)?;
        }
        else {
            let mut f = self.f_hdrs.as_ref().unwrap();
            f.write(content)?;
        }
        Ok(())
        //Ok(self.f.write(content)?)
    }
}

impl Read for IOWrapperFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if let Some(ref mut f) = self.f_std {
            Ok(f.read(buf)?)
        } else {
            let mut f = self.f_hdrs.as_ref().unwrap();
            Ok(f.read(buf)?)
        }
    }
}

/// 文件分块
pub fn iowrapper_file_blocking(file_to_block:&String, target_dir:&String, n_blocks:u32)
        -> IOResult<()> {
    // 结果文件命名格式为 target_dir/%d.xxx
    // 直接简单粗暴地按字节截取可能会导致把某些字符截到了一半(因为是utf-8...)
    // 所以试试按行数尽量均匀划分，每次读写一行...
    // 文件操作应该全部使用iowrapper中的内容.
    if !iowrapper_exist(file_to_block) {
        return Err(
            MapReduceError::FileIOError(std::io::Error::new(std::io::ErrorKind::NotFound,
                                format!("File {} does not exist.",file_to_block)))
        );
    }
    let extension = match iowrapper_get_extension(file_to_block) {
        Ok(ext) => Some(ext),
        Err(_) => None
    };

    let line_count = BufReader::new(IOWrapperFile::open_read(file_to_block)?)
                                                    .lines().count();
    let lines = BufReader::new(IOWrapperFile::open_read(file_to_block)?)
                                                    .lines();
    let floor = line_count / n_blocks as usize;
    let reminder = line_count % n_blocks as usize;
    let mut fws = Vec::new();
    for i in 0..n_blocks as usize {
        let block_f_name = match extension {
            Some(ref ext_name) => {   // extension只有1个.如果把其内容所有权移入for循环，会在第一次结束后drop.
                format!("{}.{}",i,ext_name)
            }
            None => {
                format!("{}",i)
            }
        };
        let output_block_path = path_join(target_dir, &block_f_name);
        // 必须先创建....
        iowrapper_create_file(&output_block_path)?;
        let fw = IOWrapperFile::open_empty(&output_block_path)?;
        fws.push(fw);
    }
    let mut saved_lines = 0;
    let mut to_write = 0;
    for line in lines {
        match line {
            Ok(mut line) => {
                line.push('\n');   //加个换行符号...
                fws[to_write].write(line.as_bytes())?;
                saved_lines += 1;
                if saved_lines >= (if to_write < reminder {floor+1} else {floor}) 
                        && to_write < n_blocks as usize-1{
                    saved_lines = 0;
                    to_write += 1;
                }
            },
            Err(_) => {continue;}
        }
    }
    Ok(())    
}
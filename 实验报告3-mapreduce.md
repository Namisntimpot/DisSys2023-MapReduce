# 实验题目与目的
+ **实验题目**：MapReduce实现  
+ **实验目的**：实现一个简单的MapReduce框架，加深对分布式计算的理解。  

# 实验设备
Ubuntu 20.04 虚拟机。

# 实验原理
用Rust语言实现。单机实现，使用多线程模拟多节点。使用动态链接库实现用户map, reduce过程向框架的“注册”。用Hashmap中的哈希模块实现shuffle和分组，以单机伪分布式部署的hdfs作为本实验使用的分布式文件系统。

# 框架设计
本实验的实现一定程度上参考以下框架，这是一个基于Java的框架：
![指导实现图](framework.png)

我的实现是：
![我的框架](my_mapreduce.png)

## 三个逻辑部分
+ Client，用户直接调用的部分。用户从MapReduce包中的某个接口配置并且启动它后，它与Server连接，Server回复后，向Server发送Job包，并且把输入文件数据存入Server指定的HDFS文件夹路径中.  
+ Server, 服务端，管理task和每个task执行中的一些关键节点，包括申请与创建，分配master去执行，接收master的结束信号，等等. Server有两个线程池：masters_poll专门用作master，workers_poll专门用作workers(执行mapper, reducer)。Server不会直接使用workers_poll，而是将其引用交给master线程.  Server只顾给masters_poll发新任务以及接收结束消息；master只顾往workers_poll发送mapper or reducer过程并接收完成消息，而不关心到底哪个才是我的worker，worker也不区分mapper还是reducer.  
+ Workers，工具线程。接收Master的调度，执行mapper或者reducer任务，根据Master随调度发来的信息，动态链接包含用户提供的动态链接库中mapper和reducer函数。  

## 任务执行具体流程
以下内容以不同逻辑部分之间的交互来组织。  
### 1, Client - Server
这是一个比较完整的流程：
1. Client to Server:  
一个“请求任务”标识，跟着需要的mapper个数 **m** 和reducer个数 **n**.  
2.  Server 在 Task 表中创建一项，包含分配给这个task的id号，并且分配一个用于存放数据文件和map, reduce 动态链接库文件的文件夹，以及存放过程文件、结果文件的文件夹, 向 Client 发送 task id，存放 数据文件 和 dll/so 文件的文件夹。  
3.  Client 将数据文件 *分块后* 复制到指定文件夹中。向 Server 发送 “复制完毕” 标识，包含了自己的任务id.  
4.  Server向master_poll中提交一个master任务。因为所实现的线程池直接包含了FIFO的处理顺序，所以不用Server额外调度。同时更新Task表中该任务的状态。  
5.  Master完成Map、Reduce工作后，向Server的Tcplistener发送“Master _, task _，执行完毕” 的标识，以及相应的输出文件在文件系统中的位置；同时Master还需要清除过程中产生的中间文件，所以Master函数需要等Server的回复。同时Master会根据Worker的回复记录中间文件的位置，因而可以只在Master就完成清理而不用下到worker。worker结束一次工作并向master发送结束消息后可以直接结束、接收下一个任务。  
6.  Server 接收到master的“完成”TCP信息之后，更新Task表中该任务的状态（把master结束消息的tcpstream存进去），找到这个表项里的tcpstream，向client回复“结果文件准备完毕”标识，并带上结果文件位置。  
7.  Client 把结果文件复制到自己需要的地方，发送“task id完毕”标识，然后直接中断连接，返回用户程序。Server收到“完毕”标识后，从task任务表中获取Master发送完成信息的tcpstream，向stream返回“Clean up”标识。Master执行清理操作之后，直接结束，接收下一个task.  

### 2, Server 与 Masters  
Server创建Task项目，直接让master_poll执行一个master过程，让线程池自己排队FIFO，不必进行额外调度。创建master的时候，Server需要提供：task id, 输入文件位置，workers_poll的引用，mapper和reducer的数量m, n，与Server Tcp通信的地址等.  

然后Master独立地管理任务进行，结束向Server的TcpListener返送一个"task id 结束"信息，结束信息包括task id以及结果文件的位置。发送结束信息后，Master等待Server的回复，等到Server的“Clean up”回复后，清理所有中间文件和结果文件（包括移动到本地的输入文件；worker产生的中间文件；结果文件），然后结束执行，可以继续接收下一个任务.  

如果master函数失败了(并非线程死了，而是处理函数失败了)，向Server发送一个含有task_id的信息，让server重新来一次.

### 3, Masters 与 Workers  
Master收到一个任务 (task id, mapper_num: m, reducer_num: n, server_host, input_file_path, dll_path)，它在内部创建一个mapper任务追踪表和一个reducer任务追踪表，追踪表收集每个mapper, reducer子任务的id与状态，以及产生的中间文件的位置。  

Master向 Workers_poll 提交 m 个 mapper 过程，提供：子任务id, 输入文件位置，dll位置，向Master发结束消息用的channal的发送端，reducer 数量n。然后等待channal中的回复，每等到一个就更新mapper表中相应任务的状态并记录中间文件位置。  

等到m个回复(执行完所有mapper后)，向 Workers_poll 提交 n 个 reducer 过程，提供：子任务id，要reduce的文件的位置，dll位置，向Master发结束消息用的channal的发送端。然后等channal中的回复。每等到一个就更新reducer表中的状态并记录这个reducer的输出文件位置。  

收到了n个reducer回复之后，任务完成，向Server发送结束消息。  

### 4, Workers  
Workers接收mapper or reducer 任务。它执行任务，不再有“下级”，成功或出错后向其当时的上级Master(随发送的任务所传递的channal)发送预定义的结构化消息。  

#### mapper过程
（主要参考作业题目中提供的[博客](https://zhuanlan.zhihu.com/p/353356138) ）
1. 从输入文件处读入一个文件到内存，是string（不用把文件复制到本地）。  
2. 从dllpath处动态链接上 mapper 函数。  
3. 直接将文件string丢入mapper函数，得到key-value键值对。  
4. 对key做哈希映射到\[0,n-1]，然后将这个key与value以json保存到本地相应哈希值的中间文件中。  
5. 关闭文件，删除本地的输入文件。向Master的通过传进来的sender发送一个结束消息，包含中间文件位置。  
6. 直接结束。  

#### reducer过程
1. 1个reducer i 对应所有mapper workers 的输出的中间文件i，这个文件路径保存在Master中传递给reducer而不必关系之前的mapper到底是哪个.reducer i 将所有mapper中的中间文件 i 读进来，形成键值对，一边读一边合并(如果有key就append, 如果没有key就create)。  
2. 读完后形成key-value。对key-value根据key排序。  
3. 动态链接reduce函数。  
4. 创建该reducer唯一的输出结果文件。  
5. 对排完序后的的key-value，遍历所有键值对，将键值对丢入reduce函数中得到一个单独的string值。将这个键值对写入结果文件中。  
6. 向Master发送结束消息，包含结果文件位置。  
7. 无需清理，直接结束。  

## 错误处理
完整的MapReduce框架中存在处理机器错误的机制，也就是Mapper, Reducer节点要向其Master“保持心跳”。我的实现中没有考虑这一点。因为是用多线程模拟多节点，所以没有过多考虑单个线程因为非程序原因突然死亡。相反，我尽量让Workers和Masters线程不会因程序执行失败而被杀死(比如panic)，而是在大量可能出错的地方增加错误处理(rust的Result<_,Error\>返回值能做到这一点，rust中绝大部分可能出错的过程都有Result<_,Error\>)，发生了错误的时候终止执行并向上级报告。  

同样也因此，我的MapReduce框架实现要求用户map, reduce过程应该充分考虑错误可能，如果是无法恢复的错误就返回错误由框架处理，而尽量避免可能直接panic()的情况，比如unwarp()。  

# 难点
## IPC，RPC
不同于有丰富的进程间通信(IPC)和远程过程调用(RPC)支持的Java，rust作为一门新兴语言在这方面的支持与现有的实现很少(实际上，是基本没找到)。IPC的问题是促使我最后选择用多线程模拟多节点的主要原因，因为即使无法真的用多机器来做分布式，用多进程来模拟也要“真实”得多——不管怎样，rust多线程间的通信的支持是方便稳定且安全的。  

而RPC是无法绕过的问题，框架不管怎样都得执行用户写的map, reduce函数。我最后使用了一个比较笨的方法，需要用户将map, reduce函数编译成动态链接库，然后将这个库复制到Server能获取的指定路径上，再由Server-Master-Worker一路“通知”下来，让Worker去链接正确的动态链接库。  

## HDFS
幸运的是rust中有一些从rust操作hdfs的第三方库，我用的是 [hdrs](https://github.com/Xuanwo/hdrs)，它基本是对libhdfs的API的封装。  

进一步地，我的框架需要同时在本地文件系统上和HDFS上进行操作，为了能够统一处理，我写了一个 iowrapper 模块，对文件操作的进行封装，从而能够用统一的接口进行文件处理而不用管它来自哪个文件系统。为了区分，hdfs中的路径在最前面加上了hdfs://。  

## 虚拟机部署HDFS、RUST
虚拟机部署HDFS的问题主要来自于对Linux的不熟悉和对网络概念的陌生，但hdfs的部署在网上有大量资料，所以不算难解决。Rust的安装也非常简单，但开发Rust的vscode插件rust-analyzer却需要2.18以上的glibc。我一开始用的是CentOS7，其默认gcc仅仅是4.几版本，glibc最高2.17.升级glibc花了大量力气，而由于glibc是非常底层的库，所以升级失败会带来非常多问题，包括但不限于大部分命令都无法正常执行。最后转用Ubuntu才最终部署成功。  

## 文件分块
最开始尝试直接用对文件按大小按块平均，但后来发现在处理utf-8等变长编码的文件时，经常出现在一个编码的中间某个字节把文件分开、导致这个字失效、产生非法字符的情况。在大规模数据中丢失一两个字符其实无所谓，但无论如何，最后的分块使用了一种按行均分的方式。——显然只有行很少或很不均匀的时候就不靠谱了，这也是我的实现的一个局限。  

# 功能实现
+ MapReduce框架  
  主要由map_reduce_server, map_reduce_server::masters, map_reducer_server::workers 三个模块组成，各自的功能已在前文详述。  
+ Mapper, Reducer  
  具体实现在 map_reducer_server::workers 中，就是两个函数：mapper_thread, reducer_thread。它们是各自的do_mapper/do_reducer 的封装，进行错误处理与必要的通信。  
+ 任务调度
  采用FIFO的任务调度。任务调度由线程池自动完成，不需要其它模块进行额外的操作。其它模块只用在有任务的时候将该任务传入线程池中即可。  
+ Shuffle, Sort  
  使用哈希模块(默认种子)完成shuffle，它有一点额外的好处是，同一个键即使在不同的线程中也会被哈希到同一个分组中。也就是，所有的mapper中，同一个键最后都会进入同一个reducer去合并。  
  使用Rust提供的BTree完成基于键的排序。  

# 代码运行
MapReduce中自带一个Server（MapReduce/src/main.rs就是一个端口号为7878的Server，它也是一个库，也可以像main.rs中那样在其它地方通过引入MapReduce依赖来创建Server）。直接运行main.rs中的Server，在**MapReduce目录**下运行执行：  
```bash
export LD_LIBRARY_PATH=${JAVA_HOME}/lib/server:${HADOOP_HOME}/lib/native:${LD_LIBRARY_PATH}
export CLASSPATH=$(${HADOOP_HOME}/bin/hadoop classpath --glob)
cargo run
```
注意，需要先操作环境变量让hdrs能够正确链接。此前需要JAVA_HOME和HADOOP_HOME环境变量被正确设置。  

MapReduce/test_client/src/main.rs中有一个测试样例，lib.rs中是mapper和reducer函数，这是一个词频统计案例。运行前将MapReduce/test_client/src/main.rs中的第11行的`map_reduce_client::Client::new(...)`的第一个参数改成自己输入文件的路径即可。  
然后在 **MapReduce/test_client/** 下执行：  
```bash
cargo build
cd target/debug
cargo run
```
结果在 MapReduce/test_client/target/debug/ret 中。  

如果是自己写mapper和reducer，则 `cargo init` 新建项目后，创建 `lib.rs`，在其中实现mapper和reducer函数：
```rust
#[no_mangle]
pub fn mapper(content : &String) -> HashMap<String, Vec<String>>;

#[no_mangle]
pub fn reducer(key:&String, value: &Vec<String>) -> Vec<String>;
```

然后在这个项目的 `Cargo.toml`中，加入MapReduce依赖(只能本地依赖，因为没有发布)，然后配置lib:  
```
[lib]
name = "XXX"
crate-type = ["dylib"]
```
注意，在lib中指定了名字后，动态库就会出现在路径 `target/debug/libXXX.so` 上。  

然后在`main.rs`中像 test_client/src/main.rs 中做的那样，创建一个 map_reduce_client 实例并初始化hdfs client，然后让map_reduce_client完成接下来的mapreduce过程即可。  

# 局限
+ 没有真正处理线程崩溃的情况，如果线程在运行过程中崩溃，会出现未知情况。  
+ 没有考虑到真正的大文件，也就是大到内存放不下的情况。现在框架使用的文件io与处理并不完全是流式处理，也有将文件所有内容直接读入内存的情况(主要是mapper)，自然也就没考虑键值对多到必须借助外部排序的情况。  
+ 按行进行文件分割不是好主意。这样在划分的时候就要完全读一遍文件，很低效，而且在文件行很少的时候失效。还是应该按块均分，提高效率。相对于大量数据，几个字符的丢失并无所谓。  
+ 用来测试的文档都比较小。  

# 实验结果
实现了一个简单的 mapreduce 框架，并具有处理错误的能力和一定的可拓展性。  

## 正确性测试
实现了mapreduce论文中提到的几个例子，包括词频统计、分布式排序、Inverted Index等。均未出错。数据文件来自大一数据结构大作业(词频统计)提供的数据文档。  

## 可拓展性
具有一定的可拓展性，masters线程、workers线程由线程池统一管理，所以增减线程节点数量非常简单。只需要在创建map_reduce_server实例的时候指定数量即可。经过测试，确实支持自定义数量且不出错。提交的代码中的server使用1个master节点和3个worker节点。  

## 性能
目前的测试中，耗费的时间几乎完全来自于hdfs的io耗时，由于测试的数据规模较小，未能进行完全的性能测试，亦是本次实验的局限。  

# 结论与体会
一定程度上实现了简单的mapreduce框架，模拟其流程。  
收获之一是对分布式计算有了一定的了解，它的思想并不复杂——是我一个人就能够实现的程度，难处在于实现中对各种有限的现实条件的处理。这个开发经历很大程度上加深了我对分布式计算的理解，以及加强多线程、多进程程序开发的能力。  

此外，用rust开发这个项目是一个比较大胆的决定，因为我此前从没学习过使用过rust。将它作为我rust学习入门的项目显然比较有挑战性，确实花费了很多时间，但结果是不错的。  

最后，我花了很长时间在Linux上配置开发环境(尤其是试图升级CentOS7的gcc和glibc)，不管怎样，确实加深了我对Linux系统的熟悉度——虽然我很难说这种熟悉度能持续多久...太久没有进行记录了。  

总之，这项实验虽然费时费力，但也很有收获。  
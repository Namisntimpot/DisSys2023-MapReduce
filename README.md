# DisSys-MapReduce
北航2023年分布式系统原理课程的期末大作业（之一，最难的那个）。一个用rust+hdfs 实现的 MapReduce 框架。只是一个玩具，因为甚至没有真正的多进程（除了确实有一个简陋的用户-服务器机制，用于发起MapReduce任务），更不要说在部署到分布式环境，MapReduce处理任务的所有行为用多线程模拟。

更多细节（包括实现的框架、远程过程调用RPC的简陋实现、运行代码，等等），见实验报告。
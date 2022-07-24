lab1: MapReduce

思路：1、实现worker向coordinator请求任务，coordinator分配任务，并由worker打印输出。在coordinator中定义TaskInfo(任务详情)、和coordinator。通信：worker使用RPC请求，call coordinator。使用chan保证通信安全

​			2、worker根据任务内容实现Map、Reduce功能(参考了src/main/mrsequential.go)

​			3、实现coordinator的结束任务功能，通过worker传回finish，使用chan记录，并完成coordinator的状态转换

​			4、coordinator操作加锁保证并法。但是不知道为什么，加了锁还是会出现DATA RACE。Debug

ng

结果：通过测试，实现了mapreduce，但是容错(worker失败)的问题有时可能出现，实现的函数不run，需要debug

总结：分布式编成，代码容易写，debug很费劲。第一次接触多进程编程和GO语言，写代码时错了太多语法错误和有关数据抢占的问题。
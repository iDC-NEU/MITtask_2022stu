**1.状态划分**：worker向master主动请求任务，worker应该按照状态进行划分：空闲（此时通过RPC从master请求新的map任务），工作中，故障（持续处理某个任务超过一定时间，此时master会将该任务重新分配给其他空闲worker），等待（在map阶段，所有任务已经分配完毕，但是map任务没有全部完成，此时不能开始reduce，部分worker需要等待）。task按照类型分为map，reduce和finished，worker针对不同类型执行不同操作。task状态分为Free，Running，Done，用于判断map和reduce阶段的结束。

**2.worker和coordinator的交互**：通过RPC，worker通过call调用coordinator中的相应函数并得到结果。例如任务分配函数，和任务状态修改函数等。coordinator在初始化时需要完成map task的创建，在程序进入reduce阶段时需要再次生成reduce task并保存到channel中。当reduce阶段结束时程序进入finished阶段，此时所有任务完成，Done()返回true，coordinator退出

**3.crash处理**：在main线程上启动一个go协程（goroutine）每隔一定时间检测当前阶段TaskMap中所有处于Running状态的任务是否超时，如果超时则将它们的状态重置为Free，并再次放入channel中等待worker调用

**4.并发安全**：使用go提供的channel存储tasks，channel是一个安全的并发队列。不~~同worker对coordinator的请求并发使用一个signal变量控制，使得同时只能有一个worker可以请求coordinator内的函数。~~ 使用信号量signal控制并发访问可能出错，这里使用Go提供的异步锁，保证同一时间只有一个worker可以通过RPC调用coordinator。

目前程序已经通过了test-mr.sh中的所有测试。


Lab3A：
Server（Leader）收到Client的Request，通过raft.Start() 提交Op给raft Leader, 然后开启特殊的Wait Channel 等待Raft 返回给自己这个Req结果。底层共识达成后，Raft的所有peer会apply 这个Op, 每个Server会在单独的线程ApplyLoop 中等待不断到来的ApplyCh中的Op。执行这个Op(GET全部执行，重复的PUT, APPEND不执行)。Apply Loop 根据Op中的信息将执行结果返回给Wait Channel , 注意，只有Leader中才有Wait Channel 在等结果。最后返回执行结果给Request。

Lab3B:
applier负责读取applych，并根据读出的消息进行apply，先判定是否Command还是Snapshot操作。
1、Command ： 过滤掉所有Index小于LastApplied的消息，更新Client对应的lastRequestId，更新KVDB。根据阈值maxraftstatesize判定是否需要Snapshot。
2、SnapShot：调用CondInstallSnap，根据结果选择是否apply该snapshot。
更新lastApllied，将apply的信息传递给WaitApplych。
Snap需要持久化的数据只有KVDB和lastRequestId这两个map，
在apply Snapshot时对这两个map进行更新即可。

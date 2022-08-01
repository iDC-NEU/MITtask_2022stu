Lab2A:leader选举。
通过ticker进行计时，每个节点设置一个150-300ms之间的随机超时时间，如果计时器超时后仍未收到leader的心跳信号，则认为leader出问题了，当前节点将自己的状态设置为candidate，并向其他节点请求投票。当candidate获得的票数超过一半时，成为leader，并发送心跳，维持自己的leader地位。

Lab2B:日志同步。
leader的心跳机制不再是发送一个空的请求，而是带有leader前一条日志和前一条日志的周期去检查peer的一致性，并且带有leader已经提交的最大日志索引。如果同步成功，则修改nextindex，和matchindex，分别表示发送给该peer的下一条日志的索引，和peer的最大已经复制日志的索引。如果日志被集群中大于一半的peer所复制，则提交日志。如果同步失败，则回退，nextIndex-1，并重试。

Lab2C:持久化以及nextIndex优化。
持久化主要由persist()和readpersist()实现，根据figure2持久化的参数主要是currentTerm,votedFor,logs。遇到的主要问题在Figure8 unreliable测试，在appendentries中当需要检查的log过多时，简单的nextIndex-1回退重试是不行的，需要直接定位到发生冲突的位置。如果当前的 log 不一致，follower 直接将当前冲突的 term 值的第一个索引返回给 leader，leader 可以直接跳过这个 term 中的所有 log entry，从上一个 term 的 log 进行尝试。以此定位到冲突索引位置，然后将冲突索引值传递给nextIndex，重新进行同步，就能减少所花费的时间了。

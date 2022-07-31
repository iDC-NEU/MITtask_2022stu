## lab2A:投票选出leader、leader维持心跳信号

1.ticker定时：使用time中的time.Tricker给每一个节点设置一个超时时间overtime，当计时器超时未接收到来自leader的心跳信号后当前节点状态变为候选者，并向其余节点发起投票请求（第一个超时的节点先发起投票请求）。

收到投票请求的节点按照以下规则进行投票处理：

- 1.比较最后一项日志的Term，也就是LastLogTerm，相同的话比较索引，也就是LastLogIndex，如果当前节点较新的话就不会投票

 * 2.如果当前节点的Term比候选者节点的Term大，拒绝投票
 * 3.如果当前节点的Term比候选者节点的Term小，那么当前节点转换为Follwer状态
 * 4.判断是否已经投过票，如果没投票并且能投票(日志更旧)，那么就投票给该候选者

完成投票处理后会返回给候选者节点一个reply，候选者根据reply的状态进行以下处理：

- 1.返回值Trem小于发送者（可能由于网络分区等原因出现）投票无效并抛弃这条信息 
- 2.返回值Trem大于发送者。候选者状态变更为为follower 并更新任期term 
- 3.投票有效。统计有效投票的票数，当票数超过所有节点数的半数时候选节点当选为leader

当leader产生后，leader节点的计时器计时一个心跳周期，其余的follower节点仍计时超时周期。每过一个心跳周期，leader会向其余follower发起日志同步或心跳信号（当需要同步的日志为空时就是心跳信号，告知follower此时leader仍存在）。

程序已经通过了2A的所有测试。

## **lab2B:日志复制和同步**

preLogIndex指leader日志的最后一条的下标(注意是从0算起还是从1算起！！！) preLogTerm指leader日志最后一条的term

- 1.lient提供新的entry给leader，leader将其写入自己的log entry中
- 2.leader执行AppendEntries RPC通知所有的follower将新增的log写入各自的log entry中
- 3.当超过半数的follower成功写入新增的log后，leader将commitIndex增1，并将该log apply到自己的状态机
- 4.leader给client一个反馈，并通知所有的follower apply新增的log（通过leader.LeaderCommit控制 leader.LeaderCommit与commitIndex值一致）

新增日志时可能发生冲突:

-  如果preLogIndex大于当前follower日志的最大的下标说明follower缺失日志，此时拒绝新增日志
-  如果preLogIndex处的任期和preLogTerm不相等，那么说明日志存在conflict，拒绝附加日志

leader会维护一个nextIndex[]数组，记录了leader可以发送每一个follower的log index，初始化为leader最后一个log index加1。leader选举成功之后会立即给所有follower发送AppendEntries RPC（不包含任何log entry， 也充当心跳消息），心跳信息同步follower日志的过程：	

- 1.leader 初始化nextIndex[x]为 leader最后一个log index + 1
- 2.AppendEntries里prevLogTerm prevLogIndex来自 logs[nextIndex[x] - 1]：最后一个log entry
- 3.如果follower判断prevLogIndex位置的log term不等于prevLogTerm，那么返回 False，否则返回True
- 4.leader收到follower的回复，如果返回值是False，则nextIndex[x] -= 1, 跳转到2. 否则进入5
- 5.同步nextIndex[x]后的所有log entries

程序已经通过了2B的所有测试。

## lab2C:持久化

当节点出现crash时能够restore到原本的状态，根据论文要求当currentTerm、voteFor、log[]三个state发生改变时，就需要进行持久化操作。

日志发生冲突时需要回退：我采用append entry发生冲突时直接返回冲突的位置，即回退到上次applied的地方继续日志提交。达到减少RPC次数的效果

目前程序尚未通过2C的Figure 8 (unreliable)测试，其它测试均已经通过。修改思路为单开一个goroutine完成日志的commit。发生冲突时就对比leader日志，回退到二者相同的地方，剩下的操作交由commit协程完成。

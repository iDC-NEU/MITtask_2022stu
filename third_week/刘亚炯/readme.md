## lab3A:在Raft层之上实现一个key-value存储系统

**Server端的RPC方法实现**： 客户端会给服务器端发送RPC请求，lab3A中有两种RPC请求，分别对应Get操作和Put/Append操作。server接到请求时如果自己不是leader，则直接返回false，不需要执行任何操作，寻找正确leader这一工作交给client实现。如果是leader，则提交这个请求给Raft层。等请求完成后返回RPC响应，如果是Get请求则需要返回得到的值，Put/Append请求只需要返回Error。

**Server层与Raft层的交互**：在系统上执行的操作是所有server都执行，但提交请求和响应RPC只有当前的leader需要做。Raft层通过applyCh给上层server提交消息。使用一个单独的goroutine监听applyCh，这个goroutine需要将请求执行完成的消息通知给执行RPC方法的goroutine，因此server端还需要一个channel（记作waitChMap）来做这件事。

由于多个client可能会同时发送请求，在server端看来就会有多个goroutine分别处理每个RPC请求，这样监听applyCh的goroutine执行完一个请求后根据command index确定给哪个RPC处理goroutine发消息。这里的waitChMap是一个int-channel的map类型，每个command index对应一个channel。server提交请求时，会得到这条请求在raft log中的index，RPC处理goroutine监听的是这个index对应的channel。也就是将请求完成的消息提交给对应index的channel。

**在下面的情况下可能会出现请求重复执行和过长时间等待的问题：**

- client 1发来请求A，client 2发来请求B。但由于网络分区，A和B被不同的“leader”提交给Raft层，且都提交到了各自raft log中index = y的位置

- 在Raft共识后，所有节点上index = y这个位置的log entry全变成了请求A

- 监听applyCh的goroutine执行请求A，发送完成的消息给index = y对应的channel，这条消息被处理client 2请求的goroutine收到

  由于存在多个client，这样index不能唯一对应一条client请求，这里使用clientId和seqId来唯一确定一个请求。

**超时判断**：在上面的情况中，处理client 1 RPC请求的goroutine会永远等不到请求A执行成功的消息（虽然请求A已经成功执行了），因此还要加入一个超时机制，不要让某个处理RPC请求的goroutine永远等下去。这样RPC的处理过程变成了等待两种情况之一发生：

1. 这个index对应的channel收到了消息，说明请求执行完成。之后根据clientId + requestId判断执行完成的请求是不是这个client发来的请求
2. 达到了设置的超时时间，不用再等了，返回RPC

**避免重复执行**：当某请求超时后这条请求可能会被重新执行一次，但是在上述情况中这条请求已经成功执行了。所以在修改vk存储之前需要根据clientId和seqId来判断该操作是否重复，没有重复则对kv存储进行修改。注意：仅有put和append操作会影响kv存储的状态，这些操作需要进行重复判断，而get操作可以重复执行。
lab4的主要任务是在前几个lab的基础上构建一个支持分片操作的Key/Value数据库服务，每个分片由不同的raft组来服务，并且该数据库服务能够正确迁移数据。<br>

lab4A：
lab4A主要由shardctrler来完成，其本质就是一个集群管理器，它主要记录了当前每个 raft 组对应的的相关集群信息，具体实现和lab3很相似<br>
其主要有join，leave，move以及query四个操作：<br>
其中Join操作是加入集群，Leave操作是删除集群，Move操作是令某个集群分配给特定的分片，Query操作是查询配置信息<br>

lab4B：
lab4B主要由shardkv来完成，主要实现分片的Key/Value数据库服务。<br>
其中需要构建多个group，每个group需要管理一定数量（大于等于1）的分片（系统默认分片为10），并对其进行管理。<br>
同时也要设计完成数据的分片存储以及数据迁移的操作，并且保证一致性<br>


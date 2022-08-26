package shardkv

import (
	"6.824/shardctrler"
	"time"
)

func (kv *ShardKV) GetAndApplyConfig() {
	num := 1
	for true {
		kv.mu.RLock()
		num = kv.currentConfig.Num + 1
		kv.lastWakeNum = num
		kv.mu.RUnlock()
		kv.DPrintln("在查询前")
		conf := kv.ck.Query(num)
		kv.DPrintln("在查询后")
		// 如果分片没有变化
		kv.mu.RLock()
		if conf.Num != kv.currentConfig.Num+1 {
			kv.mu.RUnlock()
			select {
			case num = <-kv.getConfCh:
				kv.DPrintln("follower被唤醒, 要查询的为", num)
			case <-time.After(time.Duration(ConfigDuration) * time.Millisecond):
			}
			continue
		}
		kv.mu.RUnlock()
		//DPrintln("gid", kv.gid, "读取到了新的config", conf)
		_, isLeader := kv.rf.GetState()
		if isLeader {
			kv.rf.Start(Op{
				Operation: ConfOperation,
				NewConf:   conf,
			})
		}
		select {
		case num = <-kv.getConfCh:
			kv.DPrintln("leader被唤醒, 要查询的为", num)
		case <-time.After(time.Duration(ConfigDuration) * time.Millisecond):
		}

	}
}

func (kv *ShardKV) execConf(newConf shardctrler.Config) {
	DPrintln("gid", kv.gid, "CurrentShard", kv.currentConfig.Shards, "newShard", newConf.Shards)
	DPrintln("gid", kv.gid, "curr Conf", kv.currentConfig.Num, "New conf", newConf.Num, "send", kv.shardSending, "recv", kv.shardReceiving)
	if newConf.Num == kv.currentConfig.Num {
		return
	}
	// 如果当前config是0，则不用发送
	if kv.currentConfig.Num == 0 {
		for shardId, Gid := range newConf.Shards {
			if Gid == kv.gid {
				if _, ok := kv.kvStore[shardId]; !ok {
					kv.kvStore[shardId] = make(map[string]string)
				}
				if _, ok := kv.shardLastReq[shardId]; !ok {
					kv.shardLastReq[shardId] = make(map[int64]int64)
				}
			}
		}
	}
	if kv.currentConfig.Num == 0 && newConf.Num == 1 {
		kv.currentConfig = newConf
		return
	}

	// 上一个Config的还没有处理完成，拒绝下一个Config更新
	//for _, isReceiving := range kv.shardReceiving {
	//	if isReceiving {
	//		return
	//	}
	//}
	for i := 0; i < len(kv.shardReceiving); i++ {
		if kv.shardReceiving[i] || kv.shardSending[i] {
			return
		}
	}

	DPrintln("机器", kv.me, "Gid", kv.gid, "装载新Conf之前需要接收", kv.shardReceiving)
	for shardId, gid := range kv.currentConfig.Shards {
		//kv.shardSending[shardId] = false
		if gid == kv.gid {
			newGid := newConf.Shards[shardId]
			if newGid == gid {
				continue
			} else {
				kv.shardSending[shardId] = true
				DPrintln("机器", kv.me, "GID", kv.gid, "新config", newConf.Num, "需要发送的块", shardId)

			}
		} else if newConf.Shards[shardId] == kv.gid {
			kv.shardReceiving[shardId] = true
			DPrintln("机器", kv.me, "GID", kv.gid, "新config", newConf.Num, "需要接收的块", shardId)
		}
	}
	DPrintln("机器", kv.me, "Gid", kv.gid, "装载新Conf之后需要接收", kv.shardReceiving)
	DPrintln("机器", kv.me, "Gid", kv.gid, "装载新Conf之后需要发送", kv.shardSending)

	kv.currentConfig = newConf
	// 设置好当前正在处理的配置
	kv.sendShardCh <- newConf
}

func (kv *ShardKV) SendMoveShard() {
	//for true {
	//	if _, isLeader := kv.rf.GetState(); !isLeader {
	//		select {
	//		case <-kv.sendShardCh:
	//		case <-time.After(time.Duration(ConfigDuration) * time.Millisecond):
	//		}
	//		continue
	//	}
	//	sendData := kv.getSendData()
	//	if len(sendData) == 0 {
	//		select {
	//		case <-kv.sendShardCh:
	//		case <-time.After(time.Duration(ConfigDuration) * time.Millisecond):
	//		}
	//		continue
	//	}
	//	for gid, sendArg := range sendData {
	//		go kv.sendShard(gid, sendArg)
	//	}
	//	select {
	//	case <-kv.sendShardCh:
	//	case <-time.After(time.Duration(ConfigDuration) * time.Millisecond):
	//	}
	//}
	sendingNum := 0
	lastSend := -1
	isFirstSend := true
	for true {
		select {
		case conf := <-kv.sendShardCh:
			sendingNum = conf.Num
		case <-time.After(100 * time.Millisecond):
		}
		if _, isLeader := kv.rf.GetState(); !isLeader {
			isFirstSend = true
			continue
		}

		// 查看是否已经发送过
		if lastSend == sendingNum && !isFirstSend {
			// 已经发送过了，但是经历了leader-->follower-->leader的过程，所以，可能之前的并没有进行确认
			continue
		}

		kv.mu.RLock()
		sendData := kv.getSendData()
		if len(sendData) == 0 {
			kv.mu.RUnlock()
			continue
		}
		kv.mu.RUnlock()
		for gid, sendArg := range sendData {
			go kv.sendShard(gid, sendArg)
		}
		lastSend = sendingNum
		isFirstSend = false
	}
	//for _ = range kv.sendShardCh {
	//	if _, isLeader := kv.rf.GetState(); !isLeader {
	//		continue
	//	}
	//	kv.mu.RLock()
	//	sendData := kv.getSendData()
	//	if len(sendData) == 0 {
	//		kv.mu.RUnlock()
	//		continue
	//	}
	//	kv.mu.RUnlock()
	//	for gid, sendArg := range sendData {
	//		go kv.sendShard(gid, sendArg)
	//	}
	//}
}

func (kv *ShardKV) getSendData() map[int]MoveArgs {
	sendData := make(map[int]MoveArgs)
	for shardId, isSend := range kv.shardSending {
		if isSend {
			newGid := kv.currentConfig.Shards[shardId]
			if _, ok := sendData[newGid]; !ok {
				sendData[newGid] = MoveArgs{
					ConfigNum:    kv.currentConfig.Num,
					KvShard:      make(map[int]map[string]string),
					ShardLastReq: make(map[int]map[int64]int64),
				}
			}
			sendData[newGid].KvShard[shardId] = kv.copyStringMap(kv.kvStore[shardId])
			sendData[newGid].ShardLastReq[shardId] = kv.copyInt64Map(kv.shardLastReq[shardId])
		}
	}
	return sendData
}

func (kv *ShardKV) sendShard(gid int, args MoveArgs) {
	kv.mu.RLock()
	servers := kv.currentConfig.Groups[gid]
	kv.mu.RUnlock()
	reply := MoveReply{}
	index := 0
	keys := make([]int, 0, len(args.KvShard))
	for key := range args.KvShard {
		keys = append(keys, key)
	}
	for {
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			DPrintln("机器", kv.me, "config", args.ConfigNum, "发送", keys, "给", gid, "server", servers[index], "因为不是Leader退出不发")
			return
		}
		serverRPC := kv.make_end(servers[index])
		DPrintln("机器", kv.me, "config", args.ConfigNum, "发送", keys, "给", gid, "server", servers[index])
		ok := serverRPC.Call("ShardKV.InstallShard", &args, &reply)
		DPrintln("机器", kv.me, "gid", kv.gid, "发送", keys, "给", gid, "server", servers[index], "结果", reply)

		if reply.Err == OK {
			//kv.mu.Lock()
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				return
			}
			kv.rf.Start(Op{Operation: SendConfirmOperation, SendComNum: keys})
			//for shardId := range args.KvShard {
			//	kv.shardSending[shardId] = false
			//	delete(kv.kvStore, shardId)
			//}
			//kv.mu.Unlock()
			return
		} else if reply.Err == ErrWrongLeader || !ok {
			index = (index + 1) % len(servers)
		}
	}

}

func (kv *ShardKV) InstallShard(args *MoveArgs, reply *MoveReply) {

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	keys := make([]int, 0)
	for key := range args.KvShard {
		keys = append(keys, key)
	}
	DPrintln("机器", kv.me, "gid", kv.gid, "confNum", kv.currentConfig.Num, "一开始收到",
		args.ConfigNum, "的", keys)
	kv.mu.RLock()
	DPrintln("机器", kv.me, "gid", kv.gid, "confNum", kv.currentConfig.Num, "进入锁收到",
		args.ConfigNum, "的", keys)
	myConfNum := kv.currentConfig.Num
	if myConfNum < args.ConfigNum {
		reply.Err = ConfigTooNew
		isWakeup := false
		if myConfNum+1 > kv.lastWakeNum {
			kv.lastWakeNum = myConfNum + 1
			isWakeup = true
		}
		kv.mu.RUnlock()
		DPrintln("机器", kv.me, "gid", kv.gid, "收到", kv.configingNum, "before getConfCh")
		if isWakeup {
			kv.getConfCh <- myConfNum + 1
		}
		DPrintln("机器", kv.me, "gid", kv.gid, "收到", kv.configingNum, "after getConfCh")
		// TODO: 这里添加唤醒读取Config的操作
		return
	}
	shards := make([]int, 0, 10)
	shardMap := make(map[int]map[string]string)
	reqMap := make(map[int]map[int64]int64)
	for shardId := range args.KvShard {
		shards = append(shards, shardId)
		if kv.shardReceiving[shardId] != true {
			delete(args.KvShard, shardId)
			delete(args.ShardLastReq, shardId)
		} else {
			shardMap[shardId] = kv.copyStringMap(args.KvShard[shardId])
			reqMap[shardId] = kv.copyInt64Map(args.ShardLastReq[shardId])
		}
	}
	DPrintln("机器", kv.me, "gid", kv.gid, "confNum", kv.currentConfig.Num, "收到",
		args.ConfigNum, "的", shards)
	kv.mu.RUnlock()
	if len(args.KvShard) == 0 {
		reply.Err = OK
		return
	}

	command := Op{
		Operation: InstallShardOperation,
		ShardMap:  shardMap,
		ConfigNum: args.ConfigNum,
		ReqMap:    reqMap,
	}

	index, _, _ := kv.rf.Start(command)
	if index == -1 {
		reply.Err = ErrWrongLeader
		return
	}

	kv.indexCondMutex.Lock()
	if _, ok := kv.indexCond[index]; !ok {
		kv.indexCond[index] = make(chan Op, 2)
	}
	commandChan := kv.indexCond[index]
	kv.indexCondMutex.Unlock()

	select {
	case <-commandChan:
		reply.Err = OK
	case <-time.After(time.Duration(Timeout) * time.Millisecond):
		reply.Err = ErrWrongLeader
	}

	close(commandChan)
	kv.indexCondMutex.Lock()
	delete(kv.indexCond, index)
	kv.indexCondMutex.Unlock()

}

func (kv *ShardKV) execInstallShard(configNum int, shardMap map[int]map[string]string, reqMap map[int]map[int64]int64) {
	if kv.currentConfig.Num != configNum {
		return
	}
	for shardId, shardStore := range shardMap {
		if kv.shardReceiving[shardId] == false {
			continue
		}
		kv.kvStore[shardId] = shardStore
		kv.shardLastReq[shardId] = reqMap[shardId]
		kv.shardReceiving[shardId] = false
		DPrintln("\n\n机器", kv.me, "gid", kv.gid, "config", kv.currentConfig.Num, "收到分片", shardId, "待接收", kv.shardReceiving)
	}
}

func (kv *ShardKV) copyStringMap(oldMap map[string]string) map[string]string {
	newMap := make(map[string]string)
	for key, value := range oldMap {
		newMap[key] = value
	}
	return newMap
}

func (kv *ShardKV) copyInt64Map(oldMap map[int64]int64) map[int64]int64 {
	newMap := make(map[int64]int64)
	for key, value := range oldMap {
		newMap[key] = value
	}
	return newMap
}

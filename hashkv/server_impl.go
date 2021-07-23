package hashkv

import (
	"fmt"

	"umich.edu/eecs491/proj4/common"
)

const DEBUG = false

//
// additions to HashKV state
//
type HashKVImpl struct {
	// shard index -> all served requestIDs of the keys in this shard
	shard2requestIDs map[int][]int64
	key2value map[string]string
	// only the served requestIDs of the keys in this server's shards
	requestIDs map[int64]bool
	dead bool
	
}


// x must be 0-15
func add1(x int) int{
	if x != common.NShards - 1{
		return x + 1
	}
	return 0
}

func inrange(start int, end int, index int) bool{
    if start == -1{
		return false
	}
	if start == end{
		return start == index
	}
	if start < end{
		return start <= index && index <= end
	}
	return (start <= index && index <= common.NShards - 1) || (0 <= index && index <= end)
}

//
// initialize kv.impl.*
//
func (kv *HashKV) InitImpl() {
	kv.impl.shard2requestIDs = make(map[int][]int64)
	kv.impl.key2value = make(map[string]string)
	kv.impl.requestIDs = make(map[int64]bool)
	kv.impl.dead = false
	
	// if this is the first server
	if kv.predID == kv.ID && kv.ID == kv.succID{
		if DEBUG{
			fmt.Printf("DEBUG: server %s is the first server\n", kv.me)
		}
        return
	}
    
	// otherwise need to tell pred and succ
	
	// tell pred
	// it's fine even it has changed the pred'state but pred's reply got dropped
	for true{
		args := &UpdateArgs{ServerAddr: kv.me, ServerID: kv.ID, Store: nil, Record: nil, Shard2requestIDs: nil, NewServer: true}
		reply := &UpdateReply{}
		result := common.Call(kv.pred, "HashKV.SuccUpdate", args, reply)
		if result{
			break
		}
	}
    
	// tell succ
	// if it has changed the pred's state but pred's reply got dropped, 
	// need to utilize ShardStart and ShardEnd to let pred know so that the pred still give us the right data
	shardStart := add1(common.Key2Shard(kv.predID))
	shardEnd := common.Key2Shard(kv.ID)
	if common.Key2Shard(kv.predID) == common.Key2Shard(kv.ID) && kv.ID > kv.predID{
		shardStart = -1
		shardEnd = -1
	}
	for true{
		args := &UpdateArgs{ServerAddr: kv.me, ServerID: kv.ID, Store: nil, Record: nil, Shard2requestIDs: nil, NewServer: true,
		                    ShardStart: shardStart, ShardEnd: shardEnd}
		reply := &UpdateReply{}
		result := common.Call(kv.succ, "HashKV.PredUpdate", args, reply)
        if result{
            kv.impl.key2value = reply.Store
			kv.impl.requestIDs = reply.Record
			kv.impl.shard2requestIDs = reply.Shard2requestIDs
			break;
		}
	}

	if DEBUG{
		fmt.Printf("DEBUG: server %s now have shard %d to %d, pred: %s\n", 
		    kv.me, shardStart, shardEnd, kv.pred)
	}
}

//
// hand off shards before shutting down
//
func (kv *HashKV) KillImpl() {
	kv.mu.Lock()
	defer kv.mu.Unlock()


	kv.impl.dead = true
	// if this is the last live server
	if kv.predID == kv.ID && kv.ID == kv.succID{
		if DEBUG{
			fmt.Printf("DEBUG: server %s died, this is the last server\n", kv.me)
		}
		return
	}
    
    // tell pred
	// it's fine even it has changed the pred'state but pred's reply got dropped
	for true{
		args := &UpdateArgs{ServerAddr: kv.succ, ServerID: kv.succID, Store: nil, Record: nil, Shard2requestIDs: nil, NewServer: false}
		reply := &UpdateReply{}
		result := common.Call(kv.pred, "HashKV.SuccUpdate", args, reply)
		if result{
			break
		}
	}
    
    // tell succ
	// if it has changed the pred's state but pred's reply got dropped, 
	// need to utilize ShardStart and ShardEnd to let pred know so that the pred still give us the right data
	shardStart := add1(common.Key2Shard(kv.predID))
	shardEnd := common.Key2Shard(kv.ID)
    if common.Key2Shard(kv.predID) == common.Key2Shard(kv.ID) && kv.ID > kv.predID{
		shardStart = -1
		shardEnd = -1
	}

	store := make(map[string]string)
	record := make(map[int64]bool)
	shard2requestIDs := make(map[int][]int64)	
	for key, val := range kv.impl.key2value{
		shardIndex := common.Key2Shard(key)
		if inrange(shardStart, shardEnd, shardIndex){
			store[key] = val
		}
	}
	if shardStart >= 0 {   
		i:= shardStart
        for ; i != shardEnd; i = add1(i){
			shard2requestIDs[i] = kv.impl.shard2requestIDs[i]
			for _, val := range kv.impl.shard2requestIDs[i]{
				record[val] = true
			}
		}
		shard2requestIDs[i] = kv.impl.shard2requestIDs[i]
		for _, val := range kv.impl.shard2requestIDs[i]{
			record[val] = true
		}
	}
	
	for true{
        args := &UpdateArgs{ServerAddr: kv.pred, ServerID: kv.predID, Store: store, Record: record, 
		                    Shard2requestIDs: shard2requestIDs, NewServer: false,
							ShardStart: shardStart, ShardEnd: shardEnd}
		reply := &UpdateReply{}
		result := common.Call(kv.succ, "HashKV.PredUpdate", args, reply)
		if result{
			break
		}
	}
    if DEBUG{
		fmt.Printf("DEBUG: server %s died, which had shard %d to %d\n", kv.me, shardStart, shardEnd)
	}
}

//
// RPC handler for client Get requests
//
func (kv *HashKV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
    
	if kv.impl.dead{
		return fmt.Errorf("Dead Server!")
	}

	// check if it's in range
	shardStart := add1(common.Key2Shard(kv.predID))
	shardEnd := common.Key2Shard(kv.ID)
	if common.Key2Shard(kv.predID) == common.Key2Shard(kv.ID) && kv.ID > kv.predID{
		shardStart = -1
		shardEnd = -1
	}

	shardIndex := common.Key2Shard(args.Key)
	if inrange(shardStart, shardEnd, shardIndex){
        _, exist := kv.impl.key2value[args.Key]
		if exist{
			reply.Err = OK
			reply.Value = kv.impl.key2value[args.Key]
		}else{
			reply.Err = ErrNoKey
		}
	}else{
        reply.Err = ErrWrongServer
		// if DEBUG{
		// 	fmt.Printf("DEBUG: server %s have shard %d to %d  not responsible for %d", 
		// 	    kv.me, add1(common.Key2Shard(kv.predID)), add1(common.Key2Shard(kv.ID)), shardIndex)
		// }
	}
	return nil
}

//
// RPC handler for client Put and Append requests
//
func (kv *HashKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
    kv.mu.Lock()
	defer kv.mu.Unlock()

    if kv.impl.dead{
		return fmt.Errorf("Dead Server!")
	}

	// check if it's in range
	shardStart := add1(common.Key2Shard(kv.predID))
	shardEnd := common.Key2Shard(kv.ID)
	if common.Key2Shard(kv.predID) == common.Key2Shard(kv.ID) && kv.ID > kv.predID{
		shardStart = -1
		shardEnd = -1
	}

	shardIndex := common.Key2Shard(args.Key)
    
	if inrange(shardStart, shardEnd, shardIndex){
        // check if already served
		_, exist := kv.impl.requestIDs[args.Impl.RequestID]
		if exist{
			reply.Err = ErrAlreadyServed
			return nil
		}

		kv.impl.requestIDs[args.Impl.RequestID] = true
		_, exist = kv.impl.shard2requestIDs[shardIndex]
		if !exist{
			kv.impl.shard2requestIDs[shardIndex] = make([]int64, 0)
		}
		kv.impl.shard2requestIDs[shardIndex] = append(kv.impl.shard2requestIDs[shardIndex], args.Impl.RequestID)
		if args.Op == "Put"{
            kv.impl.key2value[args.Key] = args.Value
		}else{
			kv.impl.key2value[args.Key] += args.Value
		}
		reply.Err = OK
	}else{
		reply.Err = ErrWrongServer
		// if DEBUG{
		// 	fmt.Printf("DEBUG: server %s have shard %d to %d  not responsible for %d", 
		// 	    kv.me, add1(common.Key2Shard(kv.predID)), add1(common.Key2Shard(kv.ID)), shardIndex)
		// }
	}

	return nil
}

//
// Add RPC handlers for any other RPCs you introduce
//

// need to update successor of current server
func (kv *HashKV) SuccUpdate(args *UpdateArgs, reply *UpdateReply) error{ 
	kv.mu.Lock()
	defer kv.mu.Unlock()
    
	kv.succ = args.ServerAddr
	kv.succID = args.ServerID

	return nil
}

func (kv *HashKV) PredUpdate(args *UpdateArgs, reply *UpdateReply) error{
    kv.mu.Lock()
	defer kv.mu.Unlock()

    // if it because a new server is added
	// if a request comes in before the state changes, this server will update data and transfer data to added server
	// if a request comes in after the state changes, but the reply got dropped, this server will reject the request
	//     this server will then transfer the old data to the added server, and the client will finally find the added server and update the data
	if args.NewServer{
        kv.pred = args.ServerAddr
		kv.predID = args.ServerID

        // transfer data
		reply.Store = make(map[string]string)
		for key, val := range kv.impl.key2value{
			shardIndex := common.Key2Shard(key)
			if inrange(args.ShardStart, args.ShardEnd, shardIndex){
				reply.Store[key] = val
			} 
		}

		reply.Record = make(map[int64]bool)
		reply.Shard2requestIDs = make(map[int][]int64)
		if args.ShardStart >= 0 {
			i := args.ShardStart
			for ; i != args.ShardEnd; i = add1(i){
				reply.Shard2requestIDs[i] = kv.impl.shard2requestIDs[i]
				for _, requestID := range kv.impl.shard2requestIDs[i]{
					reply.Record[requestID] = true
				}
			}
			reply.Shard2requestIDs[i] = kv.impl.shard2requestIDs[i]
			for _, requestID := range kv.impl.shard2requestIDs[i]{
				reply.Record[requestID] = true
			}
		}

		if DEBUG{
			shardStart := add1(common.Key2Shard(kv.predID))
			shardEnd := common.Key2Shard(kv.ID)
			if common.Key2Shard(kv.predID) == common.Key2Shard(kv.ID) && kv.ID > kv.predID{
				shardStart = -1
				shardEnd = -1
			}
			fmt.Printf("DEBUG: server %s now have shard %d to %d\n", kv.me, shardStart, shardEnd)
		}
		
	}else{
	// if it because a server is died
	    
		// check if we have already changed the state
        if kv.predID == args.ServerID{
			return nil
		}

		kv.pred = args.ServerAddr
		kv.predID = args.ServerID
        
		for key, val := range args.Store{
		    kv.impl.key2value[key] = val
		}

		for key, val := range args.Shard2requestIDs{
			kv.impl.shard2requestIDs[key] = val
			for _, requestID := range val{
				kv.impl.requestIDs[requestID] = true
			}
		}

		if DEBUG{
			shardStart := add1(common.Key2Shard(kv.predID))
			shardEnd := common.Key2Shard(kv.ID)
			if common.Key2Shard(kv.predID) == common.Key2Shard(kv.ID) && kv.ID > kv.predID{
				shardStart = -1
				shardEnd = -1
			}
			fmt.Printf("DEBUG: server %s now have shard %d to %d\n", kv.me, shardStart, shardEnd)
		}
        
	}

	return nil
}
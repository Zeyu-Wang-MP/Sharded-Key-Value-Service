package shardkv

import (
	_"fmt"

	"umich.edu/eecs491/proj4/common"
)

//
// Define what goes into "value" that Paxos is used to agree upon.
// Field names must start with capital letters
//
type Op struct {
	OpId int64 // used to compare different Op
	
	Type string // can be "Get", "PutAppend", "GiveDataTo", "ReceiveData"
	
	// used in PutAppend
	Key string
	Val string
    Operation string // can be "Put" "Append"
	RequestID int64 // PutAppend request ID

	// used in GiveDataTo
	GiveShardIndex int // when it = -1, specical case that this group has all shards
	ServerAddrs []string
	GiveDataToRPCID int64
	GiveDataMe int // used to only let the rpc receiver call ReceiveData rpc rather than all replicas, other just update state not transfer data

	// used in ReceiveData
	ReceiveDataRPCID int64
	ReceivedShardIndex int
	ReceivedKey2val map[string]string
	ReceivedRequestIDs []int64
}

//
// Method used by PaxosRSM to determine if two Op values are identical
//
func equals(v1 interface{}, v2 interface{}) bool {
    op1, _ := v1.(Op)
	op2, _ := v2.(Op)
    return op1.OpId == op2.OpId && op1.RequestID == op2.RequestID && 
	    op1.GiveDataToRPCID == op2.GiveDataToRPCID && op1.ReceiveDataRPCID == op2.ReceiveDataRPCID
}

//
// additions to ShardKV state
//
type ShardKVImpl struct {
	key2Val map[string]string
	// only for my responsible shards
	requestIDs map[int64]bool
	shard2requestIDs map[int][]int64
    // my shards
	shards map[int]bool
    
    // used to avoid receiving other group's data twice
	ReceiveDataRPCIDs map[int64]bool
	// used to avoid execute master's GiveDataTo RPC twice
	GiveDataToRPCIDs map[int64]bool
}

//
// initialize kv.impl.*
//
func (kv *ShardKV) InitImpl() {
	kv.impl.key2Val = make(map[string]string)
	kv.impl.requestIDs = make(map[int64]bool)
	kv.impl.shard2requestIDs = make(map[int][]int64)
	kv.impl.shards = make(map[int]bool)

    kv.impl.ReceiveDataRPCIDs = make(map[int64]bool)
	kv.impl.GiveDataToRPCIDs = make(map[int64]bool)
}

//
// RPC handler for client Get requests
//
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
    kv.mu.Lock()
	defer kv.mu.Unlock()
    
	// use addop to get the latest state
	kv.rsm.AddOp(Op{OpId: common.Nrand(),Type: "Get"})
	
	if _, exist := kv.impl.shards[common.Key2Shard(args.Key)]; !exist{
        reply.Err = ErrWrongGroup
		return nil
	}

	if _, exist := kv.impl.key2Val[args.Key]; !exist{
		reply.Err = ErrNoKey
		return nil
	}

	reply.Err = OK
	reply.Value = kv.impl.key2Val[args.Key]
	return nil
}

//
// RPC handler for client Put and Append requests
//
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
    
	// used to get the latest state, and also do the putappend(can be invalid due to shard or already served)
	kv.rsm.AddOp(Op{OpId: common.Nrand(), Type: "PutAppend", Key: args.Key, Val: args.Value, Operation: args.Op, RequestID: args.Impl.RequestID})
    
	// check if this key is in our shards
	if _, exist := kv.impl.shards[common.Key2Shard(args.Key)]; !exist{
		reply.Err = ErrWrongGroup
		return nil
	}

	// if this request already get served, still reply OK
	reply.Err = OK
	return nil
}


// RPC called by shardmaster
func (kv *ShardKV) GiveDataTo(args *common.GiveDataToArgs, reply *common.GiveDataToReply) error{
    kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.rsm.AddOp(Op{OpId: common.Nrand(), Type: "GiveDataTo", GiveShardIndex: args.ShardIndex, 
	    ServerAddrs: args.ServerAddrs, GiveDataToRPCID: args.RPCID, GiveDataMe: kv.me})
	return nil
}


// RPC called by other shardKV
func (kv *ShardKV) ReceiveData(args *ReceiveDataArgs, reply *ReceiveDataReply) error{
    kv.mu.Lock()
	defer kv.mu.Unlock()

    kv.rsm.AddOp(Op{OpId: common.Nrand(), Type: "ReceiveData", ReceiveDataRPCID: args.RPCID, ReceivedShardIndex: args.ShardIndex, 
	    ReceivedKey2val: args.Key2val, ReceivedRequestIDs: args.RequestIDs})
	return nil
}

//
// Execute operation encoded in decided value v and update local state
//
func (kv *ShardKV) ApplyOp(v interface{}) {
	op, _ := v.(Op)
	if op.Type == "PutAppend"{
		kv.PutAppendImpl(op)
	} else if op.Type == "GiveDataTo"{
		kv.GiveDataToImpl(op)
	} else if op.Type == "ReceiveData"{
		kv.ReceiveDataImpl(op)
	}
}


func (kv *ShardKV) PutAppendImpl(op Op){
    // check if the key is in the shards that we are responsible
	if _, exist := kv.impl.shards[common.Key2Shard(op.Key)]; !exist{
		return
	}

	// check if we already served this request
	if _, exist := kv.impl.requestIDs[op.RequestID]; exist{
		return
	}

	// update state
	if op.Operation == "Put"{
		kv.impl.key2Val[op.Key] = op.Val
	} else{
		kv.impl.key2Val[op.Key] += op.Val
	}
	kv.impl.requestIDs[op.RequestID] = true
	shardIndex := common.Key2Shard(op.Key)
	if _, exist := kv.impl.shard2requestIDs[shardIndex]; !exist{
		kv.impl.shard2requestIDs[shardIndex] = make([]int64, 0)
	}
	kv.impl.shard2requestIDs[shardIndex] = append(kv.impl.shard2requestIDs[shardIndex], op.RequestID)
}

func (kv *ShardKV) GiveDataToImpl(op Op){
    // check if we already served this request
	if _, exist := kv.impl.GiveDataToRPCIDs[op.GiveDataToRPCID]; exist{
		return
	}
    
	// update state
	kv.impl.GiveDataToRPCIDs[op.GiveDataToRPCID] = true
    
	// special case when shardIndex == -1
	if op.GiveShardIndex == -1{
        for i := 0; i < common.NShards; i++{
			kv.impl.shards[i] = true
		}
		return
	}

    // remove shard 
	delete(kv.impl.shards, op.GiveShardIndex)

	// transfer data only if I am the server who receive the RPC from the shardmaster
	if kv.me == op.GiveDataMe{
		dataToTransfer := make(map[string]string)
		for key, val := range kv.impl.key2Val{
			if common.Key2Shard(key) == op.GiveShardIndex{
				dataToTransfer[key] = val
			}
		}
		args := ReceiveDataArgs{RPCID: common.Nrand(), ShardIndex: op.GiveShardIndex, 
		    Key2val: dataToTransfer, RequestIDs: kv.impl.shard2requestIDs[op.GiveShardIndex]}
		reply := ReceiveDataReply{}

		success := false
		for !success{
			for _, receiverAddr := range op.ServerAddrs{
				result := common.Call(receiverAddr, "ShardKV.ReceiveData", &args, &reply)
				if result{
					success = true
					break
				}
			}
		}
		
	}
}

func (kv *ShardKV) ReceiveDataImpl(op Op){
    // check if we already served this request
	if _, exist := kv.impl.ReceiveDataRPCIDs[op.ReceiveDataRPCID]; exist{
		return
	}

	// update state
	kv.impl.ReceiveDataRPCIDs[op.ReceiveDataRPCID] = true
    for key, val := range op.ReceivedKey2val{
		kv.impl.key2Val[key] = val
	}
	kv.impl.shard2requestIDs[op.ReceivedShardIndex] = op.ReceivedRequestIDs
	for _, requestId := range op.ReceivedRequestIDs{
		kv.impl.requestIDs[requestId] = true
	}
	kv.impl.shards[op.ReceivedShardIndex] = true
}
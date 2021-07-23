package shardkv

// Field names must start with capital letters,
// otherwise RPC will break.

//
// additional state to include in arguments to PutAppend RPC.
//
type PutAppendArgsImpl struct {
	RequestID int64
}

//
// additional state to include in arguments to Get RPC.
//
type GetArgsImpl struct {
}

//
// for new RPCs that you add, declare types for arguments and reply
//

type ReceiveDataArgs struct{
    RPCID int64
	ShardIndex int // which shard I give you
	Key2val map[string]string
	RequestIDs []int64 // all requests ID in this shard
}

type ReceiveDataReply struct{
}
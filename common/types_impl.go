package common

//
// define here any data types that you need to access in two packages without
// creating circular dependencies
//
type GiveDataToArgs struct{
	ShardIndex int // which shard to give
    ServerAddrs []string // who to give
	RPCID int64
}

type GiveDataToReply struct{
}
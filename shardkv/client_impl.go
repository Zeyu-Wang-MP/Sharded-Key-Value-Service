package shardkv

import (
	"time"

	"umich.edu/eecs491/proj4/common"
)

//
// additions to Clerk state
//
type ClerkImpl struct {
}

//
// initialize ck.impl.*
//
func (ck *Clerk) InitImpl() {
}

//
// fetch the current value for a key.
// return "" if the key does not exist.
// keep retrying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	shardIndex := common.Key2Shard(key)
    args := GetArgs{Key: key}
	reply := GetReply{}
	for{
		currConfig := ck.sm.Query(-1)
		for _, serverAddr := range currConfig.Groups[currConfig.Shards[shardIndex]]{
            if common.Call(serverAddr, "ShardKV.Get", &args, &reply){
				if reply.Err == OK{
					return reply.Value
				} else if reply.Err == ErrNoKey{
					return ""
				} else if reply.Err == ErrWrongGroup{
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

//
// send a Put or Append request.
// keep retrying forever until success.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	shardIndex := common.Key2Shard(key)
	args := PutAppendArgs{Key: key, Value: value, Op: op, Impl: PutAppendArgsImpl{RequestID: common.Nrand()}}
	reply := PutAppendReply{}

	for{
		currConfig := ck.sm.Query(-1)
		for _, serverAddr := range currConfig.Groups[currConfig.Shards[shardIndex]]{
			if common.Call(serverAddr, "ShardKV.PutAppend", &args, &reply){
				if reply.Err == OK{
					return
				} else if reply.Err == ErrWrongGroup{
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

package hashkv

import "umich.edu/eecs491/proj4/common"
import "time"

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

	for {
		for _, server := range ck.servers {
			getArgs := GetArgs{key, GetArgsImpl{}}
			getReply := GetReply{}

			if common.Call(server, "HashKV.Get", &getArgs, &getReply) {
				if getReply.Err == OK {
					return getReply.Value
				} else if getReply.Err == ErrNoKey{
					return ""
				}
			}
			// Otherwise, getReply.Err must be ErrWrongServer, so try the next server
			time.Sleep(time.Millisecond * 10)
		}
	}
}

//
// send a Put or Append request.
// keep retrying forever until success.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
    requestID := common.Nrand()
	putArgs := PutAppendArgs{key, value, op, PutAppendArgsImpl{RequestID: requestID}}
	putReply := PutAppendReply{}

	for {
		for _, server := range ck.servers {
			if common.Call(server, "HashKV.PutAppend", &putArgs, &putReply) {
				if putReply.Err == OK {
					return
				} else if putReply.Err == ErrAlreadyServed{
					return
				}
			}
			// Otherwise, getReply.Err must be ErrWrongServer, so try the next server
			time.Sleep(time.Millisecond * 10)
		}
	}
}

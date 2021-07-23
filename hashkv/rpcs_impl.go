package hashkv

// Field names must start with capital letters,
// otherwise RPC will break.
const ErrAlreadyServed = "ErrAlreadyServed"
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

type UpdateArgs struct{
	ServerAddr string
	ServerID string
	
    // used when a server died and need to give data
	Store map[string]string
    Record map[int64]bool
	Shard2requestIDs map[int][]int64
	
	// tell if this RPC happens because a new server added or removed
	NewServer bool
    
	// [start, end]
    ShardStart int
	ShardEnd int
}

type UpdateReply struct{
	Store map[string]string
	Record map[int64]bool
	Shard2requestIDs map[int][]int64
}


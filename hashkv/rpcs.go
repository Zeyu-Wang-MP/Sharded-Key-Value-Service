package hashkv

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	Impl  PutAppendArgsImpl
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key  string
	Impl GetArgsImpl
}

type GetReply struct {
	Err   Err
	Value string
}

package hashkv

import (
	"sync"
)

type Clerk struct {
	mu      sync.Mutex
	servers []string
	impl    ClerkImpl
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.InitImpl()
	return ck
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

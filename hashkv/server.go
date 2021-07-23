package hashkv

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
)


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type HashKV struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing

	pred, me, succ     string
	predID, ID, succID string

	impl HashKVImpl
}

// tell the server to shut itself down.
func (kv *HashKV) kill() {
	kv.KillImpl()

	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
}

// call this to find out if the server is dead.
func (kv *HashKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

func (kv *HashKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *HashKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a hashkv server.
// servers[] is a slice which contains the ports of the predecessor, this
//     server, and the successor, in that order.
// IDs[] is a slice which contains the IDs of the predecessor, this
//     server, and the successor, in that order.
//
func StartServer(servers []string, IDs []string) *HashKV {
	kv := new(HashKV)
	kv.pred = servers[0]
	kv.predID = IDs[0]
	kv.me = servers[1]
	kv.ID = IDs[1]
	kv.succ = servers[2]
	kv.succID = IDs[2]
	kv.InitImpl()

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	os.Remove(kv.me)
	l, e := net.Listen("unix", kv.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("HashKV(%v) accept: %v\n", kv.me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}

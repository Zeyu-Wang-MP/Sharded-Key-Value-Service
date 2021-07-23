package paxos

import (
	"math"
	"math/rand"
	"sync"
	"time"

	"umich.edu/eecs491/proj4/common"
)

type Instance struct {
	// proposer part
	status   Fate
	pMutex   sync.Mutex
	highestN int64
	currN    int64
	val      interface{}

	// acceptor part
	aMutex sync.Mutex
	np     int64
	na     int64
	va     interface{}
}

func makeInstance(n int64) *Instance {
	res := &Instance{}

	res.status = Pending
	res.highestN = 0
	res.currN = n
	res.val = nil

	res.np = -1
	res.na = -1
	res.va = nil
	return res
}

//
// additions to Paxos state.
//
type PaxosImpl struct {
	instances      []*Instance
	currStartIndex int
	maxKnownSeq    int
	doneSeqs       []int
}

//
// your px.impl.* initializations here.
//
func (px *Paxos) initImpl() {
	px.impl.currStartIndex = 0
	px.impl.instances = make([]*Instance, 0)
	px.impl.maxKnownSeq = -1
	px.impl.doneSeqs = make([]int, len(px.peers))
	for i := 0; i < len(px.peers); i++ {
		px.impl.doneSeqs[i] = -1
	}
}

func (px *Paxos) propose(seq int, ins *Instance, val interface{}) {
	ins.pMutex.Lock()
	defer ins.pMutex.Unlock()

	if ins.status != Pending {
		return
	}

	for ins.status != Decided && !px.isdead() {
		for ins.currN <= ins.highestN {
			ins.currN += int64(len(px.peers))
		}
		// send prepare to all peers
		okCount := 0
		var highestNa int64 = -1
		highestVa := val
		for i, addr := range px.peers {
			px.mu.Lock()
			args := &PrepareArgs{Seq: seq, N: ins.currN, Me: px.me, MaxDone: px.impl.doneSeqs[px.me]}
			px.mu.Unlock()

			reply := &PrepareReply{}
			var valid bool
			if i == px.me {
				px.Prepare(args, reply)
				valid = true
			} else {
				valid = common.Call(addr, "Paxos.Prepare", args, reply)
			}
			// when we got the response, check if we can do gc
			if valid {
				px.checkGC(i, reply.MaxDone)
			}
			// if rejected, update the highest n seen
			if valid && reply.Status == Reject {
				if ins.highestN < reply.HighestN {
					ins.highestN = reply.HighestN
				}
			}
			if valid && reply.Status == OK {
				okCount++
				if reply.Na > highestNa {
					highestNa = reply.Na
					highestVa = reply.Va
				}
			}
		}
		if okCount <= len(px.peers)/2 {
			ins.pMutex.Unlock()
			time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
			ins.pMutex.Lock()
			continue
		}

		// send accpet to all peers
		okCount = 0
		for i, addr := range px.peers {
			px.mu.Lock()
			args := &AcceptArgs{Seq: seq, N: ins.currN, Val: highestVa, Me: px.me, MaxDone: px.impl.doneSeqs[px.me]}
			px.mu.Unlock()

			reply := &AcceptReply{}
			var valid bool
			if i == px.me {
				px.Accept(args, reply)
				valid = true
			} else {
				valid = common.Call(addr, "Paxos.Accept", args, reply)
			}

			if valid {
				px.checkGC(i, reply.MaxDone)
			}

			if valid && reply.Status == OK {
				okCount++
			}
		}
		if okCount <= len(px.peers)/2 {
			ins.pMutex.Unlock()
			time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
			ins.pMutex.Lock()
			continue
		}
		// if decided
		ins.status = Decided
		ins.val = highestVa
		px.updateMaxKnown(seq)

		// send decided to all peers
		for i, addr := range px.peers {
			px.mu.Lock()
			args := &DecidedArgs{Seq: seq, Val: highestVa, Me: px.me, MaxDone: px.impl.doneSeqs[px.me]}
			px.mu.Unlock()

			reply := &DecidedReply{}
			var valid bool
			if i != px.me {
				valid = common.Call(addr, "Paxos.Learn", args, reply)
			}
			if valid {
				px.checkGC(i, reply.MaxDone)
			}
		}
	}
}

// if the sequence required is larger than current max
// need to append some intances
// call this function with px.mutex holded
func (px *Paxos) appendInstances(seq int) int {
	// append to the slices if needed
	currIndex := seq - px.impl.currStartIndex
	if currIndex+1 > len(px.impl.instances) {
		length := len(px.impl.instances)
		for i := 0; i < currIndex+1-length; i++ {
			px.impl.instances = append(px.impl.instances, makeInstance(int64(px.me)))
		}
	}
	return currIndex
}

// update the max known instance sequence number
// means we already know the value of this instance
// this function will require px.mutex lock
func (px *Paxos) updateMaxKnown(seq int) {
	px.mu.Lock()
	defer px.mu.Unlock()

	if seq > px.impl.maxKnownSeq {
		px.impl.maxKnownSeq = seq
	}
}

// update the doneSeqs according to <peerIndex> and <maxDone>
// check if we need to update the currStartIndex and do gc
// call this function everytime we get a new maxDone from other peers
// this function will require px.mutex lock()
func (px *Paxos) checkGC(peerIndex int, maxDone int) {
	px.mu.Lock()
	defer px.mu.Unlock()

	if maxDone > px.impl.doneSeqs[peerIndex] {
		px.impl.doneSeqs[peerIndex] = maxDone
	}

	min := math.MaxInt64
	for _, each := range px.impl.doneSeqs {
		if each < min {
			min = each
		}
	}
	// if we don't need to do gc
	if min < px.impl.currStartIndex {
		return
	}

	// do gc
	// how much instance do we need to gc
	number := min - px.impl.currStartIndex + 1

	for i := 0; i < number; i++ {
		px.impl.instances[i] = nil
	}

	px.impl.instances = px.impl.instances[number:]
	px.impl.currStartIndex += number
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	px.mu.Lock()
	defer px.mu.Unlock()

	// append to the slices if needed
	currIndex := px.appendInstances(seq)

	// check if this instance is already dicided/forgotten
	if currIndex < 0 || px.impl.instances[currIndex].status == Decided {
		return
	}
	// otherwise start to propose
	go px.propose(seq, px.impl.instances[currIndex], v)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	px.mu.Lock()
	defer px.mu.Unlock()

	px.impl.doneSeqs[px.me] = seq
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	res := px.impl.maxKnownSeq
	return res
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peer's z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peer's Min does not reflect another peer's Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers' Min()s will not increase
// even if all reachable peers call Done(). The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefore cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	px.mu.Lock()
	defer px.mu.Unlock()

	res := px.impl.currStartIndex
	return res
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so, what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	px.mu.Lock()
	defer px.mu.Unlock()

	currIndex := px.appendInstances(seq)
	if currIndex < 0 {
		return Forgotten, nil
	}
	return px.impl.instances[currIndex].status, px.impl.instances[currIndex].val
}

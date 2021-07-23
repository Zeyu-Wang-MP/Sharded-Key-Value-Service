package paxosrsm

import (
	"sync"
	"time"

	"umich.edu/eecs491/proj4/paxos"
)

//
// additions to PaxosRSM state
//
type PaxosRSMImpl struct {
	seq int // highest sequence applied
	// logger log.Logger
	mu sync.Mutex
}

//
// initialize rsm.impl.*
//
func (rsm *PaxosRSM) InitRSMImpl() {
	// file, err := os.OpenFile("/tmp/kvrsm.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// rsm.impl.logger.SetOutput(file)
}

//
// application invokes AddOp to submit a new operation to the replicated log
// AddOp returns only once value v has been decided for some Paxos instance
//
func (rsm *PaxosRSM) AddOp(v interface{}) {
	rsm.impl.mu.Lock()
	defer rsm.impl.mu.Unlock()

	// rsm.impl.logger.Printf("paxosrsm %v add op", rsm.me)

	// Timeout to reset to baseto on Start
	baseto := time.Millisecond
	to := baseto

	started := false

	for {
		status, value := rsm.px.Status(rsm.impl.seq)
		if status == paxos.Decided {
			// rsm.impl.logger.Printf("paxosrsm %v applying decided seq=%v", rsm.me, rsm.impl.seq)
			rsm.applyOp(value)

			rsm.px.Done(rsm.impl.seq)
			rsm.impl.seq++

			if rsm.equals(value, v) {
				// rsm.impl.logger.Printf("paxosrsm %d add op returning", rsm.me)
				return
			}

			started = false
		} else if !started && status == paxos.Pending {
			// rsm.impl.logger.Printf("paxosrsm %v starting seq=%v", rsm.me, rsm.impl.seq)
			rsm.px.Start(rsm.impl.seq, v)
			started = true
			to = baseto
			continue
		} else if status == paxos.Forgotten {
			// rsm.impl.logger.Printf("paxosrsm %v forgotten seq=%v", rsm.me, rsm.impl.seq)
			rsm.impl.seq++
			started = false
			continue
		}

		time.Sleep(to)
		if to < 100*time.Millisecond {
			to *= 2
		}
	}
}

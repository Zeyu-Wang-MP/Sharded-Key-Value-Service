package paxos

import "fmt"

// In all data types that represent RPC arguments/reply, field names
// must start with capital letters, otherwise RPC will break.

const (
	OK     = "OK"
	Reject = "Reject"
)

type Response string

type PrepareArgs struct {
	Seq int
	N   int64

	Me      int
	MaxDone int
}

type PrepareReply struct {
	Status   Response
	N        int64
	Na       int64
	Va       interface{}
	HighestN int64

	MaxDone int
}

type AcceptArgs struct {
	Seq int
	N   int64
	Val interface{}

	Me      int
	MaxDone int
}

type AcceptReply struct {
	Status Response
	N      int64

	MaxDone int
}

type DecidedArgs struct {
	Seq int
	Val interface{}

	Me      int
	MaxDone int
}

type DecidedReply struct {
	MaxDone int
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	// check if this sequence number is larger than current capacity
	px.mu.Lock()
	currIndex := px.appendInstances(args.Seq)
	if currIndex < 0 {
		px.mu.Unlock()
		fmt.Printf("%v  %v\n", args.Seq, currIndex)
		return fmt.Errorf("Prepare handler: instance already forgotten")
	}
	var ins *Instance = px.impl.instances[currIndex]
	reply.MaxDone = px.impl.doneSeqs[px.me]
	px.mu.Unlock()

	// check if we need to do gc since we got a new maxDone from others
	px.checkGC(args.Me, args.MaxDone)

	ins.aMutex.Lock()
	defer ins.aMutex.Unlock()

	if args.N > ins.np {
		ins.np = args.N
		reply.Status = OK
		reply.N = args.N
		reply.Na = ins.na
		reply.Va = ins.va
	} else {
		reply.Status = Reject
		reply.HighestN = ins.np
	}
	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	currIndex := px.appendInstances(args.Seq)
	if currIndex < 0 {
		px.mu.Unlock()
		fmt.Printf("%v  %v\n", args.Seq, currIndex)
		return fmt.Errorf("Accept handler: instance already forgotten")
	}
	var ins *Instance = px.impl.instances[currIndex]
	reply.MaxDone = px.impl.doneSeqs[px.me]
	px.mu.Unlock()

	px.checkGC(args.Me, args.MaxDone)

	ins.aMutex.Lock()
	defer ins.aMutex.Unlock()

	if args.N >= ins.np {
		ins.np = args.N
		ins.na = args.N
		ins.va = args.Val
		reply.N = args.N
		reply.Status = OK
	} else {
		reply.Status = Reject
	}

	return nil
}

func (px *Paxos) Learn(args *DecidedArgs, reply *DecidedReply) error {
	px.mu.Lock()
	currIndex := px.appendInstances(args.Seq)
	if currIndex < 0 {
		px.mu.Unlock()
		fmt.Printf("%v  %v\n", args.Seq, currIndex)
		return fmt.Errorf("Learn handler: instance already forgotten")
	}
	var ins *Instance = px.impl.instances[currIndex]
	reply.MaxDone = px.impl.doneSeqs[px.me]
	px.mu.Unlock()

	px.checkGC(args.Me, args.MaxDone)

	ins.pMutex.Lock()
	defer ins.pMutex.Unlock()

	if ins.status == Decided {
		return nil
	}

	// if this instance is still pending
	ins.status = Decided
	ins.val = args.Val
	px.updateMaxKnown(args.Seq)

	return nil
}

//
// add RPC handlers for any RPCs you introduce.
//

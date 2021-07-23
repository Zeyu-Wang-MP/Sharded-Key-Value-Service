package shardmaster

import (
	"fmt"
	"math"
	"time"

	"umich.edu/eecs491/proj4/common"
)

const DEBUG = false

//
// Define what goes into "value" that Paxos is used to agree upon.
// Field names must start with capital letters.
//
type Op struct {
	OpId    int64
	OpType  string
	GID     int64
	Servers []string
	Shard   int
	Num     int
	Me      int
}

//
// Method used by PaxosRSM to determine if two Op values are identical
//
func equals(v1 interface{}, v2 interface{}) bool {
	op1 := v1.(Op)
	op2 := v2.(Op)
    
	return op1.OpId == op2.OpId
}

//
// additions to ShardMaster state
//
type ShardMasterImpl struct {
}

//
// initialize sm.impl.*
//
func (sm *ShardMaster) InitImpl() {
}

//
// RPC handlers for Join, Leave, Move, and Query RPCs
//
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.rsm.AddOp(Op{OpId: common.Nrand(), OpType: "Join", GID: args.GID, Servers: args.Servers, Me: sm.me})

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.rsm.AddOp(Op{OpId: common.Nrand(), OpType: "Leave", GID: args.GID, Me: sm.me})

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.rsm.AddOp(Op{OpId: common.Nrand(), OpType: "Move", Shard: args.Shard, GID: args.GID, Me: sm.me})

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	
	
	sm.rsm.AddOp(Op{OpId: common.Nrand(), OpType: "Query", Me: sm.me})

	if args.Num <= -1 || args.Num >= len(sm.configs) {
		reply.Config = sm.configs[len(sm.configs)-1]
	} else {
		reply.Config = sm.configs[args.Num]
	}

	return nil
}

//
// Execute operation encoded in decided value v and update local state
//
func (sm *ShardMaster) ApplyOp(v interface{}) {

	op, _ := v.(Op)

	if op.OpType == "Join" {
		sm.joinImpl(op)
	} else if op.OpType == "Leave" {
		sm.leaveImpl(op)
	} else if op.OpType == "Move" {
		sm.moveImpl(op)
	} else if op.OpType == "Query" {
		// Nothing to apply
		// There is one small issue though.  The assumption here is that ApplyOp
		// will return, giving up the lock, and then AddOp will return to the Query
		// RPC handler.  Once this happens, Query will try to obtain the lock
		// and then format the reply to the caller.  At this point, another RPC
		// handler could steal the lock and apply another operation violating linearizeability.
		// There is a non-zero probability that this will happen, but I think
		// it is a very low chance.  I can only think of hacks to fix this, nothing
		// elegant has occured to me yet.
	} else {
		panic("ApplyOp unrecognized op type")
	}
}

func (sm *ShardMaster) joinImpl(op Op) {
	cur := len(sm.configs) - 1
    currConfig := sm.configs[cur]
	if DEBUG{
		cur := len(sm.configs) - 1
		groups := make(map[int64][]int)
		for shard, grpNum := range sm.configs[cur].Shards {
			groups[grpNum] = append(groups[grpNum], shard)
		}
		fmt.Printf("DEBUG: JOIN: new group %d\n", op.GID)
		fmt.Printf("DEBUG: config Id: %d current shards distribution: \n", cur)
		for gid, shards := range groups{
			fmt.Printf("DEBUG:                                          group %d : ", gid)
			for _, shard := range shards{
				fmt.Printf("%d ", shard)
			}
			fmt.Printf("\n")
		}
	}
	// create a new config with number cur + 1
	newConfig := Config{Num: cur + 1}
	newConfig.Groups = make(map[int64][]string)

	if cur == 0 {
		// set the servers for gid
		newConfig.Groups[op.GID] = op.Servers

		// Set all the shards to the one and only group
		for i, _ := range newConfig.Shards {
			newConfig.Shards[i] = op.GID
		}
        sm.configs = append(sm.configs, newConfig)
		sm.AskGroupToGive(op.GID, -1, op.GID, op.Servers)
		return
	} else {
		// Check if the group trying to join already exists in the current
		// configuration.  IDK what to do if this is the case...
		if _, exists := sm.configs[cur].Groups[op.GID]; exists {
			// TODO: group already exists in configs
			// panic("Trying to join with a group already in current config")
			return
		}

		// Copy config to save previous group information
		newConfig.Shards = sm.configs[cur].Shards
		for g, s := range sm.configs[cur].Groups {
			newConfig.Groups[g] = s
		}
		newConfig.Groups[op.GID] = op.Servers

		// Create a map from GID to Shards
		// This map will be used to determine the current load
		// and shift load appropriately onto the new group
		groups := make(map[int64][]int)
		for shard, grpNum := range sm.configs[cur].Shards {
			groups[grpNum] = append(groups[grpNum], shard)
		}

		// If the average load is 1, return
		// We can't split the load any more envenly than this, and
		// the minimum amount of work is to do nothing
		avgLoad := float64(len(sm.configs[cur].Shards)) / float64(len(groups))
		if math.Abs(avgLoad-1.0) > 1e-6 {
			// Calculate the target load, so we know when to stop transfering shards
			// to the new group
			targetLoad := len(sm.configs[cur].Shards) / (len(groups) + 1) // integer division
			newGroupLoad := make([]int, 0)

			// Take shards from groups above average load and assign them
			// to the new group
			for group, shards := range groups {
				if len(newGroupLoad) >= targetLoad {
					break
				}

				if float64(len(shards)) > avgLoad {
					newGroupLoad = append(newGroupLoad, shards[0])
					groups[group] = shards[1:]
				}
			}

			// Take shards from all other groups evenly
			run := true
			for run {
				for group, shards := range groups {
					if len(newGroupLoad) >= targetLoad {
						run = false
						break
					}
                    if len(shards) > 0{
                        newGroupLoad = append(newGroupLoad, shards[0])
					    groups[group] = shards[1:]
					}
				}
			}
			// Assign shards to the new group
			for _, shard := range newGroupLoad {
				newConfig.Shards[shard] = op.GID
                
				oldGID := currConfig.Shards[shard]
				if op.Me == sm.me{
					sm.AskGroupToGive(oldGID, shard, op.GID, op.Servers)
				}
			}
		}
	}

	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) leaveImpl(op Op) {
	if DEBUG{
		cur := len(sm.configs) - 1
		groups := make(map[int64][]int)
		for shard, grpNum := range sm.configs[cur].Shards {
			groups[grpNum] = append(groups[grpNum], shard)
		}
		fmt.Printf("DEBUG: LEAVE: group %d\n", op.GID)
		fmt.Printf("DEBUG: config Id: %d current shards distribution: \n", cur)
		for gid, shards := range groups{
			fmt.Printf("DEBUG:                                        group %d : ", gid)
			for _, shard := range shards{
				fmt.Printf("%d ", shard)
			}
			fmt.Printf("\n")
		}
	}
	curConfig := sm.configs[len(sm.configs)-1]
    if _, exist := curConfig.Groups[op.GID]; !exist{
        return
	}

	newConfig := Config{Num: curConfig.Num + 1}
	newConfig.Shards = curConfig.Shards
	newConfig.Groups = make(map[int64][]string)
	for g, s := range curConfig.Groups {
		if g != op.GID {
			newConfig.Groups[g] = s
		}
	}

	shards := make([]int, 0)
	groups := make(map[int64]int)

	for g, _ := range curConfig.Groups {
		if g != op.GID {
			groups[g] = 0
		}
	}

	for shard, group := range newConfig.Shards {
		if group == op.GID {
			shards = append(shards, shard)
		} else {
			groups[group] += 1
		}
	}

	oldAvgLoad := float64(len(newConfig.Shards)) / float64(len(curConfig.Groups))

	for group, numShards := range groups {
		if len(shards) == 0 {
			break
		}

		if float64(numShards) < oldAvgLoad {
			// assign shard to the new group
			shardIndexToGive := shards[0]
			newConfig.Shards[shards[0]] = group
			shards = shards[1:]

			if op.Me == sm.me{
				sm.AskGroupToGive(op.GID, shardIndexToGive, group, curConfig.Groups[group])
			}
		}
	}

	for len(shards) > 0 {
		for group, _ := range groups {
			if len(shards) == 0 {
				break
			}
            shardIndexToGive := shards[0]
			// assign shard to the new group
			newConfig.Shards[shards[0]] = group
			shards = shards[1:]

			if op.Me == sm.me{
				sm.AskGroupToGive(op.GID, shardIndexToGive, group, curConfig.Groups[group])
			}
		}
	}

	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) moveImpl(op Op) {
	if DEBUG{
		cur := len(sm.configs) - 1
		groups := make(map[int64][]int)
		for shard, grpNum := range sm.configs[cur].Shards {
			groups[grpNum] = append(groups[grpNum], shard)
		}
		fmt.Printf("DEBUG: MOVE: move shard %d to group %d\n", op.Shard, op.GID)
		fmt.Printf("DEBUG: config Id: %d current shards distribution: \n", cur)
		for gid, shards := range groups{
			fmt.Printf("DEBUG:                                        group %d : ", gid)
			for _, shard := range shards{
				fmt.Printf("%d ", shard)
			}
			fmt.Printf("\n")
		}
	}
	curConfig := sm.configs[len(sm.configs)-1]

	// If the gid doesn't exist in the current groups
	if _, exists := curConfig.Groups[op.GID]; !exists {
		return
	}

	if curConfig.Shards[op.Shard] == op.GID{
		// don't need to do anything
		return
	}
    
	newConfig := Config{}
	newConfig.Num = curConfig.Num + 1
	newConfig.Shards = curConfig.Shards
	newConfig.Groups = make(map[int64][]string)
	for g, s := range curConfig.Groups {
		newConfig.Groups[g] = s
	}
    
	oldGID := curConfig.Shards[op.Shard]
	if op.Me == sm.me{
		sm.AskGroupToGive(oldGID, op.Shard, op.GID, curConfig.Groups[op.GID])
	}

	newConfig.Shards[op.Shard] = op.GID

	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) AskGroupToGive(groupToGive int64, shardIndex int, groupToReceive int64, receiverAddrs []string){
	currConfig := sm.configs[len(sm.configs) - 1]
	if DEBUG{
		fmt.Printf("DEBUG: group %d give group %d shard %d\n", groupToGive, groupToReceive, shardIndex)
	}
	
	args := common.GiveDataToArgs{ShardIndex: shardIndex, ServerAddrs: receiverAddrs, RPCID: common.Nrand()}
	reply := common.GiveDataToReply{}

	for{
		for _, serverAddr := range currConfig.Groups[groupToGive]{
			if common.Call(serverAddr, "ShardKV.GiveDataTo", &args, &reply){
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	
}
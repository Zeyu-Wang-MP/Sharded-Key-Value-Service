# Sharded Key/Value Storage System based on Paxos
This is a course project of a distributed system course. <br/>
<br/>
This project contains 4 main packages, which as a whole can be used as a distributed, sharded, replicated Key/Value storage system 
which supports "Put", "Append", and "Get" operations and provides linearizability. <br/>
<br/>
`paxos` and `paxosrsm` implemented the paxos protocol and a Replicated State Machine using paxos library, which provides
a useful interface to use paxos library. <br/>
<br/>
`shardmaster` implemented a replicated shard master server managing the replica groups of servers, handling died and joined servers and doing the load-balancing among groups. <br/>
<br/>
`shardkv` implemented a shard server operating as part of a replica group, in which every server can serve concurrent requests.<br/>
<br/>
The shard master provides a distributed hash table and every key can be hashed to one shard/partition. Each replica group will be responsible for part of all shards so that the perfermance can increase with the newly joined servers. Since both the `shardmaster` and `shardkv` is fault-tolerant, the whole system is fault-tolerant as long as the majority of a replica group or shard masters lives.
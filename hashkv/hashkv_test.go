package hashkv

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"testing"
	"time"

	"umich.edu/eecs491/proj4/common"
)

const NKeys = 20

type Server struct {
	ID   string
	port string
}

func port(tag string, host int) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "skv-"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += tag + "-"
	s += strconv.Itoa(host)
	return s
}

func setup(t *testing.T, tag string, nservers int) []Server {
	servers := make([]Server, nservers)
	for i := 0; i < nservers; i++ {
		servers[i].ID = strconv.FormatUint(uint64(common.Nrand()), 10)
		servers[i].port = port(tag, i)
	}
	return servers
}

func setup2(t *testing.T, tag string, nservers int) []Server {
	servers := make([]Server, nservers)
	for i := 0; i < nservers; i++ {
		// servers[i].ID = strconv.FormatUint(uint64(common.Nrand()), 10)
		if i == 0 {
			servers[i].ID = strconv.Itoa(2508854991458392016)
		} else if i == 1 {
			servers[i].ID = strconv.Itoa(3098560461358383880)
		} else {
			servers[i].ID = strconv.Itoa(i)
		}
		servers[i].port = port(tag, i)
	}
	return servers
}

func setup3(t *testing.T, tag string, nservers int) []Server {
	servers := make([]Server, nservers)
	for i := 0; i < nservers; i++ {
		// servers[i].ID = strconv.FormatUint(uint64(common.Nrand()), 10)
		if i == nservers-1 {
			servers[i].ID = strconv.Itoa(2508854991458392016)
		} else if i == nservers-2 {
			servers[i].ID = strconv.Itoa(3098560461358383880)
		} else {
			servers[i].ID = strconv.Itoa(i)
		}
		servers[i].port = port(tag, i)
	}
	return servers
}

func check(t *testing.T, servers []Server, kvmap map[string]string, unassigned bool) {
	compare := func(i, j int) bool {
		h1 := common.Key2Shard(servers[i].ID)
		h2 := common.Key2Shard(servers[j].ID)
		return (h1 < h2) || ((h1 == h2) && (servers[i].ID < servers[j].ID))
	}
	sort.Slice(servers, compare)

	done := make(chan error, 3*NKeys)
	for key, val := range kvmap {
		j := 0
		for ; j < len(servers); j++ {
			if common.Key2Shard(key) <= common.Key2Shard(servers[j].ID) {
				break
			}
		}
		assigned := j % len(servers)

		go checkAssign(key, val, servers[assigned].port, true, done)
		if unassigned {
			pred := (assigned + len(servers) - 1) % len(servers)
			succ := (assigned + len(servers) + 1) % len(servers)
			go checkAssign(key, val, servers[pred].port, false, done)
			go checkAssign(key, val, servers[succ].port, false, done)
		}
	}

	drainOne := func(done <-chan error) {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < NKeys; i++ {
		drainOne(done)
		if unassigned {
			drainOne(done)
			drainOne(done)
		}
	}
}

func checkAssign(key string, value string, s string, assigned bool, done chan error) {
	ck := MakeClerk([]string{s})

	ch := make(chan string)
	go func() {
		v := ck.Get(key)
		ch <- v
		close(ch)
	}()

	select {
	case v := <-ch:
		if !assigned {
			done <- fmt.Errorf("DEBUG: Server %s served request on key %s in shard %d, which is not assigned to it", s, key, common.Key2Shard(key))
			return
		}
		if v != value {
			done <- fmt.Errorf("DEBUG: Server %s returned incorrect value for key %s in shard %d", s, key, common.Key2Shard(key))
			return
		}
	case <-time.After(2 * time.Second):
		if assigned {
			done <- fmt.Errorf("DEBUG: Server %s did not serve request on key %s in shard %d, which is assigned to it", s, key, common.Key2Shard(key))
			return
		}
	}

	done <- nil
}

func findNeighbors(s Server, servers []Server) (Server, Server) {
	compare := func(i, j int) bool {
		h1 := common.Key2Shard(servers[i].ID)
		h2 := common.Key2Shard(servers[j].ID)
		return (h1 < h2) || ((h1 == h2) && (servers[i].ID < servers[j].ID))
	}
	sort.Slice(servers, compare)

	var i int
	for i = 0; i < len(servers); i++ {
		if servers[i] == s {
			break
		}
	}

	return servers[(i+len(servers)-1)%len(servers)], servers[(i+len(servers)+1)%len(servers)]
}

func TestBasic(t *testing.T) {
	runtime.GOMAXPROCS(4 * NKeys)

	nservers := 8
	s := setup(t, "basic", nservers)
	hashkv_servers := make([]*HashKV, 0)

	fmt.Printf("DEBUG: Test: Basic joins ...\n")

	hashkv_servers = append(hashkv_servers, StartServer([]string{s[0].port, s[0].port, s[0].port}, []string{s[0].ID, s[0].ID, s[0].ID}))
	hashkv_servers[0].Setunreliable(true)

	ports := make([]string, 0)
	ports = append(ports, s[0].port)
	ck := MakeClerk(ports)
	vals := make(map[string]string)
	rr := rand.New(rand.NewSource(int64(os.Getpid())))
	for j := 0; j < NKeys; j++ {
		key := strconv.Itoa(rr.Int())
		vals[key] = strconv.Itoa(rr.Int())
		ck.Put(key, vals[key])
	}

	for i := 1; i < nservers; i++ {
		curr_s := make([]Server, i+1)
		copy(curr_s, s[:i+1])
		pred, succ := findNeighbors(s[i], curr_s)
		hashkv_servers = append(hashkv_servers, StartServer([]string{pred.port, s[i].port, succ.port}, []string{pred.ID, s[i].ID, succ.ID}))
		hashkv_servers[len(hashkv_servers)-1].Setunreliable(true)
		check(t, curr_s, vals, false)

		ports = append(ports, s[i].port)
		ck = MakeClerk(ports)
		for key, _ := range vals {
			vals[key] = strconv.Itoa(rr.Int())
			ck.Put(key, vals[key])
		}
	}
	curr_s := make([]Server, nservers)
	copy(curr_s, s)
	check(t, curr_s, vals, true)

	fmt.Printf("DEBUG:   ... Passed\n")

	fmt.Printf("DEBUG:  Test: Basic leaves ...\n")

	for i := 0; i < nservers-1; i++ {
		hashkv_servers[0].kill()
		hashkv_servers = hashkv_servers[1:]
		curr_s := make([]Server, nservers-1-i)
		copy(curr_s, s[i+1:])
		check(t, curr_s, vals, false)

		ports = ports[1:]
		ck = MakeClerk(ports)
		for key, _ := range vals {
			vals[key] = strconv.Itoa(rr.Int())
			ck.Put(key, vals[key])
		}
	}

	fmt.Printf("DEBUG:   ... Passed\n")
}

func TestUnreliable(t *testing.T) {
	runtime.GOMAXPROCS(4 * NKeys)

	nservers := 8
	s := setup(t, "basic", nservers)
	hashkv_servers := make([]*HashKV, 0)

	fmt.Printf("Test: Unreliable joins ...\n")

	hashkv_servers = append(hashkv_servers, StartServer([]string{s[0].port, s[0].port, s[0].port}, []string{s[0].ID, s[0].ID, s[0].ID}))

	// Make first server unreliable
	hashkv_servers[0].Setunreliable(true)

	ports := make([]string, 0)
	ports = append(ports, s[0].port)
	ck := MakeClerk(ports)
	vals := make(map[string]string)
	rr := rand.New(rand.NewSource(int64(os.Getpid())))
	for j := 0; j < NKeys; j++ {
		key := strconv.Itoa(rr.Int())
		vals[key] = strconv.Itoa(rr.Int())
		ck.Put(key, vals[key])
	}

	for i := 1; i < nservers; i++ {
		curr_s := make([]Server, i+1)
		copy(curr_s, s[:i+1])
		pred, succ := findNeighbors(s[i], curr_s)
		hashkv_servers = append(hashkv_servers, StartServer([]string{pred.port, s[i].port, succ.port}, []string{pred.ID, s[i].ID, succ.ID}))

		// Make server unreliable
		hashkv_servers[len(hashkv_servers)-1].Setunreliable(true)

		check(t, curr_s, vals, false)

		ports = append(ports, s[i].port)
		ck = MakeClerk(ports)
		for key, _ := range vals {
			vals[key] = strconv.Itoa(rr.Int())
			ck.Put(key, vals[key])
		}
	}
	curr_s := make([]Server, nservers)
	copy(curr_s, s)
	check(t, curr_s, vals, true)

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Unreliable leaves ...\n")

	for i := 0; i < nservers-1; i++ {
		hashkv_servers[0].kill()
		hashkv_servers = hashkv_servers[1:]
		curr_s := make([]Server, nservers-1-i)
		copy(curr_s, s[i+1:])
		check(t, curr_s, vals, false)

		ports = ports[1:]
		ck = MakeClerk(ports)
		for key, _ := range vals {
			vals[key] = strconv.Itoa(rr.Int())
			ck.Put(key, vals[key])
		}
	}

	fmt.Printf("  ... Passed\n")
}

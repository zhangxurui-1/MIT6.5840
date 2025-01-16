package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu     sync.Mutex
	kvmap  map[string]string
	record map[string]struct {
		argid ArgID
		rpl   string
	}
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// fmt.Println("KVServer.Get() args:", args)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.Key
	val := ""
	if v, ok := kv.kvmap[key]; ok {
		val = v
	}
	reply.Value = val

	// fmt.Printf("records: %v\n", kv.record)
	// fmt.Println("reply.Value:", reply.Value)
	// fmt.Println("kvmap:", kv.kvmap)
	// // Your code here.
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// fmt.Println("KVServer.Put() args:", args)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 过滤重复的request
	if r, ok := kv.record[args.Aid.EndName]; ok && r.argid == args.Aid {
		return
	}

	key := args.Key
	val := args.Value

	kv.kvmap[key] = val
	// 更新record
	kv.record[args.Aid.EndName] = struct {
		argid ArgID
		rpl   string
	}{args.Aid, ""}

	// fmt.Println("kvmap:", kv.kvmap)
	// Your code here.
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// fmt.Println("KVServer.Append() args:", args)
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 过滤重复的request
	if r, ok := kv.record[args.Aid.EndName]; ok && r.argid == args.Aid {
		reply.Value = r.rpl
		return
	}

	key := args.Key
	val := args.Value

	ret := ""
	if oldv, ok := kv.kvmap[key]; ok {
		ret = oldv
	}
	kv.kvmap[key] = kv.kvmap[key] + val
	reply.Value = ret

	kv.record[args.Aid.EndName] = struct {
		argid ArgID
		rpl   string
	}{args.Aid, ret}

	// fmt.Println("kvmap:", kv.kvmap)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.kvmap = make(map[string]string)
	kv.record = make(map[string]struct {
		argid ArgID
		rpl   string
	})

	return kv
}

package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func DFPrintf(id int, format string, a ...any) {
	filename := strconv.Itoa(id) + ".txt"
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()
	fmt.Fprintf(file, format, a...)
}

type Op struct {
	ID        ArgsId
	Operation string
	Key       string
	Value     string
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	dead   int32 // set by Kill()
	deadch chan struct{}

	kvmap             map[string]string
	rcdmap            map[string]Record
	lastApplied       int
	lastIncludedIndex int

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
}

func (kv *KVServer) checkLeader(argsId ArgsId) bool {
	term, isLeader := kv.rf.GetState()

	if Debug {
		defer func() {
			DFPrintf(kv.me, "checkLeader done... (term: %v, argsId: %v)\n", term, argsId)
		}()
	}
	if !isLeader {
		return false
	}
	name := strconv.Itoa(kv.me)

	kv.mu.Lock()
	nilCmdRcd, ok := kv.rcdmap[name]
	kv.mu.Unlock()

	nilCmdId := ArgsId{name, term}
	if ok && (nilCmdRcd.ArgsId == nilCmdId) {
		for !nilCmdRcd.Applied {
			time.Sleep(5 * time.Millisecond)

			kv.mu.Lock()
			nilCmdRcd = kv.rcdmap[name]

			// 这条record被覆盖（没有被commit）
			if nilCmdRcd.ArgsId != nilCmdId {
				kv.mu.Unlock()
				return false
			}
			// 这条空command没有被commit
			if nilCmdRcd.Index < kv.lastApplied && !nilCmdRcd.Applied {

				delete(kv.rcdmap, name)
				kv.mu.Unlock()
				return false
			}
			kv.mu.Unlock()
		}
		t, ld := kv.rf.GetState()
		return (t == term) && ld
	}

	// 如果leader是新上任的，那么首先start并commit一条空的command
	command := Op{
		ID:        nilCmdId,
		Operation: "NIL",
	}

	rcd := Record{
		ArgsId:  command.ID,
		Done:    make(chan struct{}),
		Applied: false,
	}

	kv.mu.Lock()
	kv.rcdmap[name] = rcd

	index, t, ld := kv.rf.Start(command)
	if !ld {
		delete(kv.rcdmap, name)
		kv.mu.Unlock()

		return false
	}
	kv.mu.Unlock()
	if t != term {
		return false
	}

	for {
		t, ld := kv.rf.GetState()
		select {
		case <-rcd.Done:
			return (t == term) && (ld == isLeader)
		case <-kv.deadch:
			return false
		default:
			kv.mu.Lock()

			rcd_updated := kv.rcdmap[name]
			// 这条record被覆盖（没有被commit）
			if rcd_updated.ArgsId != nilCmdId {
				kv.mu.Unlock()
				return false
			} else if kv.rcdmap[name].Applied {
				kv.mu.Unlock()
				return (t == term) && (ld == isLeader)
			}

			if rcd_updated.Index <= 0 {
				rcd_updated.Index = index
				kv.rcdmap[name] = rcd_updated
			} else if (rcd.Index < kv.lastApplied && !kv.rcdmap[name].Applied) || !ld {
				delete(kv.rcdmap, name)
				kv.mu.Unlock()
				return false
			}
			kv.mu.Unlock()
			// if Debug {
			// 	DFPrintf(kv.me, "(%v) checkpoint\n", t)
			// }

			time.Sleep(5 * time.Millisecond)
		}
	}
}

func (kv *KVServer) checkDuplicate(argsId ArgsId, reply interface{}) bool {
	kv.mu.Lock()
	rcd, ok := kv.rcdmap[argsId.ClientId]
	kv.mu.Unlock()

	if Debug {
		if !ok {
			DFPrintf(kv.me, "kv.rcdmap[%v]=nil\n", argsId.ClientId)
		} else {
			DFPrintf(kv.me, "kv.rcdmap[%v]=%v\n", argsId.ClientId, rcd)
		}
	}

	switch r := reply.(type) {
	case *GetReply:
		if ok && rcd.ArgsId == argsId {
			r.Err = rcd.Err
			r.RequestId = rcd.ArgsId
			r.Value = rcd.Value
			if Debug {
				DFPrintf(kv.me, "duplicate (GET)..., reply %v\n", r)
			}
			return true
		}
	case *PutAppendReply:
		if ok && rcd.ArgsId == argsId {
			r.Err = rcd.Err
			r.RequestId = rcd.ArgsId
			if Debug {
				DFPrintf(kv.me, "duplicate (PUT/APPEND)..., reply %v\n", r)
			}
			return true
		}
	default:
		log.Fatalf("unexpected type of %v\n", reply)
	}
	return false
}

func (kv *KVServer) startCommand(command Op, args interface{}, reply interface{}) {
	switch ags := args.(type) {
	case *GetArgs:
		rpl, ok := reply.(*GetReply)
		if !ok {
			log.Fatalf("args (%v) and reply (%v) unmatch\n", args, reply)
		}

		rcd := Record{
			ArgsId:  ags.ID,
			Done:    make(chan struct{}),
			Applied: false,
		}

		// 要先存rcdmap，再调用kv.rf.Start()
		kv.mu.Lock()
		kv.rcdmap[ags.ID.ClientId] = rcd

		index, term, isleader := kv.rf.Start(command)

		if Debug {
			defer func() {
				DFPrintf(kv.me, "\t(%v) reply: %v\n", term, reply)
			}()
		}

		if !isleader {
			delete(kv.rcdmap, ags.ID.ClientId)
			kv.mu.Unlock()

			rpl.Err = ErrWrongLeader
			rpl.RequestId = ags.ID
			return
		}
		kv.mu.Unlock()

		for {
			select {
			case <-rcd.Done:
				kv.mu.Lock()
				rpl.Err = kv.rcdmap[ags.ID.ClientId].Err
				rpl.Value = kv.rcdmap[ags.ID.ClientId].Value
				rpl.RequestId = kv.rcdmap[ags.ID.ClientId].ArgsId
				kv.mu.Unlock()
				return
			case <-kv.deadch:
				kv.mu.Lock()
				rpl.Err = ErrWrongLeader
				rpl.Value = ""
				rpl.RequestId = ags.ID
				kv.mu.Unlock()
				return
			default:
				kv.mu.Lock()
				if _, ld := kv.rf.GetState(); !ld || kv.killed() {
					rpl.Err = ErrWrongLeader
					rpl.Value = ""
					rpl.RequestId = rcd.ArgsId

					kv.mu.Unlock()
					return
				}

				rcd_updated := kv.rcdmap[ags.ID.ClientId]
				if rcd_updated.Index <= 0 {
					// 单独更新一下rcdmap中的index
					rcd_updated.Index = index
					kv.rcdmap[ags.ID.ClientId] = rcd_updated
				} else if rcd_updated.Index < kv.lastApplied && !rcd_updated.Applied {
					rpl.Err = ErrWrongLeader
					rpl.Value = ""
					rpl.RequestId = rcd_updated.ArgsId
					if Debug {
						DFPrintf(kv.me, "\tkv.rcdmap[%v]=%v, deleted (kv.lastApplied: %v)\n",
							rcd_updated.ArgsId.ClientId, rcd_updated, kv.lastApplied)
					}
					delete(kv.rcdmap, rcd_updated.ArgsId.ClientId)

					kv.mu.Unlock()
					return
				}
				kv.mu.Unlock()
				time.Sleep(5 * time.Millisecond)
			}
		}

	case *PutAppendArgs:
		rpl, ok := reply.(*PutAppendReply)
		if !ok {
			log.Fatalf("args (%v) and reply (%v) unmatch\n", args, reply)
		}

		rcd := Record{
			ArgsId:  ags.ID,
			Done:    make(chan struct{}),
			Applied: false,
		}
		kv.mu.Lock()
		kv.rcdmap[ags.ID.ClientId] = rcd

		index, term, isleader := kv.rf.Start(command)

		if Debug {
			defer func() {
				DFPrintf(kv.me, "\t(%v) reply: %v\n", term, reply)
			}()
		}

		if !isleader {
			delete(kv.rcdmap, ags.ID.ClientId)
			kv.mu.Unlock()

			rpl.Err = ErrWrongLeader
			rpl.RequestId = ags.ID
			return
		}
		kv.mu.Unlock()

		for {
			select {
			case <-rcd.Done:
				kv.mu.Lock()
				rpl.Err = kv.rcdmap[ags.ID.ClientId].Err
				rpl.RequestId = rcd.ArgsId
				kv.mu.Unlock()
				return
			case <-kv.deadch:
				kv.mu.Lock()
				rpl.Err = ErrWrongLeader
				rpl.RequestId = ags.ID
				kv.mu.Unlock()
				return
			default:
				kv.mu.Lock()
				if t, ld := kv.rf.GetState(); !ld || kv.killed() {
					rpl.Err = ErrWrongLeader
					rpl.RequestId = rcd.ArgsId

					if Debug {
						DFPrintf(kv.me, "\t(%v) Not a leader, reply: %v\n", t, rpl)
					}
					kv.mu.Unlock()
					return
				}

				rcd_updated := kv.rcdmap[ags.ID.ClientId]

				if rcd_updated.Index <= 0 {
					rcd_updated.Index = index
					kv.rcdmap[ags.ID.ClientId] = rcd_updated
				} else if rcd_updated.Index < kv.lastApplied && !rcd_updated.Applied {
					rpl.Err = ErrWrongLeader
					rpl.RequestId = rcd_updated.ArgsId

					if Debug {
						DFPrintf(kv.me, "\tkv.rcdmap[%v]=%v, deleted (kv.lastApplied: %v)\n",
							rcd_updated.ArgsId.ClientId, rcd_updated, kv.lastApplied)
					}
					delete(kv.rcdmap, rcd_updated.ArgsId.ClientId)

					kv.mu.Unlock()
					return
				}
				kv.mu.Unlock()
				time.Sleep(5 * time.Millisecond)
			}
		}

	default:
		if Debug {
			DFPrintf(kv.me, "wrong args type\n")
		}
	}

}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	if !kv.checkLeader(args.ID) {
		reply.Err = ErrWrongLeader
		reply.RequestId = args.ID
		return
	}
	if kv.checkDuplicate(args.ID, reply) {
		return
	}

	// if Debug {
	// 	DFPrintf(kv.me, "checkpoint (%v)\n", args.ID)
	// }
	command := Op{
		ID:        args.ID,
		Operation: "GET",
		Key:       args.Key,
	}
	kv.startCommand(command, args, reply)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	if !kv.checkLeader(args.ID) {
		reply.Err = ErrWrongLeader
		reply.RequestId = args.ID
		return
	}
	if kv.checkDuplicate(args.ID, reply) {
		return
	}

	// if Debug {
	// 	DFPrintf(kv.me, "checkpoint (%v)\n", args.ID)
	// }
	command := Op{
		ID:        args.ID,
		Operation: "PUT",
		Key:       args.Key,
		Value:     args.Value,
	}
	kv.startCommand(command, args, reply)
	// Your code here.
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if !kv.checkLeader(args.ID) {
		reply.Err = ErrWrongLeader
		reply.RequestId = args.ID
		return
	}

	if kv.checkDuplicate(args.ID, reply) {
		return
	}

	// if Debug {
	// 	DFPrintf(kv.me, "checkpoint (%v)\n", args.ID)
	// }

	command := Op{
		ID:        args.ID,
		Operation: "APPEND",
		Key:       args.Key,
		Value:     args.Value,
	}
	kv.startCommand(command, args, reply)
}

func (kv *KVServer) encodeSnapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	kv.lastIncludedIndex = kv.lastApplied
	e.Encode(kv.lastIncludedIndex)
	// kvmap
	e.Encode(len(kv.kvmap))
	for k, v := range kv.kvmap {
		e.Encode(k)
		e.Encode(v)
	}
	// rcdmap
	e.Encode(len(kv.rcdmap))
	for k, v := range kv.rcdmap {
		e.Encode(k)
		e.Encode(v)
	}

	return w.Bytes()
}

func (kv *KVServer) decodeSnapShot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}

	kv.kvmap = make(map[string]string)
	kv.rcdmap = make(map[string]Record)

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var (
		kvmapSize         int
		rcdmapSize        int
		lastIncludedIndex int
	)

	if d.Decode(&lastIncludedIndex) != nil || d.Decode(&kvmapSize) != nil {
		log.Fatal("snapshot decode error")
	}
	kv.lastIncludedIndex = lastIncludedIndex
	if kv.lastApplied < kv.lastIncludedIndex {
		kv.lastApplied = kv.lastIncludedIndex
	}

	for i := 0; i < kvmapSize; i++ {
		var k, v string
		if d.Decode(&k) != nil || d.Decode(&v) != nil {
			log.Fatal("snapshot decode error")
		}
		kv.kvmap[k] = v
	}

	if d.Decode(&rcdmapSize) != nil {
		log.Fatal("snapshot decode error")
	}
	for i := 0; i < rcdmapSize; i++ {
		var (
			k   string
			rcd Record
		)
		if d.Decode(&k) != nil || d.Decode(&rcd) != nil {
			log.Fatal("snapshot decode error")
		}
		kv.rcdmap[k] = rcd
	}
}

func (kv *KVServer) apply() {
	for {
		select {
		case apl := <-kv.applyCh:
			if apl.CommandValid {
				c := apl.Command.(Op)
				kv.mu.Lock()
				r, ok2 := kv.rcdmap[c.ID.ClientId]
				if !ok2 || r.ArgsId != c.ID {
					r = Record{ArgsId: c.ID, Index: apl.CommandIndex}
				}

				switch c.Operation {
				case "GET":
					v, ok := kv.kvmap[c.Key]
					if ok {
						r.Err = OK
						r.Value = v
					} else {
						r.Err = ErrNoKey
						r.Value = ""
					}

				case "PUT":
					kv.kvmap[c.Key] = c.Value
					r.Err = OK

				case "APPEND":
					v, ok := kv.kvmap[c.Key]
					if !ok {
						v = ""
					}
					kv.kvmap[c.Key] = v + c.Value
					r.Err = OK
				case "NIL":
				}

				r.Applied = true
				kv.rcdmap[c.ID.ClientId] = r

				if r.Done != nil {
					go func() {
						select {
						case r.Done <- struct{}{}:
						// 接收端可能已经退出（比如已经不是leader了）
						case <-time.After(1 * time.Second):
						}
					}()
				}

				kv.lastApplied = apl.CommandIndex

				if Debug {
					DFPrintf(kv.me, "\tcommand %v applied, rcdmap[%v]=%v\n",
						apl.CommandIndex, c.ID.ClientId, r)
				}
				kv.mu.Unlock()

				// snapshot
				if kv.rf.GetRaftStateSize() >= kv.maxraftstate {
					snapshot := kv.encodeSnapshot()
					kv.rf.Snapshot(kv.lastIncludedIndex, snapshot)
				}
			} else if apl.SnapshotValid {
				if Debug {
					DFPrintf(kv.me, "\tapply snapshot start (lastIncludedIndex: %v, lastIncludedTerm: %v)",
						apl.SnapshotIndex, apl.SnapshotTerm)
				}
				kv.decodeSnapShot(apl.Snapshot)
				if Debug {
					DFPrintf(kv.me, "\tapply snapshot end (lastIncludedIndex: %v, lastIncludedTerm: %v)",
						apl.SnapshotIndex, apl.SnapshotTerm)
				}
			}

		case <-kv.deadch:
			return
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	go func() {
		for {
			select {
			case kv.deadch <- struct{}{}:
			case <-time.After(5 * time.Second):
				close(kv.deadch)
				return
			}
		}
	}()
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.deadch = make(chan struct{})
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.kvmap = make(map[string]string)
	kv.rcdmap = make(map[string]Record)
	kv.lastApplied = 0
	kv.lastIncludedIndex = 0
	kv.decodeSnapShot(persister.ReadSnapshot())

	go kv.apply()
	// You may need initialization code here.

	return kv
}

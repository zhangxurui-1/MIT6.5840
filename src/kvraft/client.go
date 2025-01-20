package kvraft

import (
	"crypto/rand"
	"log"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id           string
	counter      int
	leaderServer int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.counter = 0
	ck.id = randstring(20)
	ck.leaderServer = -1
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	if ck.leaderServer < 0 {
		ck.leaderServer = int(nrand()) % len(ck.servers)
	}

	ck.counter++
	args := GetArgs{
		Key: key,
		ID:  ArgsId{ClientId: ck.id, SerialNum: ck.counter},
	}
	reply := GetReply{}

	err := ""
	for {
		ok := ck.servers[ck.leaderServer].Call("KVServer.Get", &args, &reply)
		err = string(reply.Err)
		if err == OK {
			return reply.Value
		}
		if !ok {
			ck.leaderServer = (ck.leaderServer + 1) % len(ck.servers)
		} else if err == ErrNoKey {
			return ""
		} else if err == ErrWrongLeader {
			if reply.Leader >= 0 {
				ck.leaderServer = reply.Leader
			} else {
				ck.leaderServer = (ck.leaderServer + 1) % len(ck.servers)
			}
		} else {
			log.Fatalf("unexpected Err type: %v int reply %v\n", err, reply)
		}
	}

	// You will have to modify this function.
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

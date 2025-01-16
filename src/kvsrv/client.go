package kvsrv

import (
	"crypto/rand"
	"fmt"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	id     string
	reqn   int
	server *labrpc.ClientEnd
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	ck.reqn = 0
	ck.id = randstring(20)
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.

func (ck *Clerk) Get(key string) string {
	id := ArgID{EndName: ck.id, SeqNum: ck.reqn}
	args := GetArgs{Key: key, Aid: id}
	reply := GetReply{}

	ok := ck.server.Call("KVServer.Get", &args, &reply)
	for !ok {
		reply = GetReply{}
		ok = ck.server.Call("KVServer.Get", &args, &reply)
	}

	ck.reqn++
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	id := ArgID{EndName: ck.id, SeqNum: ck.reqn}
	args := PutAppendArgs{Key: key, Value: value, Aid: id}
	reply := PutAppendReply{}

	ok := false

	for !ok {
		if op == "Put" {
			ok = ck.server.Call("KVServer.Put", &args, &reply)
		} else if op == "Append" {
			ok = ck.server.Call("KVServer.Append", &args, &reply)
		} else {
			fmt.Println("unexpected opration")
			return ""
		}
	}

	ck.reqn++

	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}

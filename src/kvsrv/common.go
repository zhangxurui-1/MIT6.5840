package kvsrv

import "fmt"

// Put or Append
type PutAppendArgs struct {
	Aid         ArgID
	Key         string
	Value       string
	Seq_Confirm []int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Aid   ArgID
	Value string
}

type GetArgs struct {
	Aid         ArgID
	Key         string
	Seq_Confirm []int // sequence numbers of requests whose reply has arrived at client
}

type GetReply struct {
	Aid   ArgID
	Value string
}

// ArgID用于唯一标识一个Client的request
type ArgID struct {
	EndName string
	SeqNum  int
}

func (pargs *PutAppendArgs) String() string {
	s := fmt.Sprintf("{ArgID: %v, Seq_Confirm: %v, Key: %v, Value: %v}", pargs.Aid, pargs.Seq_Confirm, pargs.Key, pargs.Value)
	return s
}

func (gargs *GetArgs) String() string {
	s := fmt.Sprintf("{ArgID: %v, Seq_Confirm: %v, Key: %v}", gargs.Aid, gargs.Seq_Confirm, gargs.Key)
	return s
}

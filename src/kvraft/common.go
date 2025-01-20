package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	ID    ArgsId
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	RequestId ArgsId
	Err       Err
	Leader    int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ID ArgsId
}

type GetReply struct {
	RequestId ArgsId
	Err       Err
	Value     string
	Leader    int
}

// ArgId用于唯一标识一个Client的request
type ArgsId struct {
	ClientId  string
	SerialNum int
}

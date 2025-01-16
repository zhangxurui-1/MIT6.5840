package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type RPCTask struct {
	TaskType   int    // 任务类型(Map/Reduce)
	TaskId     int    // 任务编号
	InputFile  string // 输入文件
	OutputFile string // 输出文件
	TS         TimeStamp
	N_Reduce   int
}

// 一些常量定义
const (
	T_MAPTASK         = 0
	T_REDUCETASK      = 1
	T_FINISH          = 2
	T_WAIT            = 3
	T_DEFAULT         = -1
	PHASE_MAP         = 0
	PHASE_REDUCE      = 1
	ST_UNALLOCATE     = 0
	ST_RUNNING        = 1
	RESPOND_TIMELIMIT = 10
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

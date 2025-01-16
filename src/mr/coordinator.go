package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type TimeStamp struct {
	N int
	T time.Time
}

type Task struct {
	taskType   int       // 任务类型(Map/Reduce)
	taskId     int       // 任务ID，就是一个编号
	m_iFile    string    // Map任务的输入文件
	m_contents string    // Map任务输入文件的内容
	r_iFile    []string  // Reduce任务的输入文件
	status     int       // 当前任务的状态(未分配/执行中)，已完成的任务会直接删除掉
	dtime      TimeStamp // 任务最近一次分发的时间
	nReduce    int       // Reduce任务的数量
}

type Coordinator struct {
	// Your definitions here.
	mapTasks    map[int]Task // 用于存放Map任务
	reduceTasks map[int]Task // 用于存放Reduce任务
	nReduce     int
	phase       int // 当前阶段(Map阶段/Reduce阶段)
	req         chan *RPCTask
	resp        chan *RPCTask
	done        bool // 整个MapReduce任务是否完成
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// 用于worker请求任务
// request: completed task
// reply: empty struct for allocating new task
func (c *Coordinator) RequestTask(request *RPCTask, reply *RPCTask) error {
	c.req <- request
	for {
		select {
		case r := <-c.resp:
			reply.TaskType = r.TaskType
			reply.TaskId = r.TaskId
			reply.TS = r.TS
			reply.N_Reduce = r.N_Reduce
			reply.InputFile = r.InputFile
			return nil
		default:

		}
	}
}

func (c *Coordinator) schedule() {
	c.phase = PHASE_MAP
	tasks := &c.mapTasks
	n := 0
	for {
		select {
		case request := <-c.req:
			n = 0

			// worker的request中表示有任务完成
			if request.TaskType == T_MAPTASK || request.TaskType == T_REDUCETASK {
				t := (*tasks)[request.TaskId]
				// 在规定时间内接收到worker的响应，相应的Task才算完成
				if drt := time.Since(t.dtime.T); request.TS.N == t.dtime.N && drt <= RESPOND_TIMELIMIT*time.Second {
					delete(*tasks, request.TaskId)
				}
			}

			// 如果所有的Map Task都完成，则进入Reduce阶段
			if c.phase == PHASE_MAP && len(*tasks) == 0 {
				c.phase = PHASE_REDUCE
				for i := 0; i < c.nReduce; i++ {
					c.reduceTasks[i] = Task{
						taskType: T_REDUCETASK,
						taskId:   i,
						status:   ST_UNALLOCATE,
					}
				}
				tasks = &c.reduceTasks
			}

			if c.phase == PHASE_REDUCE && len(*tasks) == 0 {
				resp := RPCTask{TaskType: T_FINISH}
				c.resp <- &resp
			} else {
				// 分发新的任务
				// 默认分发一个WAIT(等待)任务，如果后续的循环找到了可分配的任务，再修改resp的值
				resp := RPCTask{TaskType: T_WAIT}
				for tid, task := range *tasks {
					if task.status == ST_UNALLOCATE || time.Since(task.dtime.T) > RESPOND_TIMELIMIT*time.Second {
						task.dtime.N++
						task.dtime.T = time.Now()
						task.status = ST_RUNNING
						(*tasks)[tid] = task

						if c.phase == PHASE_MAP {
							resp.TaskType = T_MAPTASK
							resp.InputFile = task.m_iFile
						} else {
							resp.TaskType = T_REDUCETASK
						}

						resp.TaskId = task.taskId
						resp.TS = task.dtime
						resp.N_Reduce = c.nReduce

						// fmt.Println("response:", resp)
						break
					}
				}
				c.resp <- &resp

			}

		default:
			if c.phase == PHASE_REDUCE && len(*tasks) == 0 && n >= 40 {
				c.done = true
				return
			}
			n++
			time.Sleep(250 * time.Millisecond)
		}
	}

}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	c.schedule()
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	// Your code here.

	return c.done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// fmt.Println("initializing...")
	c := Coordinator{}

	// Your code here.
	c.done = false
	c.nReduce = nReduce
	c.phase = 0
	c.mapTasks = make(map[int]Task)
	c.reduceTasks = make(map[int]Task)
	c.req = make(chan *RPCTask)
	c.resp = make(chan *RPCTask)
	for i, f := range files {
		// 一个文件对应一个map任务
		t := Task{
			taskType: T_MAPTASK,
			taskId:   i,
			m_iFile:  f,
			status:   ST_UNALLOCATE,
			dtime:    TimeStamp{0, time.Now()},
		}

		c.mapTasks[i] = t
	}

	c.server()
	return &c
}

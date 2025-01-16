package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/fs"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// CallExample()

	// Your worker implementation here.
	var t *Task = GetTask(nil)
	for {

		if t == nil {
			fmt.Println("get task failed")
			return
		}

		if t.taskType == T_MAPTASK {
			if ok := doMapTask(mapf, t); !ok {
				fmt.Printf("failed to execute Map task %v\n", t.taskId)
				t = nil
			}
		} else if t.taskType == T_REDUCETASK {
			if ok := doReduceTask(reducef, t); !ok {
				fmt.Printf("failed to execute Reduce task %v\n", t.taskId)
				t = nil
			}
		} else if t.taskType == T_WAIT {
			time.Sleep(500 * time.Millisecond)
			t = nil

		} else if t.taskType == T_FINISH {
			// fmt.Println("Exit")
			break
		}
		t = GetTask(t) // 传入旧的task，获得新的task
		//time.Sleep(1 * time.Second)
	}

}

func GetTask(task *Task) *Task {
	// request中封装worker刚完成的任务
	request := RPCTask{}
	if task == nil {
		request.TaskType = T_DEFAULT
		request.TaskId = -1
	} else {
		request.TaskType = task.taskType
		request.TaskId = task.taskId
		request.TS = TimeStamp{N: task.dtime.N}
	}
	// reply用于接收新任务
	reply := RPCTask{}

	ok := call("Coordinator.RequestTask", &request, &reply)
	if !ok {
		fmt.Println("call Coordinator.RequestTask failed.")
		return nil
	}

	if reply.TaskType == T_MAPTASK {
		var t = Task{
			taskType: reply.TaskType,
			taskId:   reply.TaskId,
			m_iFile:  reply.InputFile,
			dtime:    reply.TS,
			nReduce:  reply.N_Reduce,
		}
		t.m_contents = ReadFile(reply.InputFile)
		return &t
	} else if reply.TaskType == T_REDUCETASK {
		var t = Task{
			taskType: reply.TaskType,
			taskId:   reply.TaskId,
			dtime:    reply.TS,
			nReduce:  reply.N_Reduce,
		}
		t.r_iFile = inputFiles_Reduce(t.taskId)
		return &t

	} else if reply.TaskType == T_FINISH {
		var t = Task{
			taskType: T_FINISH,
		}
		return &t
	} else if reply.TaskType == T_WAIT {
		var t = Task{
			taskType: T_WAIT,
		}
		return &t
	}
	return nil
}

func inputFiles_Reduce(taskId int) []string {
	taskIdStr := strconv.Itoa(taskId)
	files := make([]string, 0)
	filepath.Walk(".", func(path string, info fs.FileInfo, err error) error {

		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		filename := info.Name()

		if len(filename) > 8 && filename[:3] == "mr-" && filename[len(filename)-6-len(taskIdStr):] == "-"+taskIdStr+".json" {
			files = append(files, filename)
		}
		return nil
	})
	return files
}

// 执行Map Task
func doMapTask(mapf func(string, string) []KeyValue, task *Task) bool {
	// fmt.Printf("executing Map task %v...\n", task.taskId)

	filename := task.m_iFile

	contents := ReadFile(filename)
	kva := mapf(filename, contents)
	// sort.Sort(ByKey(kva))

	intermediate := make(map[int][]KeyValue, task.nReduce)
	for i := 0; i < len(kva); i++ {
		pos := ihash(kva[i].Key) % task.nReduce
		intermediate[pos] = append(intermediate[pos], kva[i])
	}

	for i := 0; i < task.nReduce; i++ {
		ofilename := fmt.Sprintf("mr-%d-%d.json", task.taskId, i)
		ofile, err := os.OpenFile(ofilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		enc := json.NewEncoder(ofile)
		if err != nil {
			fmt.Printf("failed to open %v\n", ofilename)
			return false
		}

		for _, kv := range intermediate[i] {
			err := enc.Encode(&kv)
			if err != nil {
				fmt.Printf("failed to write to %v\n", ofilename)
				return false
			}
		}
		ofile.Close()
	}
	return true
}

// 执行Reduce Task
func doReduceTask(reducef func(string, []string) string, task *Task) bool {
	// fmt.Println(task.r_iFile)
	// fmt.Printf("executing Reduce task %v...\n", task.taskId)

	// 读取intermediate files
	kva := make([]KeyValue, 0)
	if len(task.r_iFile) == 0 {
		fmt.Println("no intermediate files")
		return false
	}
	for _, f := range task.r_iFile {
		ifile, err := os.Open(f)
		if err != nil {
			fmt.Printf("failed to open %v\n", f)
			return false
		}
		// 从json文件decode
		dec := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

		ifile.Close()
	}

	sort.Sort(ByKey(kva))

	i := 0
	ofilename_tmp := "mr-out-" + strconv.Itoa(task.taskId) + "-tmp"
	ofile, _ := os.Create(ofilename_tmp)

	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofilename := "mr-out-" + strconv.Itoa(task.taskId)
	os.Rename(ofilename_tmp, ofilename)
	ofile.Close()

	return true
}

func ReadFile(file string) string {
	f, err := os.Open(file)
	if err != nil {
		log.Fatalf("cannot open %v", file)
	}
	content, err := io.ReadAll(f)
	if err != nil {
		log.Fatalf("cannot read %v", file)
	}
	f.Close()
	return string(content)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err) // unexpected EOF
	return false
}

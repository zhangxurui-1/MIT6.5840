package main

import (
	"fmt"
	"time"
)

type Task struct {
	status int
	ch     chan struct{}
}

var tasks map[int]Task
var global = make(chan int)

func main() {
	initial()
	for i := 0; i < 10; i++ {
		go request()
	}
	schedule()
}

func initial() {
	tasks = make(map[int]Task, 10)
	for i := 0; i < 10; i++ {
		t := Task{status: 0, ch: make(chan struct{})}
		tasks[i] = t
		go func(idx int) {
			tasks[i].ch <- struct{}{}
		}(i)
	}
}

func request() {
	time.Sleep(time.Second)
	for i, t := range tasks {
		select {
		case <-tasks[i].ch:
			if tasks[i].status == 1 {
				break
			}
			t.status = 1
			tasks[i] = t
			fmt.Printf("task %v done\n", i)
			global <- i
			return
		default:
		}
	}
}

func schedule() {
	n := len(tasks)
	fmt.Println(n)
	for {
		n = len(tasks)
		fmt.Println(n)
		if n == 0 {
			return
		}
		select {
		case idx := <-global:
			delete(tasks, idx)
			n--
		default:
			time.Sleep(time.Second)
		}

	}

}

func f1() {

	ch := make(chan int)
	go f2(ch)
	for {
		// select {
		// case ch <- 1:
		// 	fmt.Println("write to channel")
		// default:
		// 	fmt.Println("blocked")
		// }

		fmt.Println("write to the channel")
		ch <- 1

		// time.Sleep(500 * time.Millisecond)
		time.Sleep(2 * time.Second)
	}

}

func f2(ch chan int) {
	for {
		select {
		case <-ch:
			fmt.Println("Read from channel")
		default:
			fmt.Println("blocked")
		}
		time.Sleep(time.Second)
	}
}

package raft

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
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

func LogFmtString(log map[int]Entry) string {
	s := ""
	keys := make([]int, 0, len(log))
	for key := range log {
		keys = append(keys, key)
	}

	// 对键进行排序
	sort.Ints(keys)

	// 按照键的顺序输出元素
	for _, key := range keys {
		value := log[key]
		s += fmt.Sprintf("\t\t%v: %v\n", key, value)
	}
	return s
}

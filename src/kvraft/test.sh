#!/bin/bash

# 设置超时时间
timeout_duration=300

# 设置最大运行次数
max_runs=20
current_run=0

# 循环执行测试
while [ $current_run -lt $max_runs ]; do
    current_run=$((current_run + 1))
    echo "Test $current_run / $max_runs..."
    
    # 使用 timeout 命令运行测试，并设置超时
    # timeout $timeout_duration go test -race -run TestPersistPartitionUnreliableLinearizable4A
    # go test -race -run 4A
    # timeout $timeout_duration go test -race -run TestManyPartitionsManyClients4A
    # timeout $timeout_duration go test -race -run TestPersistConcurrentUnreliable4A
    go test -race -run 4B
    # timeout $timeout_duration go test -race -run TestSnapshotUnreliable4B
    
    # 检查上一个命令的退出状态
    if [ $? -eq 0 ]; then
        # 删除当前文件夹下的所有 .txt 文件
        rm -f *.txt
        echo "All .txt files have been deleted."
        echo
    else
        # 中断脚本
        echo "Stopping"
        echo
        break
    fi
done

echo "finished."
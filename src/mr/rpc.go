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

type TaskType int

const (
	Map    TaskType = 1
	Reduce TaskType = 2
	Done   TaskType = 3
)

// GetTask Rpcs 被闲置worker发送给coordinator请求任务

// 请求任务args为空
type GetTaskArgs struct{}

type GetTaskReply struct {
	TaskType     TaskType // 什么类型请求？
	TaskNum      int      // 请求map/reduce数量
	NReduceTasks int      //
	MapFile      string   // 要map的文件名
	NMapTasks    int
}

// FinishedTask RPCs 被闲置worker发给coordinator标明一个task完成
type FinishedTaskArgs struct {
	TaskType TaskType
	TaskNum int // 哪个task
}

// workers不需要等回复
type FinishedTaskReply struct {}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
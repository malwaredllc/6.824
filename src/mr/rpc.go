package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

type TaskType string

const (
	Map = "Map"
	Reduce = "Reduce"
	Retry = "Retry"
	Exit = "Exit"
	TaskTimeout = 15 * time.Second
	RetryInterval = 3 * time.Second
)

type TaskArgs struct {}

type TaskResponse struct {
	Id			int
	Type 		TaskType	
	Num			int
	NReduce		int
	Target		string
	done		bool
}

type DoneArgs struct {
	Id		int
	Type	TaskType
}

type DoneResponse struct {}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

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

type TaskArgs struct {
	WorkerId	int
}

type TaskResponse struct {
	Index		int
	Type 		TaskType	
	File 		string
	NReduce		int
}

type DoneArgs struct {
	Index		int
	Type		TaskType
	WorkerId 	int
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

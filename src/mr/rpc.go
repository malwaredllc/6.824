package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type TaskType int

const (
	Map = 1
	Reduce = 2
)

type TaskArgs struct {}

type TaskResponse struct {
	Type 		TaskType	
	JobNum		int
	RedNum		int
	Target 		string
}

type DoneArgs struct {
	Target 	string
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

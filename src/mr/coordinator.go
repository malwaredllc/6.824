package mr

import (
	"log"
	"net"
	"os"
	"net/rpc"
	"net/http"
	"sync"
	"time"
)

type TaskType int
type TaskStatus int

// task types
const (
	Map TaskType = iota
	Reduce 
	Retry
	Exit
)

// task statuses
const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

// other consts
const (
	TempDir = "."
	TaskTimeout = 15 * time.Second
	RetryInterval = 3 * time.Second
	NoWorkerAssigned = -1
)
type Task struct {
	Index	 	int
	WorkerId	int
	File		string
	Type		TaskType
	Status		TaskStatus
}

type Coordinator struct {
	mapTasks		[]*Task
	reduceTasks		[]*Task	
	mapRemaining 	int
	reduceRemaining int
	nReduce			int	
	mu				sync.RWMutex
}

func (c *Coordinator) GetTask(args *TaskArgs, res *TaskResponse) error {
	workerId := args.WorkerId

	// get next task
	var task *Task
	if c.mapRemaining > 0 {
		task = c.assignMapTask(workerId)
	} else if c.reduceRemaining > 0 {
		task = c.assignReduceTask(workerId)
	} else {
		task = c.assignExitTask(workerId)
	}

	// update response object with task info
	res.Index = task.Index
	res.Type = task.Type
	res.File = task.File
	res.NReduce = c.nReduce

	// monitor task for completion and re-execute if necessary
	go c.monitorTaskProgress(task)
	return nil
}

// RPC handler for worker to report map task is done.
// We update the worker ID of the task to whichever worker ID first reported it done.
// This is because it is possible the task has been re-assigned due to timeout (taking
// too long to finish), so we want to know which machine the completed output file is on.
// In this local example it isn't useful since the worker ID is just the worker PID, but
// when ran in a cluster it would be used to know which machine the output file is on.
func (c *Coordinator) TaskDone(args *DoneArgs, res *DoneResponse) error {
	i := args.Index
	workerId := args.WorkerId

	c.mu.Lock()
	defer c.mu.Unlock()

	if args.Type == Map && c.mapTasks[i].Status != Completed {
			c.mapTasks[i].Status = Completed
			c.mapTasks[i].WorkerId = workerId
			c.mapRemaining--
	} else if args.Type == Reduce && c.reduceTasks[i].Status != Completed {
			c.reduceTasks[i].Status = Completed
			c.reduceTasks[i].WorkerId = workerId
			c.reduceRemaining--
	}

	return nil
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
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reduceRemaining == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	n := len(files)
	c := Coordinator{}
	c.mapTasks = make([]*Task, n)
	c.reduceTasks = make([]*Task, nReduce)
	c.mapRemaining = n
	c.reduceRemaining = nReduce
	c.nReduce = nReduce

	// create list of ready unassigned map tasks
	for i, file := range files {
		c.mapTasks[i] = &Task{
			Index:		i,
			WorkerId:	NoWorkerAssigned,
			File:		file,
			Type:		Map,
			Status:		Idle,
		}
	}

	// create list of ready unassigned reduce tasks
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = &Task{
			Index:		i,
			WorkerId:	NoWorkerAssigned,
			File:		"",
			Type:		Reduce,
			Status:		Idle,
		}
	} 

	//log.Printf("created coordinator")
	c.server()
	return &c
}

// Assigns next idle map task and spins off a thread to monitor it's progress
func (c *Coordinator) assignMapTask(workerId int) *Task {
	c.mu.Lock()
	defer c.mu.Unlock()

	var task *Task
	for i := 0; i < len(c.mapTasks); i++ {
		if c.mapTasks[i].Status == Idle {
			task = c.mapTasks[i]
			task.Index = i
			task.Status = InProgress
			task.WorkerId = workerId
			return task
		}
	}
	return &Task{Type: Retry, Index: -1, Status: Completed, WorkerId: workerId}
}

func (c *Coordinator) assignReduceTask(workerId int) *Task {
	c.mu.Lock()
	defer c.mu.Unlock()

	var task *Task
	for i := 0; i < len(c.reduceTasks); i++ {
		if c.reduceTasks[i].Status == Idle {
			task = c.reduceTasks[i]
			task.Index = i
			task.Status = InProgress
			task.WorkerId = workerId
			return task
		}
	}
	return &Task{Type: Retry, Index: -1, Status: Completed, WorkerId: workerId}
}

func (c *Coordinator) assignExitTask(workerId int) *Task {
	return &Task{
		Index:		-1,
		WorkerId:	workerId,
		File:		"",
		Type:		Exit,
		Status:		Completed,		
	}
}

// check on task in [TaskTimeout] seconds and mark it idle if it's still not done
func (c *Coordinator) monitorTaskProgress(task *Task) {
	time.Sleep(TaskTimeout)
	c.mu.Lock()
	defer c.mu.Unlock()
	if task.Status != Completed {
		log.Printf("marking task %d as idle", task.Index)
		task.Status = Idle
		task.WorkerId = NoWorkerAssigned
	}
}
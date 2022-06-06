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

const (
	TempDir = "tmp"
	Map = "Map"
	Reduce = "Reduce"
	Retry = "Retry"
	Exit = "Exit"
	TaskTimeout = 15 * time.Second
	RetryInterval = 3 * time.Second
)


type Coordinator struct {
	mapCh			chan string
	reduceCh		chan int
	mapTasks		map[int]*TaskResponse
	reduceTasks		map[int]*TaskResponse
	numFiles		int
	nReduce			int	
	mapNum			int
	taskId			int
	mapRemaining 	int
	reduceRemaining int
	mu				sync.RWMutex
}

func (c *Coordinator) GetTask(args *TaskArgs, res *TaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.mapCh) > 0 {
		c.assignMapTask(res)
	} else if len(c.reduceCh) > 0 {
		c.assignReduceTask(res)
	} else {
		c.assignExitTask(res)
	}
	return nil
}

// RPC handler for worker to report map task is done
func (c *Coordinator) TaskDone(args *DoneArgs, res *DoneResponse) error {
	log.Printf("Task %d %s done", args.Id, args.Type)
	if args.Type == Map {
		c.mu.Lock()
		c.mapTasks[args.Id].done = true
		c.mapRemaining--
		c.mu.Unlock()
	} else {
		c.mu.Lock()
		c.reduceTasks[args.Id].done = true
		c.reduceRemaining--
		c.mu.Unlock()
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
	c.mapCh = make(chan string, n)
	c.reduceCh = make(chan int, nReduce)
	c.mapTasks = make(map[int]*TaskResponse)
	c.reduceTasks = make(map[int]*TaskResponse)
	c.mapRemaining = n
	c.reduceRemaining = nReduce
	c.numFiles = n
	c.nReduce = nReduce
	c.mapNum = 0
	c.taskId = 0

	// create thread safe queue of map tasks (files to process)
	for _, file := range files {
		c.mapCh <- file
	}

	// create thread safe queue for reduce tasks
	for i := 0; i < nReduce; i++ {
		c.reduceCh <- i
	} 

	log.Printf("created coordinator")
	c.server()
	return &c
}

func (c *Coordinator) assignMapTask(res *TaskResponse) error {
	// update task response with values for next map task
	filename := <-c.mapCh
	res.Id = c.taskId
	res.Type = Map
	res.Target = filename
	res.Num = c.mapNum
	res.NReduce = c.nReduce
	res.done = false

	// increment counters and update task map
	c.mapTasks[res.Id] = res
	c.taskId++
	c.mapNum++

	// if task not done in 10 seconds, re-enqueue it
	go func(taskId int) {
		time.Sleep(TaskTimeout)
		c.mu.RLock()
		defer c.mu.RUnlock()
		if !c.mapTasks[taskId].done {
			log.Printf("re-enqueueing task %s %d", res.Type, res.Num)
			c.mapCh <- res.Target // this is the file to map
		} else {
			log.Printf("task %s %d confirmed done", res.Type, res.Num)
		} 
	}(res.Id)

	return nil
}

func (c *Coordinator) assignReduceTask(res *TaskResponse) error {
	// if some map tasks are still processing, tell worker to retry later
	if c.mapRemaining > 0 {
		log.Printf("Not all map tasks done, can't assigning reduce task yet...")
		res.Id = c.taskId
		res.Type = Retry
		c.taskId++
		return nil
	}

	// otherwise assign a reduce task
	redNum := <-c.reduceCh
	res.Id = c.taskId
	res.Type = Reduce
	res.Target = "" 	// no target for reduce task
	res.Num = redNum
	res.NReduce = c.nReduce

	// increment counter and store reduce task
	c.taskId++
	c.reduceTasks[res.Id] = res

	// if task not done in 10 seconds, re-enqueue it
	go func(taskId int) {
		time.Sleep(TaskTimeout)
		c.mu.RLock()
		defer c.mu.RUnlock()
		if !c.reduceTasks[taskId].done {
			log.Printf("re-enqueueing task %s %d", res.Type, res.Num)
			c.reduceCh <- res.Num // this is the reduce task number
		} else {
			log.Printf("task %s %d confirmed done", res.Type, res.Num)
		}
	}(res.Id)
	
	return nil
}

func (c *Coordinator) assignExitTask(res *TaskResponse) error {
	res.Id = c.taskId
	res.Type = Exit
	c.taskId++
	return nil
}
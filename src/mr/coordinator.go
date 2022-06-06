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

type Coordinator struct {
	mapCh			chan string
	redCh			chan int
	mapTasks		map[int]*TaskResponse
	redTasks		map[int]*TaskResponse
	numFiles		int
	nReduce			int	
	mapNum			int
	taskId			int
	mapRemaining 	int
	reduceRemaining int
	mu			sync.RWMutex
}

func (c *Coordinator) GetTask(args *TaskArgs, res *TaskResponse) error {
	// if map tasks are not done, try assigning one out
	if len(c.mapCh) != 0 {
		filename := <-c.mapCh

		// update task response with values for next map task
		c.mu.RLock()
		res.Id = c.taskId
		res.Type = Map
		res.Target = filename
		res.Num = c.mapNum
		res.NReduce = c.nReduce
		res.done = false
		c.mu.RUnlock()

		// increment counters and update task map
		c.mu.Lock()
		c.taskId++
		c.mapNum++
		c.mapTasks[res.Id] = res
		c.mu.Unlock()

		// if task not done in 10 seconds, re-enqueue it
		go func(taskId int) {
			time.Sleep(TaskTimeout)
			c.mu.RLock()
			if !c.mapTasks[taskId].done {
				log.Printf("re-enqueueing task %s %d", res.Type, res.Num)
				c.mapCh <- res.Target // this is the file to map
			} else {
				log.Printf("task %s %d confirmed done", res.Type, res.Num)
			} 
			c.mu.RUnlock()
		}(res.Id)

		return nil

	} else {
		
		for {
			// if map tasks all assigned but still processing, return nil so worker can retry later134G
			if c.mapRemaining > 0 {
				log.Printf("Not all map tasks done, can't assigning reduce task yet...")
				return nil
			
			// if reduce task ready, stop waiting
			} else if len(c.redCh) > 0 {
				break
			// if no reduce tasks left but not all assigned are done, exit
			} else {
				log.Printf("No reduce tasks left but not all assigned are done, exiting")
				os.Exit(0)
			}
		}

		redNum := <-c.redCh

		// now assign a reduce task
		c.mu.RLock()
		res.Id = c.taskId
		res.Type = Reduce
		res.Target = "" 	// no target for reduce task
		res.Num = redNum
		res.NReduce = c.nReduce
		c.mu.RUnlock()

		// increment counter and store reduce task
		c.mu.Lock()
		c.taskId++
		c.redTasks[res.Id] = res
		c.mu.Unlock()

		// if task not done in 10 seconds, re-enqueue it
		go func(taskId int) {
			time.Sleep(TaskTimeout)
			c.mu.RLock()
			if !c.redTasks[taskId].done {
				log.Printf("re-enqueueing task %s %d", res.Type, res.Num)
				c.redCh <- res.Num // this is the reduce task number
			} else {
				log.Printf("task %s %d confirmed done", res.Type, res.Num)
			}
			c.mu.RUnlock()
		}(res.Id)

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
		if c.allMapTasksDone() {
			// add each reducer id to channel
			for i := 0; i < c.nReduce; i++ {
				c.redCh <- i
			}
		}
	} else {
		c.mu.Lock()
		c.redTasks[args.Id].done = true
		c.reduceRemaining--
		c.mu.Unlock()
	}
	return nil
}

func (c *Coordinator) allMapTasksDone() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mapRemaining == 0
}

func (c *Coordinator) allReduceTasksDone() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.reduceRemaining == 0
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
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.reduceRemaining == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	n := len(files)
	c := Coordinator{}
	c.mapCh = make(chan string, n)
	c.redCh = make(chan int, nReduce)
	c.mapTasks = make(map[int]*TaskResponse)
	c.redTasks = make(map[int]*TaskResponse)
	c.mapRemaining = n
	c.reduceRemaining = nReduce
	c.numFiles = n
	c.nReduce = nReduce
	c.mapNum = 0
	c.taskId = 0

	// create thread safe queue of jobs (files to process)
	for _, file := range files {
		c.mapCh <- file
	}
	log.Printf("created coordinator")
	c.server()
	return &c
}

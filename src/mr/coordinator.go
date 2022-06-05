package mr

import (
	"fmt"
	"log"
	"net"
	"os"
	"net/rpc"
	"net/http"
	"sync"
	"errors"
	"time"
)

type Coordinator struct {
	jobCh		chan string
	mapCh		chan string
	doneCh		chan string
	nJobs		int
	nReduce		int	
	jobNum		int
	redNum		int
	jobsDone	bool
	mu			sync.Mutex
}

func (c *Coordinator) GetTask(args *TaskArgs, res *TaskResponse) error {
	// if job channel still open, assign it as a map task to worker
	if len(c.jobCh) > 0 {
		item := <-c.jobCh

		// update job number and assign it with target filename
		c.mu.Lock()
		res.Type = Map
		res.Target = item
		res.JobNum = c.jobNum
		res.RedNum = c.redNum
		c.jobNum++
		c.mu.Unlock()

		// re-enqueue job if file doesn't exist after 10 sec (worker crashed)
		go func(res *TaskResponse) {
			filepath := fmt.Sprintf("mr-tmp/mr-%d-%d.json", res.JobNum, res.RedNum-1)
			time.Sleep(10 * time.Second)
			if _, err := os.Stat(filepath); errors.Is(err, os.ErrNotExist) {
				c.jobCh <- res.Target
				log.Printf("Re-enqueued map %s because %s did not exist", res.Target, filepath)
			}
		}(res)

		return nil
	}
	return nil
}

// RPC handler for worker to report map task is done
func (c *Coordinator) MapDone(args *DoneArgs, res *DoneResponse) error {
	log.Printf("Map %s done", args.Target)
	c.mapCh <- args.Target

	// if all maps are done, start assigning reduce jobs
	if len(c.mapCh) == c.nJobs {
		c.jobsDone = true
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
	ret := false
	if len(c.doneCh) == c.nJobs {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	n := len(files)
	c := Coordinator{}
	c.jobCh = make(chan string, n)
	c.mapCh = make(chan string, n)
	// create thread safe queue of jobs (files to process)
	for _, file := range files {
		c.jobCh <- file
	}
	c.nJobs = n
	c.nReduce = nReduce
	c.jobNum = 0
	c.redNum = nReduce
	c.jobsDone = false
	log.Printf("created coordinator")
	c.server()
	return &c
}

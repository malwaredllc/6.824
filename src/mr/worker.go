package mr

import (
	"fmt"
	"os"
	"log"
	"time"
	"errors"
	"net/rpc"
	"hash/fnv"
	"io/ioutil"
	"encoding/json"
	"path/filepath"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	os.MkdirAll(TempDir, os.ModePerm)
	for {
		task := getTaskFromCoordinator()

		switch task.Type {

		case Map:
			doMapTask(mapf, task)
			notifyTaskDone(task.Index, task.Type)

		case Reduce:
			doReduceTask(reducef, task)
			notifyTaskDone(task.Index, task.Type)

		case Retry:
			//log.Printf("No task ready, retrying shortly...")
			time.Sleep(RetryInterval)

		case Exit:
			//log.Printf("Exiting")
			os.Exit(0)

		default:
			//log.Printf("Invalid task type received: %v", task.Type)
			os.Exit(1)
		}
	}
}

func getTaskFromCoordinator() *TaskResponse {
	args := TaskArgs{WorkerId: os.Getpid()}
	reply := TaskResponse{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		//log.Printf("Index: %d, Type: %v, File: %s, nReduce: %d", reply.Index, reply.Type, reply.File, reply.NReduce)
		return &reply
	} else {
		//log.Printf("call failed - coordinator is down. exiting\n")
		os.Exit(0)
	}
	return nil
}

func doMapTask(mapf func(string, string) []KeyValue, task *TaskResponse) error {
	intermediate := []KeyValue{}
	file, err := os.Open(task.File)
	if err != nil {
		log.Fatalf("cannot open %v", task.File)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.File)
	}
	file.Close()
	kva := mapf(task.File, string(content))
	intermediate = append(intermediate, kva...)

	// write to nReduce JSON files
	files := make([]*json.Encoder, task.NReduce)
	for i := 0; i < task.NReduce; i++ {
		path := fmt.Sprintf("%s/mr-%d-%d.json", TempDir, task.Index, i)
		f, err := os.Create(path)
		if err != nil {
			log.Fatalf("error creating %v", path)
		}
		enc := json.NewEncoder(f)
		files[i] = enc
		defer f.Close()
	}
	for _, kv := range intermediate {
		idx := ihash(kv.Key) % task.NReduce
		enc := files[idx]
		err := enc.Encode(&kv)
		if err != nil {
			//log.Printf("failed to encode %v", kv)
		}
	}
	return nil
}

func doReduceTask(reducef func(string, []string) string, task *TaskResponse) error {
	reduceId := task.Index
	files, err := filepath.Glob(fmt.Sprintf("%s/mr-*-%d.json", TempDir, reduceId))
	if err != nil {
		log.Fatalf("failed to read dir '%s'", TempDir)
	}

	// NOTE: using a hash table for grouping values for a key works for the examples
	// in this lab, however, for data sets too large to fit in an in-memory hash table,
	// we would have to store all key-value pairs on disk and do an external sort,
	// as described in the MapReduce paper https://pdos.csail.mit.edu/6.824/papers/mapreduce.pdf
	kvMap := make(map[string][]string)
	var kv KeyValue
	for _, filePath := range files {
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatalf("unable to open file %s", filePath)
		}

		// read json file and deserialize into key/value pairs
		dec := json.NewDecoder(file)
		for dec.More() {
			err = dec.Decode(&kv)
			if err != nil {
				//log.Printf("unable to decode json key/value pair")
			}
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}

		// Create temp file
		tmpFilePath := fmt.Sprintf("%v/mr-out-%v-%v", TempDir, reduceId, os.Getpid())
		tmpFile, err := os.Create(tmpFilePath)
		if err != nil {
			log.Fatalf("unable to create file %s", tmpFilePath)
		}

		// Call reduce and write to temp file
		for k, vals := range kvMap {
			v := reducef(k, vals)
			_, err := fmt.Fprintf(tmpFile, "%v %v\n", k, v)
			if err != nil {
				log.Fatalf("unable to write key/value pair %v - %v to file %s", k, v, tmpFilePath)
			}
		}

		// atomically rename temp files to ensure no one observes partial files
		tmpFile.Close()
		newPath := fmt.Sprintf("%s/mr-out-%v", TempDir, reduceId)
		err = os.Rename(tmpFilePath, newPath)
		if err != nil {
			log.Fatalf("unable to rename %s to %s", tmpFilePath, newPath)
		}
	}

	return nil
}

// notify coordinator task is done
func notifyTaskDone(taskId int, taskType TaskType) error {
	args := DoneArgs{Index: taskId, Type: taskType, WorkerId: os.Getpid()}
	reply := DoneResponse{}
	ok := call("Coordinator.TaskDone", &args, &reply)
	if ok {
		return nil
	}
	return errors.New(fmt.Sprintf("failed to send task done for %v %v", taskId, taskType))
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// coordinator is down, exit
		os.Exit(0)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

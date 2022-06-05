package mr

import (
	"fmt"
	"os"
	"log"
	"errors"
	"net/rpc"
	"hash/fnv"
	"io/ioutil"
	"encoding/json"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
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

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		task := GetTaskFromCoordinator()
		switch task.Type {
		case Map:
			// get key-value pairs
			intermediate := []KeyValue{}
			file, err := os.Open(task.Target)
			if err != nil {
				log.Fatalf("cannot open %v", task.Target)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", task.Target)
			}
			file.Close()
			kva := mapf(task.Target, string(content))
			intermediate = append(intermediate, kva...)

			sort.Sort(ByKey(intermediate))

			// write to nReduce JSON files
			for redNum := 0; redNum < task.RedNum; redNum++ {
				mapFile := fmt.Sprintf("mr-tmp/mr-%d-%d.json", task.JobNum, redNum)
				f, err := os.Create(mapFile)
				if err != nil {
					log.Fatalf("cannot create %v", mapFile)
				}
				defer f.Close()

				enc := json.NewEncoder(f)
				for _, kv := range intermediate {
					err := enc.Encode(&kv)
					if err != nil {
						log.Printf("error encoding to json: %v", err)
					}
				}
				log.Printf("created %s", mapFile)
			}
			break

		case Reduce:
			log.Printf("reduce task received: %s", task.Target)

			oname := "mr-out-0"
			ofile, _ := os.Create(oname)
		
			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)
		
				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		
				i = j
			}
		
			ofile.Close()
			break
		default:
			log.Printf("invalid task type: %d, %s", task.Type, task.Target)
		}
	}
}

func GetTaskFromCoordinator() *TaskResponse {
	// declare args structure
	args := TaskArgs{}

	// declare a reply structure.
	reply := TaskResponse{}

	// send the RPC request, wait for the reply.
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		var t string
		if reply.Type == Map {
			t = "Map"
		} else {
			t = "Reduce"
		}
		log.Printf("got task: %s %d %s\n", t, reply.Type, reply.Target)
		return &reply
	} else {
		log.Printf("call failed!\n")
	}
	return nil
}

func SendMapDone(filename string) error {
	args := DoneArgs{Target: filename}
	reply := DoneResponse{}
	ok := call("Coordinator.MapDone", &args, &reply)
	if ok {
		return nil
	}
	return errors.New(fmt.Sprintf("failed to send map done for %s", filename))
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
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

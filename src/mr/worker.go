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
	"regexp"
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
		if task.Type != Map {
			log.Printf("done")
			break
		}

		// MAP
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

		// write to nReduce JSON files
		files := make([]*json.Encoder, task.RedNum)
		for i := 0; i < task.RedNum; i++ {
			path := fmt.Sprintf("mr-tmp/mr-%d-%d.json", task.JobNum, i)
			f, err := os.Create(path)
			if err != nil {
				log.Fatalf("error creating %v", path)
			}
			enc := json.NewEncoder(f)
			files[i] = enc
			defer f.Close()
		}
		for _, kv := range intermediate {
			idx := ihash(kv.Key) % task.RedNum
			enc := files[idx]
			err := enc.Encode(&kv)
			if err != nil {
				log.Printf("failed to encode %v", kv)
			}
		}	

		// REDUCE
		mapFiles, err := ioutil.ReadDir("mr-tmp")
		if err != nil {
			log.Fatalf("failed to read dir mr-tmp")
		}

		// find each map file for this reduce task ("mr-[job]-[task].json")
		for i := 0; i < task.RedNum; i++ {
			intermediate := []KeyValue{}
			pattern := fmt.Sprintf("-%d.json", i)
			for _, mapFile := range mapFiles {
				matched, _ := regexp.MatchString(pattern, mapFile.Name())
				if matched {
					file, err = os.Open(fmt.Sprintf("mr-tmp/%s", mapFile.Name()))
					if err != nil {
						log.Fatalf("couldn't open %s", mapFile.Name())
					}
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						intermediate = append(intermediate, kv)
					}
				}
			}

			// sort keys
			sort.Sort(ByKey(intermediate))

			// create output file
			oname := fmt.Sprintf("mr-tmp/mr-out-%d", i)
			ofile, _ := os.Create(oname)
		
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-[task]
			idx := 0
			for idx < len(intermediate) {
				j := idx + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[idx].Key {
					j++
				}
				values := []string{}
				for k := idx; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[idx].Key, values)
		
				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[idx].Key, output)
		
				idx = j
			}
			ofile.Close()
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
		log.Printf("Target: %s, nReduce: %d,  JobNum: %d", reply.Target, reply.RedNum, reply.JobNum)
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

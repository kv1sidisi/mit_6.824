package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type IAmWorker struct {
	id int
	c  *rpc.Client
}

func makeClient(serverAddr string, port string) *rpc.Client {
	client, err := rpc.DialHTTP("tcp", serverAddr+":"+port)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	return client
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func (w *IAmWorker) call(rpcname string, args any, reply any) error {
	err := w.c.Call(rpcname, args, reply)
	if err != nil {
		return nil
	}

	return err
}

// SayHello reisters worker in coordinator
func (w *IAmWorker) SayHello() error {
	args := EmptyArgs{}

	reply := HelloReply{}

	err := w.call("Coordinator.Hello", &args, &reply)
	if err != nil {
		return err
	}
	w.id = reply.ID
	return nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	worker := IAmWorker{}
	worker.c = makeClient("127.0.0.1", "1234")

	err := worker.SayHello()
	if err != nil {
		log.Fatal("couldn't get id:", err)
	}

	fmt.Println(worker.id)

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

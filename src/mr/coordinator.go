package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

type Coordinator struct {
	workers []ActiveWorker
	// Your definitions here.
}

type ActiveWorker struct {
	id int
}

// Your code here -- RPC handlers for the worker to call.

// Hello is rpc method
// returns registered worker id
func (c *Coordinator) Hello(args *EmptyArgs, reply *HelloReply) error {
	nextID := len(c.workers)
	worker := ActiveWorker{
		id: nextID,
	}
	c.workers = append(c.workers, worker)
	reply.ID = nextID
	fmt.Println("registered", c.workers)
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":1234")
	if err != nil {
		log.Fatal("listen error:", err)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server()
	return &c
}

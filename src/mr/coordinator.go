package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
)

type CoordinatorPhase string

const (
	mapPhase    CoordinatorPhase = "mapPhase"
	reducePhase CoordinatorPhase = "reducePhase"
	donePhase   CoordinatorPhase = "donePhase"
)

type TaskType string

const (
	mapType    TaskType = "mapType"
	reduceType TaskType = "reduceType"
)

type TaskMeta struct {
	taskType     TaskType
	iD           int
	filename     string
	retriedTimes int
}

type Queue struct {
	data []*TaskMeta
}

func (q *Queue) Enqueue(v *TaskMeta) {
	q.data = append(q.data, v)
}

func (q *Queue) Dequeue() (*TaskMeta, bool) {
	if len(q.data) == 0 {
		return nil, false
	}
	v := q.data[0]
	q.data = q.data[1:]
	return v, true
}

func (q *Queue) IsEmpty() bool {
	return len(q.data) == 0
}

const maxRetries = 5

type Coordinator struct {
	mu      sync.RWMutex
	workers map[int]*ActiveWorker
	reduceN int
	phase   CoordinatorPhase

	mapQ    Queue
	reduceQ Queue

	mapTaskByID    map[int]*TaskMeta
	reduceTaskByID map[int]*TaskMeta

	retryQ     Queue
	maxRetries int
}

type ActiveWorker struct {
	id int
}

// Hello is rpc method
// returns registered worker id
func (c *Coordinator) Hello(args *EmptyArgs, reply *HelloReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	nextID := len(c.workers)
	worker := &ActiveWorker{
		id: nextID,
	}

	c.workers[nextID] = worker

	reply.ID = nextID
	fmt.Println("registered", c.workers)
	return nil
}

// GiveTask is rpc method
// returns task for worker
func (c *Coordinator) GiveTask(args *TaskArgs, reply *TaskReply) error {
	wId := args.ID

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
	c := Coordinator{
		mu:         sync.RWMutex{},
		workers:    make([]ActiveWorker, 0),
		reduceN:    nReduce,
		phase:      mapPhase,
		maxRetries: maxRetries,
	}

	mapTasks, mapTaskByID := makeMapTasks(files)
	reduceTasks, reduceTaskByID := makeReduceTasks(nReduce)
	retryTasks := make([]*TaskMeta, 0)

	c.mapQ.data = mapTasks
	c.reduceQ.data = reduceTasks
	c.retryQ.data = retryTasks

	c.mapTaskByID = mapTaskByID
	c.reduceTaskByID = reduceTaskByID

	c.server()
	return &c
}

func makeMapTasks(files []string) ([]*TaskMeta, map[int]*TaskMeta) {
	mapTasks := make([]*TaskMeta, len(files))
	TaskById := make(map[int]*TaskMeta)

	for i, v := range files {
		task := &TaskMeta{
			taskType: mapType,
			iD:       i,
			filename: v,
		}
		mapTasks = append(mapTasks, task)
		TaskById[i] = task
	}

	return mapTasks, TaskById
}

func makeReduceTasks(reduceN int) ([]*TaskMeta, map[int]*TaskMeta) {
	reduceTasks := make([]*TaskMeta, reduceN)
	TaskById := make(map[int]*TaskMeta)

	for i := 0; i < reduceN; i++ {
		task := &TaskMeta{
			taskType: reduceType,
			iD:       i,
		}
		reduceTasks = append(reduceTasks, task)
		TaskById[i] = task
	}

	return reduceTasks, TaskById
}

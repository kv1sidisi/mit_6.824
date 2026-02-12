package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
)

// GiveTask is rpc method
// returns task for worker
func (c *Coordinator) GiveTask(args *TaskArgs, reply *TaskReply) error {
	var task *TaskMeta
	var ok bool

	// get task
	switch c.phase {
	case mapPhase:
		if !c.retryQ.IsEmpty() {
			task, ok = c.mapQ.Dequeue()
		} else {
			task, ok = c.retryQ.Dequeue()
		}
	case reducePhase:
		if !c.retryQ.IsEmpty() {
			task, ok = c.reduceQ.Dequeue()
		} else {
			task, ok = c.retryQ.Dequeue()
		}
	case donePhase:
		reply.Type = Exit
		return nil
	}

	// write task data to reply
	if !ok {
		reply.Type = Wait
		return nil
	}

	// TODO: could be removed if we fight for task til end
	if task.retriedTimes == c.maxRetries {
		reply.Type = Wait
		return nil
	}

	if task.taskType == mapType {
		reply.Type = Map
		reply.MapTaskID = task.iD
		reply.Filename = task.filename
	} else {
		reply.Type = Reduce
		reply.ReduceTaskID = task.iD
	}

	reply.ReduceNum = c.reduceN

	return nil
}

func (c *Coordinator) ReceiveReport(args *ReportArgs, reply *ReportReply) error {
	id := args.TaskID
	taskType := args.Type
	status := args.Status

	var task *TaskMeta

	switch taskType {
	case Map:
		task = c.mapTaskByID[id]
	case Reduce:
		task = c.reduceTaskByID[id]
	}
	if status == failed {
		task.retriedTimes++
		c.retryQ.Enqueue(task)
	}
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

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mu:         sync.RWMutex{},
		workers:    make(map[int]*ActiveWorker),
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

	c.tasksNum = len(files) + nReduce

	c.server()
	return &c
}

func makeMapTasks(files []string) ([]*TaskMeta, map[int]*TaskMeta) {
	mapTasks := make([]*TaskMeta, len(files))
	TaskByID := make(map[int]*TaskMeta)

	for i, v := range files {
		task := &TaskMeta{
			taskType: mapType,
			iD:       i,
			filename: v,
		}
		mapTasks = append(mapTasks, task)
		TaskByID[i] = task
	}

	return mapTasks, TaskByID
}

func makeReduceTasks(reduceN int) ([]*TaskMeta, map[int]*TaskMeta) {
	reduceTasks := make([]*TaskMeta, reduceN)
	TaskByID := make(map[int]*TaskMeta)

	for i := range reduceN {
		task := &TaskMeta{
			taskType: reduceType,
			iD:       i,
		}
		reduceTasks = append(reduceTasks, task)
		TaskByID[i] = task
	}

	return reduceTasks, TaskByID
}

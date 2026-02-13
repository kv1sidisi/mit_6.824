package mr

import (
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/rpc"
	"sync"
)

type CPhase string

const (
	mapPhase      CPhase = "map"
	reducePhase   CPhase = "reduce"
	donePhase     CPhase = "done"
	startingPhase CPhase = "starting"
)

type CTask struct {
	taskType TaskType
	iD       int
	filename string
}

type Coordinator struct {
	log *slog.Logger
	mu  sync.Mutex

	workerIDToTaskID map[int]*CTask
	tIDtoTask        map[int]*CTask

	phase   CPhase
	nReduce int

	mapQ    *Queue
	reduceQ *Queue
	retryQ  *Queue

	mapTasks    int
	reduceTasks int
	tasksDone   int
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		log: NewLogger(debug),
		mu:  sync.Mutex{},

		workerIDToTaskID: make(map[int]*CTask),
		tIDtoTask:        make(map[int]*CTask),

		phase:   startingPhase,
		nReduce: nReduce,

		mapQ:    CreateQueue(),
		reduceQ: CreateQueue(),
		retryQ:  CreateQueue(),

		mapTasks:    len(files),
		reduceTasks: nReduce,
	}

	slog.SetDefault(c.log)

	slog.Debug("coordinator created")

	c.addTasks(files)

	slog.Debug("tasks created")

	c.server()

	c.phase = mapPhase

	slog.Debug("map phase started")
	slog.Info("coordinator started")

	return &c
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

// Done main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.phase == donePhase
}

func (c *Coordinator) addTasks(files []string) {
	id := 0

	// create reduce tasks
	for range c.nReduce {
		task := &CTask{
			iD:       id,
			taskType: ReduceType,
		}
		c.tIDtoTask[id] = task
		c.reduceQ.Enqueue(task)
		id++
	}

	// create map tasks
	for _, v := range files {
		task := &CTask{
			iD:       id,
			taskType: MapType,
			filename: v,
		}
		c.tIDtoTask[id] = task
		c.mapQ.Enqueue(task)
		id++
	}
}

// Hello is rpc method
// returns registered worker id
func (c *Coordinator) Hello(args *EmptyArgs, reply *HelloReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	nextID := len(c.workerIDToTaskID)
	c.workerIDToTaskID[nextID] = nil

	reply.ID = nextID

	slog.Debug("worker registered", slog.Int("task", nextID))
	return nil
}

// getNextTask gets next task from queue
func (c *Coordinator) getNextTask() *CTask {
	var task *CTask
	var ok bool

	if !c.retryQ.IsEmpty() {
		task, ok = c.retryQ.Dequeue()
	}
	if ok {
		return task
	}

	switch c.phase {
	case mapPhase:
		task, ok = c.mapQ.Dequeue()
	case reducePhase:
		task, ok = c.reduceQ.Dequeue()
	}

	if !ok {
		return nil
	}

	return task
}

// GiveTask is rpc method
// returns task for worker
func (c *Coordinator) GiveTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.phase == donePhase || c.phase == startingPhase {
		reply.WTask = Wait
		slog.Debug("no task given, coordinator is in starting or done phase")
		return nil
	}

	task := c.getNextTask()

	if task == nil {
		reply.WTask = Wait
		slog.Debug("no task avaliable in queue")
		return nil
	}

	if task.taskType == MapType {
		reply.TiD = task.iD
		reply.NReduce = c.nReduce
		reply.Filename = task.filename
		reply.WTask = Run
		reply.TType = MapType
		slog.Debug("map task given", slog.Int("task", task.iD))
		return nil
	}

	if task.taskType == MapType {
		reply.TiD = task.iD
		reply.NReduce = c.nReduce
		reply.WTask = Run
		reply.TType = ReduceType
		slog.Debug("reduce task given %d", slog.Int("task", task.iD))
		return nil
	}

	slog.Debug("error getting task type")

	return ErrorTaskType
}

// GetReport is rpc method
// gets report from worker
func (c *Coordinator) GetReport(args *ReportArgs, reply *ReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	id := args.WiD
	status := args.Status

	task := c.workerIDToTaskID[id]

	if status == WSuccess {
		switch task.taskType {
		case MapType:
			c.tasksDone++
		case ReduceType:
			c.tasksDone++
		}
		slog.Debug("task done %d", task.iD)
	} else {
		c.retryQ.Enqueue(task)
		slog.Debug("task %d failed, added to retryQ", task.iD)
	}

	if c.tasksDone == c.mapTasks {
		c.phase = reducePhase
		slog.Debug("coordinator switched to reduce phase")
		return nil
	}

	if c.tasksDone == c.reduceTasks {
		c.phase = donePhase
		slog.Debug("coordinator switched to done phase")
		return nil
	}

	return nil
}

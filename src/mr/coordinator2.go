package mr

import (
	"log/slog"
	"sync"
)

type CPhase string

const (
	mapPhase      CPhase = "map"
	reducePhase   CPhase = "reduce"
	donePhase     CPhase = "done"
	startingPhase CPhase = "starting"
)

type CTaskType string

const (
	mapTaskType    CTaskType = "map"
	reduceTaskType CTaskType = "reduce"
)

const maxRetries = 5

type CTask struct {
	taskType CTaskType
	iD       int
	filename string
	retried  int
}

type Coordinator struct {
	log *slog.Logger
	mu  sync.Mutex

	workerIDToTaskID map[int]*CTask
	tIDtoTask        map[int]*CTask

	phase   CPhase
	nReduce int

	mapQ       *Queue
	reduceQ    *Queue
	retryQ     *Queue
	maxRetries int

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

		mapQ:       CreateQueue(),
		reduceQ:    CreateQueue(),
		retryQ:     CreateQueue(),
		maxRetries: maxRetries,

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

func (c *Coordinator) addTasks(files []string) {
	id := 0

	// create reduce tasks
	for range c.nReduce {
		task := &CTask{
			iD:       id,
			taskType: reduceTaskType,
		}
		c.tIDtoTask[id] = task
		c.reduceQ.Enqueue(task)
		id++
	}

	// create map tasks
	for _, v := range files {
		task := &CTask{
			iD:       id,
			taskType: mapTaskType,
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

	slog.Debug("worker %d registered", nextID)
	return nil
}

// GiveTask is rpc method
// returns task for worker
func (c *Coordinator) GiveTask(args *GetTaskArgs, reply *GetTaskReply) error {
	if c.phase == donePhase {
		reply.WTask = Wait
		return nil
	}

	// свитч на фазу
	// фаза мап - проверить ретарай очередь и отдать если пусто то ждать
	// фаза редус = проерить ретрай очередь и отдать если пусто то ждать

	// ошиюка если ниче не сработало из свичей

}

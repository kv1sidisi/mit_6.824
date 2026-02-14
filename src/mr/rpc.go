package mr

import "errors"

//
// RPC definitions.
//
// remember to capitalize all names.
//
//
// example to show how to declare the arguments
// and reply for an RPC.
//
//
// type ExampleArgs struct {
// 	X int
// }
//
// type ExampleReply struct {
// 	Y int
// }

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
// func coordinatorSock() string {
// 	s := "/var/tmp/5840-mr-"
// 	s += strconv.Itoa(os.Getuid())
// 	return s
// }

type EmptyArgs struct{}

type HelloReply struct {
	ID int
}

type WorkerTask string

const (
	Run  WorkerTask = "done"
	Wait WorkerTask = "wait"
	Exit WorkerTask = "exit"
)

type TaskType string

const (
	MapType    TaskType = "map"
	ReduceType TaskType = "reduce"
)

type GetTaskArgs struct {
	WiD int
}

type GetTaskReply struct {
	TiD      int
	WTask    WorkerTask
	TType    TaskType
	Filename string
	NReduce  int
}

var ErrorTaskType error = errors.New("error getting task type")

type WorkerReport string

const (
	WSuccess WorkerReport = "workerSuccess"
	WFailed  WorkerReport = "workerFailed"
)

type ReportArgs struct {
	WiD    int
	Status WorkerReport
}

type ReportReply struct {
}

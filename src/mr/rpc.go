package mr

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
	Map    WorkerTask = "map"
	Reduce WorkerTask = "reduce"
	Wait   WorkerTask = "wait"
)

type GetTaskArgs struct {
	WiD int
}

type GetTaskReply struct {
	TiD      int
	WTask    WorkerTask
	Filename string
	NReduce  int
}

type TaskStatus string

const (
	done   TaskStatus = "done"
	failed TaskStatus = "failer"
)

type ReportArgs struct {
	WiD    int
	Status TaskStatus
}

type ReportReply struct {
}

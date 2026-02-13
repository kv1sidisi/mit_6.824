package mr

import (
	"hash/fnv"
	"log/slog"
	"os"
)

type Queue struct {
	data []*CTask
}

func CreateQueue() *Queue {
	return &Queue{
		data: make([]*CTask, 0),
	}
}

func (q *Queue) Enqueue(v *CTask) {
	q.data = append(q.data, v)
}

func (q *Queue) Dequeue() (*CTask, bool) {
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

type logLevel string

const (
	debug logLevel = "debug"
	info  logLevel = "info"
)

func NewLogger(level logLevel) *slog.Logger {
	var opts slog.HandlerOptions
	switch level {
	case debug:
		opts.Level = slog.LevelDebug
	case info:
		opts.Level = slog.LevelInfo
	}

	handler := slog.NewTextHandler(os.Stdout, &opts)

	return slog.New(handler)
}

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

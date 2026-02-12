package mr

import (
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

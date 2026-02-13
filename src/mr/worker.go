package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"log/slog"
	"net/rpc"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type WTask struct {
	iD       int
	taskType TaskType
	filename string
	nReduce  int
}

type IAmWorker struct {
	iD int
	c  *rpc.Client

	task WorkerTask

	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
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
func (w *IAmWorker) sayHello() error {
	args := EmptyArgs{}

	reply := HelloReply{}

	err := w.call("Coordinator.Hello", &args, &reply)
	if err != nil {
		return err
	}
	w.iD = reply.ID
	return nil
}

// StartWorking gets task from coordinator and manages it
func (w *IAmWorker) startWorking() error {
	const maxRPCFails = 7
	fails := 0

	for {
		task, err := w.requestTask()
		if err != nil {
			fails++
			if fails >= maxRPCFails {
				return err
			}
			time.Sleep(time.Second)
			continue
		}

		if w.task == Wait {
			time.Sleep(time.Second)
			continue
		}

		switch task.taskType {
		case MapType:
			err = w.doMap(task.iD, task.filename, task.nReduce)
		case ReduceType:
			err = w.doReduce(task.iD)
		}

		if err != nil {
			_ = w.reportTask(WFailed)
			slog.Debug("worker failed",
				slog.Int("id", w.iD),
				slog.Any("err", err),
			)
		}
		err = w.reportTask(WSuccess)
		if err != nil {
			slog.Debug("worker failed",
				slog.Int("id", w.iD),
				slog.Any("err", err),
			)
		}
	}
}

var endNumRe = regexp.MustCompile(`\d+$`)

func endNumber(filename string) (int, bool) {
	base := filepath.Base(filename)
	name := strings.TrimSuffix(base, filepath.Ext(base))
	s := endNumRe.FindString(name)
	if s == "" {
		return 0, false
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, false
	}
	return n, true
}

func (w *IAmWorker) doReduce(reduceTaskID int) error {
	values := make([]KeyValue, 0)

	root := "."
	// go through all files in root
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		// check for temp file type
		if err != nil || d.IsDir() {
			return nil
		}
		n, ok := endNumber(d.Name())
		if !ok || n != reduceTaskID {
			return nil
		}

		// Read temp file from map task
		// TODO: возможно проблема что при ошибке файлы будут оставаться и при переделке задачи будут проблемы
		f, err := os.Open(d.Name())
		if err != nil {
			return err
		}
		defer f.Close()

		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			err = dec.Decode(&kv)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			values = append(values, kv)
		}

		outFName := fmt.Sprintf("mr-out-%d", reduceTaskID)
		ofile, err := os.Create(outFName)
		if err != nil {
			return err
		}

		sort.Sort(ByKey(values))

		// call Reduce on each distinct key in values[],
		// and print the result to mr-out-0.
		i := 0
		for i < len(values) {
			j := i + 1

			for j < len(values) && values[j].Key == values[i].Key {
				j++
			}
			final := []string{}
			for k := i; k < j; k++ {
				final = append(final, values[k].Value)
			}
			output := w.reducef(values[i].Key, final)

			fmt.Fprintf(ofile, "%v %v\n", values[i].Key, output)

			i = j
		}

		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (w *IAmWorker) doMap(taskID int, filename string, reduceNum int) error {
	// read file
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	content, err := io.ReadAll(file)
	if err != nil {
		return err
	}
	file.Close()
	if w == nil || w.mapf == nil {
		return errors.New("worker/mapf is nil")
	}
	kva := w.mapf(filename, string(content))

	// split on map temp files
	files := make([]*os.File, reduceNum)
	encoders := make([]*json.Encoder, reduceNum)

	for _, kv := range kva {
		n := ihash(kv.Key) % reduceNum

		tempName := fmt.Sprintf("out-%d-%d", taskID, n)
		if files[n] == nil {
			f, err := os.Create(tempName)
			if err != nil {
				return err
			}
			files[n] = f
			if encoders[n] == nil {
				encoders[n] = json.NewEncoder(f)
			}
			defer f.Close()
		}

		err = encoders[n].Encode(&kv)
		if err != nil {
			return err
		}
	}
	return nil
}

// requestTask asks for task from coordinator
func (w *IAmWorker) requestTask() (WTask, error) {
	args := GetTaskArgs{
		WiD: w.iD,
	}
	reply := GetTaskReply{}

	err := w.call("Coordinator.GiveTask", &args, &reply)
	if err != nil {
		return WTask{}, err
	}

	w.task = reply.WTask

	return WTask{
		iD:       reply.TiD,
		taskType: reply.TType,
		filename: reply.Filename,
		nReduce:  reply.NReduce,
	}, nil
}

// reportTask reports to coordinator final status of task
// if fails worker will wait for next task and coordinator will set failed in 10 seconds timeout
func (w *IAmWorker) reportTask(status WorkerReport) error {
	args := ReportArgs{
		WiD:    w.iD,
		Status: status,
	}

	reply := ReportReply{}

	err := w.call("Coordinator.ReceiveReport", &args, &reply)
	if err != nil {
		return err
	}

	return nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	worker := IAmWorker{}
	worker.c = makeClient("127.0.0.1", "1234")

	err := worker.sayHello()
	if err != nil {
		log.Fatal("couldn't get id:", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- worker.startWorking()
		close(errCh)
	}()

	if err := <-errCh; err != nil {
		log.Fatal("coordinator dead")
	}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

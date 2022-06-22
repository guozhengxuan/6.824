package mr

import (
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	files   []string
	nReduce int

	tasks  chan Task
	done   map[Task]chan struct{}
	issued map[Task]chan struct{}
}

type Task struct {
	fn string
	id int
}

func (c *Coordinator) Init(files []string, nReduce int) {
	c.files = files
	c.nReduce = nReduce

	c.tasks = make(chan Task, len(files))
	c.done = make(map[Task]chan struct{})
	c.issued = make(map[Task]chan struct{})
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Schedule() {
	// Load map tasks
	for i, fn := range c.files {
		task := Task{
			fn: fn,
			id: i,
		}
		c.tasks <- task
		c.done[task] = make(chan struct{})
		c.issued[task] = make(chan struct{})
	}

	// Wait for map tasks to be done
	for task := range c.done {
		task := task
		go func() {
		dispatch:
			<-c.issued[task]
			select {
			case <-c.done[task]:
				close(c.done[task])
			case <-time.After(10 * time.Second):
				c.tasks <- task
				goto dispatch
			}
		}()
	}

	// Set up for reduce tasks
}

func (c *Coordinator) Dispatch(args *EmptyArgs, reply *DispatchReply) error {
	defer func() {
		if err := recover(); err != nil {
			log.Println("All map tasks have been issued")
		}
	}()

	task := <-c.tasks
	reply = &DispatchReply{
		task:    task,
		nReduce: c.nReduce,
	}
	c.issued[task] <- struct{}{}
	return nil
}

func (c *Coordinator) MapDone(args *MapDoneArgs, reply *EmptyReply) error {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("Map task of file %v already done", args)
		}
	}()

	os.Rename(args.tempFn)
	c.done[args.task] <- struct{}{}

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server()
	return &c
}

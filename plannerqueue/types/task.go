package types

import (
	"sync"
)

const (
	MaxTasksBatchCap = 10
)

var (
	taskPool = sync.Pool{
		New: func() interface{} { return new(Task) },
	}

	tasksBatchPool = sync.Pool{
		New: func() interface{} { return new([]*Task) },
	}
)

type Task struct {
	NextRunTimestamp int64

	// Id of Task in disk storage
	ObjectID string
}

func (t *Task) Release() {
	t.NextRunTimestamp = 0
	t.ObjectID = ""

	taskPool.Put(t)
}

func NewTask(nextRunTimestamp int64, objectID string) (task *Task) {
	iTask := taskPool.Get()

	if iTask != nil {
		task := iTask.(*Task)

		task.NextRunTimestamp = nextRunTimestamp
		task.ObjectID = objectID

		return task
	}

	return nil
}

func NewTasksBatch() []*Task {
	iTasksBatch := tasksBatchPool.Get()

	if iTasksBatch != nil {
		tasksBatch := iTasksBatch.([]*Task)

		return tasksBatch
	}

	return nil
}

func ReleaseTasksBatch(tasksBatch []*Task) {
	if cap(tasksBatch) <= MaxTasksBatchCap {
		tasksBatch = tasksBatch[:0]

		tasksBatchPool.Put(tasksBatch)
	}
}

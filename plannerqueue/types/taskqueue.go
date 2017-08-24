package types

import (
	"github.com/emirpasic/gods/trees/binaryheap"
	"github.com/emirpasic/gods/utils"
)

var (
	taskqueue *TaskQueue
)

type TaskQueue struct {
	Heap *binaryheap.Heap
}

func (tq *TaskQueue) Push(tasks ...*Task) {
	tq.Heap.Push(tasks...)
}

func (tq *TaskQueue) Peek() *Task {
	// Get the oldest task from task queue (type interface{})
	iTask, ok := tq.Heap.Peek()

	if !ok || iTask == nil {
		return nil
	}

	task := iTask.(*Task)

	return task
}

// Extract tasks from queue with next_run_timestamp parameter less than current timestamp
func (tq *TaskQueue) GetTasksBefore(timestamp int64) []*Task {
	tasksBatch := NewTasksBatch()

	for {
		oldestTask := tq.Peek()

		if oldestTask == nil || timestamp < oldestTask.NextRunTimestamp {
			break
		}

		tasksBatch = append(tasksBatch, oldestTask)
	}

	return tasksBatch
}

// Initialize task queue (global variable taskqueue)
func InitializeTaskQueue() {
	if taskqueue == nil {
		taskComparator := func(first, second interface{}) int {
			firstTask := first.(*Task)
			secondTask := second.(*Task)

			result := utils.Int64Comparator(firstTask.NextRunTimestamp, secondTask.NextRunTimestamp)

			return result
		}

		heap := binaryheap.NewWith(taskComparator)

		taskQueue := new(TaskQueue)

		taskQueue.Heap = heap

		taskqueue = taskQueue
	}
}

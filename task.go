package appmain

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
)

// Task represents a task in the App.
type Task func(ctx context.Context) error

// TaskType represents the type of a task.
type TaskType int

const (
	// TaskTypeInit indicates an init task.
	TaskTypeInit TaskType = iota + 1
	// TaskTypeMain indicates a main task.
	TaskTypeMain
	// TaskTypeCleanup indicates a cleanup task.
	TaskTypeCleanup
)

// TaskContext provides information about a task.
type TaskContext interface {
	// Name returns the name passed to the Add*Task method.
	Name() string
	// Type returns the type of the task.
	Type() TaskType
	// Done returns a channel that is closed when the task completes or is skipped.
	Done() <-chan struct{}
	// Err returns the error returned by the task. It is valid only after Done is closed.
	// If the task panics, it returns *PanicError.
	// If the task is skipped, it returns ErrSkipped.
	Err() error
}

type task struct {
	name   string
	ttype  TaskType
	task   Task
	done   chan struct{}
	err    error
	config *taskConfig
}

func newTask(name string, tt TaskType, t Task, opts []TaskOption) *task {
	config := newTaskConfig(opts)
	switch tt {
	case TaskTypeInit:
		for _, at := range config.after {
			if at.Type() != TaskTypeInit {
				panic(name + ": init task can only run after init tasks: " + at.Name())
			}
		}
	case TaskTypeMain:
		for _, at := range config.after {
			switch at.Type() {
			case TaskTypeInit:
				panic(name + ": main task always runs after init tasks: " + at.Name())
			case TaskTypeCleanup:
				panic(name + ": main task cannot run after cleanup task: " + at.Name())
			}
		}
	}

	return &task{
		name:   name,
		ttype:  tt,
		task:   t,
		done:   make(chan struct{}),
		err:    nil,
		config: config,
	}
}

func (t *task) Name() string {
	return t.name
}

func (t *task) Type() TaskType {
	return t.ttype
}

func (t *task) Done() <-chan struct{} {
	return t.done
}

func (t *task) Err() error {
	return t.err
}

func (t *task) skip() {
	t.err = ErrSkipped
	close(t.done)
}

func (t *task) run(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			t.err = &PanicError{Value: r, Stack: debug.Stack()}
		}
		close(t.done)
	}()

	for _, at := range t.config.after {
		<-at.Done()
	}

	if t.config.interceptor != nil {
		t.err = t.config.interceptor(ctx, t, t.task)
	} else {
		t.err = t.task(ctx)
	}
}

// ErrSkipped is returned by TaskContext.Err when the main task did not run
// because init tasks failed or a signal was received during init tasks.
var ErrSkipped = errors.New("skipped")

// PanicError is returned by TaskContext.Err when the task panics.
type PanicError struct {
	// Value is the value passed to panic.
	Value interface{}
	// Stack is the stack trace of the goroutine that panicked.
	Stack []byte
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("panic: %v", e.Value)
}

// Unwrap returns Value if it is an error, so that errors.Is and errors.As
// can inspect the error passed to panic.
func (e *PanicError) Unwrap() error {
	err, _ := e.Value.(error)
	return err
}

package appmain

import (
	"context"
)

// TaskOption is an option for the Add*Task methods of App.
// The available options are:
//   - RunAfter
//   - Interceptor
//   - ChainInterceptors
type TaskOption interface {
	applyTask(c *taskConfig)
}

type taskOptionFunc func(c *taskConfig)

func (f taskOptionFunc) applyTask(c *taskConfig) {
	f(c)
}

type taskConfig struct {
	after       []TaskContext
	interceptor Interceptor
}

func newTaskConfig(opts []TaskOption) *taskConfig {
	c := new(taskConfig)
	for _, o := range opts {
		o.applyTask(c)
	}
	return c
}

// RunAfter specifies the tasks that must complete before the task starts.
// The task runs even if those tasks return an error.
func RunAfter(tcs ...TaskContext) TaskOption {
	return taskOptionFunc(func(c *taskConfig) {
		c.after = append(c.after, tcs...)
	})
}

// Interceptor wraps the execution of a task.
// The given Task runs the actual task, so the interceptor decides when (or whether) to call it.
type Interceptor func(context.Context, TaskContext, Task) error

func (i Interceptor) applyTask(c *taskConfig) {
	if c.interceptor != nil {
		c.interceptor = ChainInterceptors(append([]Interceptor{c.interceptor}, i)...)
	} else {
		c.interceptor = i
	}
}

// ChainInterceptors combines the given interceptors into one Interceptor.
// They are executed in the given order: the first one is the outermost.
func ChainInterceptors(is ...Interceptor) Interceptor {
	switch len(is) {
	case 0:
		panic("ChainInterceptors requires at least one interceptor")
	case 1:
		return is[0]
	default:
		head := is[0]
		tail := ChainInterceptors(is[1:]...)
		return func(ctx1 context.Context, tc TaskContext, t Task) error {
			return head(ctx1, tc, func(ctx2 context.Context) error {
				return tail(ctx2, tc, t)
			})
		}
	}
}

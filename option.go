package appmain

import (
	"fmt"
	"os"
	"syscall"
	"time"
)

// Option is an option for the New function.
// The available options are:
//   - ErrorStrategy
//   - DefaultTaskOptions
//   - NotifySignal
//   - CleanupTimeout
type Option interface {
	apply(c *config)
}

type optionFunc func(c *config)

func (f optionFunc) apply(c *config) {
	f(c)
}

type config struct {
	signals            []os.Signal
	errorStrategy      ErrorStrategy
	defaultTaskOptions []TaskOption
	cleanupTimeout     time.Duration
}

func newConfig(opts []Option) *config {
	c := &config{
		signals:       []os.Signal{os.Interrupt, syscall.SIGTERM},
		errorStrategy: DefaultErrorStrategy,
	}

	for _, o := range opts {
		o.apply(c)
	}

	return c
}

// Decision is the result of ErrorStrategy that decides whether to
// cancel the running tasks. An unknown value is treated as Exit.
type Decision int

const (
	// Continue keeps the running tasks running.
	Continue Decision = iota
	// Shutdown cancels the running tasks and exits with a success status.
	Shutdown
	// Exit cancels the running tasks and exits with an error status.
	Exit
)

func (d Decision) String() string {
	switch d {
	case Continue:
		return "Continue"
	case Shutdown:
		return "Shutdown"
	case Exit:
		return "Exit"
	default:
		return fmt.Sprintf("Decision(%d)", int(d))
	}
}

func (d Decision) statusCode() int {
	switch d {
	case Continue:
		return 0
	case Shutdown:
		return 0
	default:
		return 1
	}
}

// ErrorStrategy is an option for the New function that decides how the App
// behaves when a task returns an error. It is called only when a task
// returns an error, which is available from TaskContext.Err().
//
// It is not called when a task returns the error of its context.Context after
// the App cancels it, for example by a signal, since the task stopped as requested.
//
// It may be called concurrently, because cleanup tasks start while the tasks
// canceled by a signal are still stopping.
type ErrorStrategy func(TaskContext) Decision

func (s ErrorStrategy) apply(c *config) {
	c.errorStrategy = s
}

// DefaultErrorStrategy is the default strategy of the App.
func DefaultErrorStrategy(tc TaskContext) Decision {
	switch tc.Type() {
	case TaskTypeCleanup:
		return Continue
	case TaskTypeInit, TaskTypeMain:
		return Exit
	default:
		panic("unknown task type")
	}
}

// DefaultTaskOptions is an option for the New function that applies the
// given task options to every task in the App.
func DefaultTaskOptions(opts ...TaskOption) Option {
	return optionFunc(func(c *config) {
		c.defaultTaskOptions = append(c.defaultTaskOptions, opts...)
	})
}

// NotifySignal is an option for the New function that overrides the
// signals the App handles to start cleanup tasks.
// By default, the App handles os.Interrupt (SIGINT) and syscall.SIGTERM.
// Calling NotifySignal with no arguments disables signal handling.
func NotifySignal(sigs ...os.Signal) Option {
	return optionFunc(func(c *config) {
		c.signals = append([]os.Signal(nil), sigs...)
	})
}

// CleanupTimeout is an option for the New function that limits the time to
// run cleanup tasks, including the time to wait for the tasks canceled by a
// signal to stop. When the timeout elapses, the context.Context of cleanup
// tasks is canceled and the App exits with status 1 without waiting for them.
// Zero or a negative value means no timeout, which is the default.
func CleanupTimeout(d time.Duration) Option {
	return optionFunc(func(c *config) {
		c.cleanupTimeout = d
	})
}

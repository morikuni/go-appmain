package appmain

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
)

// App represents an application.
type App struct {
	tasks   map[TaskType][]*task
	config  *config
	sigChan chan os.Signal
	sigSet  map[os.Signal]struct{}
	done    chan struct{}
	ran     int32
}

// New creates a new App with the given options.
// The available options are:
//   - ErrorStrategy
//   - DefaultTaskOptions
//   - NotifySignal
//   - CleanupTimeout
func New(opts ...Option) *App {
	c := newConfig(opts)

	sigSet := make(map[os.Signal]struct{}, len(c.signals))
	for _, s := range c.signals {
		sigSet[s] = struct{}{}
	}

	return &App{
		tasks:   make(map[TaskType][]*task),
		config:  c,
		sigChan: make(chan os.Signal, 1),
		sigSet:  sigSet,
		done:    make(chan struct{}),
	}
}

// AddInitTask adds a named initialization task to the App.
//
// Init tasks run before main tasks; main tasks do not start until all init
// tasks have completed.
//
// By default, if any init task returns an error, the App cancels the
// context.Context of all init tasks, skips the main tasks and starts the
// cleanup tasks. To change this behavior, pass the ErrorStrategy option to New.
func (app *App) AddInitTask(name string, t Task, opts ...TaskOption) TaskContext {
	return app.addTask(name, TaskTypeInit, t, opts)
}

// AddMainTask adds a named main task to the App.
//
// Main tasks start after all init tasks have completed.
//
// By default, if any main task returns an error, the App cancels the
// context.Context of all main tasks and starts the cleanup tasks.
// To change this behavior, pass the ErrorStrategy option to New.
func (app *App) AddMainTask(name string, t Task, opts ...TaskOption) TaskContext {
	return app.addTask(name, TaskTypeMain, t, opts)
}

// AddCleanupTask adds a named cleanup task to the App.
//
// Cleanup tasks start when all main tasks have completed, when init or main
// tasks fail, or when the first signal is received. If a signal is received
// while cleanup tasks are running, their context.Context is canceled. On the
// second signal, the App exits without waiting for the cleanup tasks to complete.
//
// Unlike init and main tasks, by default the context.Context of cleanup tasks
// is not canceled even if some of them return an error.
func (app *App) AddCleanupTask(name string, t Task, opts ...TaskOption) TaskContext {
	return app.addTask(name, TaskTypeCleanup, t, opts)
}

func (app *App) addTask(name string, tt TaskType, t Task, opts []TaskOption) TaskContext {
	r := newTask(name, tt, t, append(app.config.defaultTaskOptions, opts...))
	app.tasks[tt] = append(app.tasks[tt], r)
	return r
}

// SendSignal emulates sending a signal to the App.
// Since the App handles signals by default, typical apps do not need this method.
// It is useful for testing.
//
// Signals not specified by NotifySignal are ignored, and so are signals sent
// after Run returns.
func (app *App) SendSignal(sig os.Signal) {
	if _, ok := app.sigSet[sig]; !ok {
		return
	}
	select {
	case app.sigChan <- sig:
	case <-app.done:
	}
}

// Run runs the App and returns an exit code for the main function.
// It is typically used as follows:
//
//	os.Exit(app.Run())
//
// The App handles signals only while Run is running.
// Run must be called only once, and RunContext counts as Run. It panics if
// called more than once.
func (app *App) Run() int {
	return app.RunContext(context.Background())
}

// RunContext is like Run, but it also starts the shutdown when ctx is done,
// as if the first signal is received.
//
// The context.Context of each task inherits the values of ctx. The
// context.Context of cleanup tasks is not canceled by ctx, so that cleanup
// tasks can run after ctx is done.
func (app *App) RunContext(ctx context.Context) int {
	if !atomic.CompareAndSwapInt32(&app.ran, 0, 1) {
		panic("appmain: Run must be called only once")
	}
	defer close(app.done)

	// signal.Notify with no signals relays every signal, including SIGURG used by
	// the Go runtime for preemption, so it must not be called with an empty list.
	if len(app.config.signals) > 0 {
		signal.Notify(app.sigChan, app.config.signals...)
		defer signal.Stop(app.sigChan)
	}

	code, interrupted := app.runInitAndMain(ctx)
	return app.runCleanup(ctx, code, interrupted)
}

// runInitAndMain runs init tasks and then main tasks.
// If a signal is received or ctx is done, it cancels the running tasks and
// returns their result channel without waiting, so that cleanup tasks can
// start immediately.
func (app *App) runInitAndMain(ctx context.Context) (code int, interrupted <-chan Decision) {
	initCtx, cancelInit := context.WithCancel(ctx)
	defer cancelInit()
	initResult := app.runTasks(initCtx, TaskTypeInit)

	select {
	case d := <-initResult:
		if d != Continue {
			app.skipMain()
			return d.statusCode(), nil
		}
	case <-app.sigChan:
		app.skipMain()
		return 0, initResult
	case <-ctx.Done():
		app.skipMain()
		return 0, initResult
	}

	mainCtx, cancelMain := context.WithCancel(ctx)
	defer cancelMain()
	mainResult := app.runTasks(mainCtx, TaskTypeMain)

	select {
	case d := <-mainResult:
		return d.statusCode(), nil
	case <-app.sigChan:
		return 0, mainResult
	case <-ctx.Done():
		return 0, mainResult
	}
}

// runCleanup runs cleanup tasks.
// The first signal cancels the cleanup tasks and the second signal makes it
// return immediately. A signal received during init or main tasks counts as
// the first one. ctx being done also counts as a signal, but only as the first one.
func (app *App) runCleanup(ctx context.Context, code int, interrupted <-chan Decision) int {
	base := context.Context(valueOnlyContext{ctx})
	var timeout <-chan struct{}
	if d := app.config.cleanupTimeout; d > 0 {
		var cancelTimeout context.CancelFunc
		base, cancelTimeout = context.WithTimeout(base, d)
		defer cancelTimeout()
		timeout = base.Done()
	}
	timedOutCode := func() int {
		if code == 0 {
			return 1
		}
		return code
	}

	cleanupCtx, cancelCleanup := context.WithCancel(base)
	defer cancelCleanup()
	cleanupResult := app.runTasks(cleanupCtx, TaskTypeCleanup)

	signaled := interrupted != nil
	ctxDone := ctx.Done()
	if signaled {
		ctxDone = nil
		select {
		case d := <-interrupted:
			code = d.statusCode()
		case sig := <-app.sigChan:
			return signalCode(sig)
		case <-timeout:
			return timedOutCode()
		}
	}

	for {
		select {
		case d := <-cleanupResult:
			// The cleanup tasks may have stopped because of the timeout.
			if base.Err() != nil {
				return timedOutCode()
			}
			if code == 0 {
				code = d.statusCode()
			}
			return code
		case sig := <-app.sigChan:
			if signaled {
				return signalCode(sig)
			}
			signaled = true
			cancelCleanup()
		case <-ctxDone:
			// ctx.Done is never reset, so stop receiving from it.
			ctxDone = nil
			if !signaled {
				signaled = true
				cancelCleanup()
			}
		case <-timeout:
			return timedOutCode()
		}
	}
}

// valueOnlyContext keeps the values of the parent but is never canceled.
// It is the same as context.WithoutCancel, which requires Go 1.21.
type valueOnlyContext struct {
	context.Context
}

func (valueOnlyContext) Deadline() (time.Time, bool) { return time.Time{}, false }

func (valueOnlyContext) Done() <-chan struct{} { return nil }

func (valueOnlyContext) Err() error { return nil }

func signalCode(sig os.Signal) int {
	s, ok := sig.(syscall.Signal)
	if ok {
		// By convention, a process terminated by a signal exits with 128 + <signal number>.
		// https://tldp.org/LDP/abs/html/exitcodes.html
		return int(s) + 128
	}
	return 1
}

func (app *App) skipMain() {
	for _, t := range app.tasks[TaskTypeMain] {
		t.skip()
	}
}

func (app *App) runTasks(ctx context.Context, tt TaskType) <-chan Decision {
	tasks := app.tasks[tt]
	ctx, cancel := context.WithCancel(ctx)

	doneTCs := make(chan TaskContext, len(tasks))
	for _, t := range tasks {
		t := t
		go func() {
			t.run(ctx)
			doneTCs <- t
		}()
	}

	result := make(chan Decision, 1)
	go func() {
		defer cancel()

		decision := Continue
		for range tasks {
			tc := <-doneTCs
			if err := tc.Err(); err == nil || isCanceledByApp(ctx, err) {
				continue
			}
			d := app.config.errorStrategy(tc)
			if d != Continue && decision == Continue {
				cancel()
				decision = d
			}
		}

		result <- decision
	}()

	return result
}

// isCanceledByApp reports whether err is the result of the App canceling ctx,
// as opposed to an error that the task returned on its own.
func isCanceledByApp(ctx context.Context, err error) bool {
	ctxErr := ctx.Err()
	return ctxErr != nil && errors.Is(err, ctxErr)
}

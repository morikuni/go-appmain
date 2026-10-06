package appmain

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// App represents an application.
type App struct {
	tasks   map[TaskType][]*task
	config  *config
	sigChan chan os.Signal
	sigSet  map[os.Signal]struct{}
	done    chan struct{}
}

// New creates a new App with the given options.
// The available options are:
//   - ErrorStrategy
//   - DefaultTaskOptions
//   - NotifySignal
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
// Run must be called only once.
func (app *App) Run() int {
	defer close(app.done)

	// signal.Notify with no signals relays every signal, including SIGURG used by
	// the Go runtime for preemption, so it must not be called with an empty list.
	if len(app.config.signals) > 0 {
		signal.Notify(app.sigChan, app.config.signals...)
		defer signal.Stop(app.sigChan)
	}

	ctx := context.Background()
	code, interrupted := app.runInitAndMain(ctx)
	return app.runCleanup(ctx, code, interrupted)
}

// runInitAndMain runs init tasks and then main tasks.
// If a signal is received, it cancels the running tasks and returns their
// result channel without waiting, so that cleanup tasks can start immediately.
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
	}

	mainCtx, cancelMain := context.WithCancel(ctx)
	defer cancelMain()
	mainResult := app.runTasks(mainCtx, TaskTypeMain)

	select {
	case d := <-mainResult:
		return d.statusCode(), nil
	case <-app.sigChan:
		return 0, mainResult
	}
}

// runCleanup runs cleanup tasks.
// The first signal cancels the cleanup tasks and the second signal makes it
// return immediately. A signal received during init or main tasks counts as
// the first one.
func (app *App) runCleanup(ctx context.Context, code int, interrupted <-chan Decision) int {
	cleanupCtx, cancelCleanup := context.WithCancel(ctx)
	defer cancelCleanup()
	cleanupResult := app.runTasks(cleanupCtx, TaskTypeCleanup)

	signaled := interrupted != nil
	if signaled {
		select {
		case d := <-interrupted:
			code = d.statusCode()
		case sig := <-app.sigChan:
			return signalCode(sig)
		}
	}

	for {
		select {
		case d := <-cleanupResult:
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
		}
	}
}

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
			if tc.Err() == nil {
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

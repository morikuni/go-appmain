package appmain

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func equal(tb testing.TB, a, b interface{}) {
	tb.Helper()

	if !reflect.DeepEqual(a, b) {
		tb.Fatalf("%v != %v", a, b)
	}
}

func TestApp(t *testing.T) {
	for name, tt := range map[string]struct {
		signalAt   TaskType // zero means no signal
		signals    int
		wantCode   int
		wantResult ResultSet
	}{
		"success": {
			0,
			0,
			0,
			ResultSet{
				NumInit:        2,
				SuccessInit:    2,
				NumMain:        2,
				SuccessMain:    2,
				NumCleanup:     2,
				SuccessCleanup: 2,
			},
		},
		"cancel init": {
			TaskTypeInit,
			1,
			0,
			ResultSet{
				NumInit:        2,
				SuccessInit:    1,
				NumMain:        0,
				SuccessMain:    0,
				NumCleanup:     2,
				SuccessCleanup: 2,
			},
		},
		"cancel main": {
			TaskTypeMain,
			1,
			0,
			ResultSet{
				NumInit:        2,
				SuccessInit:    2,
				NumMain:        2,
				SuccessMain:    1,
				NumCleanup:     2,
				SuccessCleanup: 2,
			},
		},
		"cancel cleanup": {
			TaskTypeCleanup,
			1,
			0,
			ResultSet{
				NumInit:        2,
				SuccessInit:    2,
				NumMain:        2,
				SuccessMain:    2,
				NumCleanup:     2,
				SuccessCleanup: 1,
			},
		},
		"cancel twice init": {
			TaskTypeInit,
			2,
			128 + int(syscall.SIGINT),
			ResultSet{
				NumInit:        2,
				SuccessInit:    1,
				NumMain:        0,
				SuccessMain:    0,
				NumCleanup:     2,
				SuccessCleanup: 1,
			},
		},
		"cancel twice main": {
			TaskTypeMain,
			2,
			128 + int(syscall.SIGINT),
			ResultSet{
				NumInit:        2,
				SuccessInit:    2,
				NumMain:        2,
				SuccessMain:    1,
				NumCleanup:     2,
				SuccessCleanup: 1,
			},
		},
		"cancel twice cleanup": {
			TaskTypeCleanup,
			2,
			128 + int(syscall.SIGINT),
			ResultSet{
				NumInit:        2,
				SuccessInit:    2,
				NumMain:        2,
				SuccessMain:    2,
				NumCleanup:     2,
				SuccessCleanup: 1,
			},
		},
	} {
		tt := tt
		t.Run(name, func(t *testing.T) {
			code, rs := runApp(t, tt.signalAt, tt.signals)
			equal(t, code, tt.wantCode)
			equal(t, rs, tt.wantResult)
		})
	}
}

type ResultSet struct {
	NumInit        int32
	SuccessInit    int32
	NumMain        int32
	SuccessMain    int32
	NumCleanup     int32
	SuccessCleanup int32
}

// runApp runs an App that has a quick task and a slow task in each phase.
// The slow task in the phase signalAt blocks until it is canceled, or until the
// test ends if signals is 2. When signals is 2, the slow cleanup task also
// blocks until the test ends.
func runApp(t *testing.T, signalAt TaskType, signals int) (int, ResultSet) {
	release := make(chan struct{})
	defer close(release)

	app := New()

	type phase struct {
		num, success int32
		quickDone    chan struct{}
		slowStarted  chan struct{}
	}
	phases := map[TaskType]*phase{}
	for _, tt := range []TaskType{TaskTypeInit, TaskTypeMain, TaskTypeCleanup} {
		p := &phase{quickDone: make(chan struct{}), slowStarted: make(chan struct{})}
		phases[tt] = p

		blocks := tt == signalAt || (signals == 2 && tt == TaskTypeCleanup)
		ignoreCtx := signals == 2

		quick := func(ctx context.Context) error {
			atomic.AddInt32(&p.num, 1)
			atomic.AddInt32(&p.success, 1)
			close(p.quickDone)
			return nil
		}
		slow := func(ctx context.Context) error {
			atomic.AddInt32(&p.num, 1)
			close(p.slowStarted)
			if blocks {
				if ignoreCtx {
					<-release
					return errors.New("released")
				}
				<-ctx.Done()
				return ctx.Err()
			}
			atomic.AddInt32(&p.success, 1)
			return nil
		}
		app.addTask("quick", tt, quick, nil)
		app.addTask("slow", tt, slow, nil)
	}

	waitRunning := func(tt TaskType) {
		<-phases[tt].quickDone
		<-phases[tt].slowStarted
	}
	if signalAt != 0 {
		go func() {
			waitRunning(signalAt)
			app.SendSignal(os.Interrupt)
			if signals == 2 {
				waitRunning(TaskTypeCleanup)
				app.SendSignal(os.Interrupt)
			}
		}()
	}

	code := runWithTimeout(t, app)
	return code, ResultSet{
		NumInit:        atomic.LoadInt32(&phases[TaskTypeInit].num),
		SuccessInit:    atomic.LoadInt32(&phases[TaskTypeInit].success),
		NumMain:        atomic.LoadInt32(&phases[TaskTypeMain].num),
		SuccessMain:    atomic.LoadInt32(&phases[TaskTypeMain].success),
		NumCleanup:     atomic.LoadInt32(&phases[TaskTypeCleanup].num),
		SuccessCleanup: atomic.LoadInt32(&phases[TaskTypeCleanup].success),
	}
}

// runWithTimeout runs the App and fails the test if Run does not return.
// The timeout only guards against a deadlock, so it is long enough not to be flaky.
func runWithTimeout(t *testing.T, app *App) int {
	t.Helper()

	result := make(chan int, 1)
	go func() { result <- app.Run() }()

	select {
	case code := <-result:
		return code
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
		return 0
	}
}

func TestApp_InitError(t *testing.T) {
	app := New()

	app.AddInitTask("", func(ctx context.Context) error {
		return errors.New("error")
	})
	var mainCount int32
	main := app.AddMainTask("", func(ctx context.Context) error {
		atomic.AddInt32(&mainCount, 1)
		return nil
	})
	var mainErr error
	app.AddCleanupTask("", func(ctx context.Context) error {
		mainErr = main.Err()
		return nil
	}, RunAfter(main))

	code := app.Run()

	equal(t, code, 1)
	equal(t, mainCount, int32(0))
	equal(t, mainErr, ErrSkipped)
}

func TestApp_SendSignalAfterRun(t *testing.T) {
	app := New()
	app.Run()

	done := make(chan struct{})
	go func() {
		// Send more signals than the buffer of the signal channel.
		app.SendSignal(os.Interrupt)
		app.SendSignal(os.Interrupt)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("SendSignal blocked after Run returned")
	}
}

func TestApp_CanceledError(t *testing.T) {
	t.Run("canceled by app", func(t *testing.T) {
		app := New()
		started := make(chan struct{})
		app.AddMainTask("", func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return fmt.Errorf("wrapped: %w", ctx.Err())
		})
		go func() {
			<-started
			app.SendSignal(os.Interrupt)
		}()

		equal(t, runWithTimeout(t, app), 0)
	})

	t.Run("returned by task", func(t *testing.T) {
		app := New()
		app.AddMainTask("", func(ctx context.Context) error {
			return context.Canceled
		})

		equal(t, runWithTimeout(t, app), 1)
	})
}

func TestApp_RunContext(t *testing.T) {
	type key struct{}
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), key{}, "value"))
	defer cancel()

	app := New()
	started := make(chan struct{})
	var mainValue interface{}
	app.AddMainTask("", func(ctx context.Context) error {
		mainValue = ctx.Value(key{})
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	var cleanupValue, cleanupErr interface{}
	app.AddCleanupTask("", func(ctx context.Context) error {
		cleanupValue = ctx.Value(key{})
		cleanupErr = ctx.Err()
		return nil
	})
	go func() {
		<-started
		cancel()
	}()

	result := make(chan int, 1)
	go func() { result <- app.RunContext(ctx) }()
	select {
	case code := <-result:
		equal(t, code, 0)
	case <-time.After(5 * time.Second):
		t.Fatal("RunContext did not return")
	}

	equal(t, mainValue, "value")
	equal(t, cleanupValue, "value")
	equal(t, cleanupErr, nil)
}

func TestApp_RunTwice(t *testing.T) {
	app := New()
	app.Run()

	defer func() {
		equal(t, recover(), "appmain: Run must be called only once")
	}()
	app.Run()
}

package appmain

import (
	"context"
	"errors"
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
		runner       func(app *App) int
		wantCode     int
		wantResult   ResultSet
		wantDuration time.Duration
	}{
		"success": {
			func(app *App) int {
				return app.Run()
			},
			0,
			ResultSet{
				NumInit:        2,
				SuccessInit:    2,
				NumMain:        2,
				SuccessMain:    2,
				NumCleanup:     2,
				SuccessCleanup: 2,
			},
			300 * time.Millisecond,
		},
		"cancel init": {
			func(app *App) int {
				time.AfterFunc(10*time.Millisecond, func() {
					app.SendSignal(os.Interrupt)
				})
				return app.Run()
			},
			0,
			ResultSet{
				NumInit:        2,
				SuccessInit:    1,
				NumMain:        0,
				SuccessMain:    0,
				NumCleanup:     2,
				SuccessCleanup: 2,
			},
			110 * time.Millisecond,
		},
		"cancel main": {
			func(app *App) int {
				time.AfterFunc(110*time.Millisecond, func() {
					app.SendSignal(os.Interrupt)
				})
				return app.Run()
			},
			0,
			ResultSet{
				NumInit:        2,
				SuccessInit:    2,
				NumMain:        2,
				SuccessMain:    1,
				NumCleanup:     2,
				SuccessCleanup: 2,
			},
			210 * time.Millisecond,
		},
		"cancel cleanup": {
			func(app *App) int {
				time.AfterFunc(210*time.Millisecond, func() {
					app.SendSignal(os.Interrupt)
				})
				return app.Run()
			},
			0,
			ResultSet{
				NumInit:        2,
				SuccessInit:    2,
				NumMain:        2,
				SuccessMain:    2,
				NumCleanup:     2,
				SuccessCleanup: 1,
			},
			210 * time.Millisecond,
		},
		"cancel twice init": {
			func(app *App) int {
				time.AfterFunc(10*time.Millisecond, func() {
					app.SendSignal(os.Interrupt)
					time.AfterFunc(10*time.Millisecond, func() {
						app.SendSignal(os.Interrupt)
					})
				})
				return app.Run()
			},
			128 + int(syscall.SIGINT),
			ResultSet{
				NumInit:        2,
				SuccessInit:    1,
				NumMain:        0,
				SuccessMain:    0,
				NumCleanup:     2,
				SuccessCleanup: 1,
			},
			20 * time.Millisecond,
		},
		"cancel twice main": {
			func(app *App) int {
				time.AfterFunc(110*time.Millisecond, func() {
					app.SendSignal(os.Interrupt)
					time.AfterFunc(10*time.Millisecond, func() {
						app.SendSignal(os.Interrupt)
					})
				})
				return app.Run()
			},
			128 + int(syscall.SIGINT),
			ResultSet{
				NumInit:        2,
				SuccessInit:    2,
				NumMain:        2,
				SuccessMain:    1,
				NumCleanup:     2,
				SuccessCleanup: 1,
			},
			120 * time.Millisecond,
		},
		"cancel twice cleanup": {
			func(app *App) int {
				time.AfterFunc(210*time.Millisecond, func() {
					app.SendSignal(os.Interrupt)
					app.SendSignal(os.Interrupt)
				})
				return app.Run()
			},
			128 + int(syscall.SIGINT),
			ResultSet{
				NumInit:        2,
				SuccessInit:    2,
				NumMain:        2,
				SuccessMain:    2,
				NumCleanup:     2,
				SuccessCleanup: 1,
			},
			210 * time.Millisecond,
		},
	} {
		t.Run(name, func(t *testing.T) {
			code, rs, d := runApp(tt.runner)
			equal(t, code, tt.wantCode)
			equal(t, rs, tt.wantResult)
			if d < tt.wantDuration || d > (tt.wantDuration+10*time.Millisecond) {
				t.Fatalf("want %v got %v", tt.wantDuration, d)
			}
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

func runApp(runner func(*App) int) (int, ResultSet, time.Duration) {
	createTask := func(count, success *int32, wait bool) func(ctx context.Context) error {
		return func(ctx context.Context) error {
			atomic.AddInt32(count, 1)
			if wait {
				select {
				case <-time.After(100 * time.Millisecond):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			atomic.AddInt32(success, 1)
			return nil
		}
	}

	app := New(ErrorStrategy(func(tc TaskContext) Decision {
		if errors.Is(tc.Err(), context.Canceled) {
			return Continue
		}
		return DefaultErrorStrategy(tc)
	}))
	var (
		nInit int32
		sInit int32
	)
	app.AddInitTask("init 1", createTask(&nInit, &sInit, false))
	app.AddInitTask("init 2", createTask(&nInit, &sInit, true))

	var (
		nMain int32
		sMain int32
	)
	app.AddMainTask("main 1", createTask(&nMain, &sMain, false))
	app.AddMainTask("main 2", createTask(&nMain, &sMain, true))

	var (
		nCleanup int32
		sCleanup int32
	)
	app.AddCleanupTask("cleanup 1", createTask(&nCleanup, &sCleanup, false))
	app.AddCleanupTask("cleanup 2", createTask(&nCleanup, &sCleanup, true))

	start := time.Now()
	code := runner(app)
	d := time.Since(start)
	return code, ResultSet{
		NumInit:        atomic.LoadInt32(&nInit),
		SuccessInit:    atomic.LoadInt32(&sInit),
		NumMain:        atomic.LoadInt32(&nMain),
		SuccessMain:    atomic.LoadInt32(&sMain),
		NumCleanup:     atomic.LoadInt32(&nCleanup),
		SuccessCleanup: atomic.LoadInt32(&sCleanup),
	}, d
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

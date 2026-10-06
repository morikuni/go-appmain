package appmain

import (
	"context"
	"errors"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestErrorStrategy_Continue(t *testing.T) {
	var errTCs []TaskContext

	app := New(ErrorStrategy(func(tc TaskContext) Decision {
		errTCs = append(errTCs, tc)
		return Continue
	}))

	var count int32
	main1 := app.AddMainTask("", func(ctx context.Context) error {
		atomic.AddInt32(&count, 1)
		return errors.New("aaa")
	})
	main2 := app.AddMainTask("", func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
		atomic.AddInt32(&count, 1)
		return errors.New("aaa")
	})
	app.AddMainTask("", func(ctx context.Context) error {
		atomic.AddInt32(&count, 1)
		return nil
	})

	code := app.Run()

	equal(t, code, 0)
	equal(t, errTCs, []TaskContext{main1, main2})
	equal(t, count, int32(3))
}

func TestErrorStrategy_Shutdown(t *testing.T) {
	var errTCs []TaskContext

	app := New(ErrorStrategy(func(tc TaskContext) Decision {
		errTCs = append(errTCs, tc)
		return Shutdown
	}))

	var count int32
	main1 := app.AddMainTask("", func(ctx context.Context) error {
		atomic.AddInt32(&count, 1)
		return errors.New("aaa")
	})
	main2 := app.AddMainTask("", func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1000 * time.Millisecond):
		}
		atomic.AddInt32(&count, 1)
		return errors.New("aaa")
	})
	app.AddMainTask("", func(ctx context.Context) error {
		atomic.AddInt32(&count, 1)
		return nil
	})

	code := app.Run()

	equal(t, code, 0)
	equal(t, errTCs, []TaskContext{main1, main2})
	equal(t, count, int32(2))
}

func TestErrorStrategy_Exit(t *testing.T) {
	var errTCs []TaskContext

	app := New(ErrorStrategy(func(tc TaskContext) Decision {
		errTCs = append(errTCs, tc)
		return Exit
	}))

	var count int32
	main1 := app.AddMainTask("", func(ctx context.Context) error {
		atomic.AddInt32(&count, 1)
		return errors.New("aaa")
	})
	main2 := app.AddMainTask("", func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1000 * time.Millisecond):
		}
		atomic.AddInt32(&count, 1)
		return errors.New("aaa")
	})
	app.AddMainTask("", func(ctx context.Context) error {
		atomic.AddInt32(&count, 1)
		return nil
	})

	code := app.Run()

	equal(t, code, 1)
	equal(t, errTCs, []TaskContext{main1, main2})
	equal(t, count, int32(2))
}

func TestDefaultTaskOptions(t *testing.T) {
	app := New(DefaultTaskOptions(
		Interceptor(func(ctx context.Context, tc TaskContext, t Task) error {
			t(ctx)
			t(ctx)
			return nil
		}),
	))

	var count int
	app.AddMainTask("", func(ctx context.Context) error {
		count++
		return errors.New("error")
	})

	code := app.Run()

	equal(t, code, 0)
	equal(t, count, 2)
}

func TestNotifySignal(t *testing.T) {
	t.Run("shutdown", func(t *testing.T) {
		app := New(NotifySignal(syscall.SIGHUP))

		var count int
		app.AddMainTask("", func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
			count++
			return nil
		})

		app.SendSignal(syscall.SIGHUP)
		code := app.Run()

		equal(t, code, 0)
		equal(t, count, 0)
	})

	t.Run("ignore", func(t *testing.T) {
		app := New(NotifySignal(syscall.SIGHUP))

		var count int
		app.AddMainTask("", func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
			count++
			return nil
		})

		app.SendSignal(syscall.SIGTERM)
		code := app.Run()

		equal(t, code, 0)
		equal(t, count, 1)
	})
}

func TestNotifySignal_Disabled(t *testing.T) {
	app := New(NotifySignal())

	var count int32
	app.AddMainTask("", func(ctx context.Context) error {
		// CPU-bound goroutines make the runtime send SIGURG for preemption,
		// which must not be treated as a shutdown signal.
		deadline := time.Now().Add(200 * time.Millisecond)
		done := make(chan struct{})
		for i := 0; i < 4; i++ {
			go func() {
				for time.Now().Before(deadline) {
				}
				done <- struct{}{}
			}()
		}
		for i := 0; i < 4; i++ {
			<-done
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		atomic.AddInt32(&count, 1)
		return nil
	})

	code := app.Run()

	equal(t, code, 0)
	equal(t, count, int32(1))
}

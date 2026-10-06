package appmain

import (
	"context"
	"sync/atomic"
	"testing"
)

func TestRunAfter(t *testing.T) {
	app := New()

	var count int32
	createTask := func(want int32) func(ctx context.Context) error {
		return func(ctx context.Context) error {
			// Tasks run in their own goroutines, where t.Fatalf must not be called.
			if got := atomic.AddInt32(&count, 1); got != want {
				t.Errorf("want %d got %d", want, got)
			}
			return nil
		}
	}

	init := app.AddInitTask("1", createTask(1))
	app.AddInitTask("2", createTask(2), RunAfter(init))

	main1 := app.AddMainTask("3", createTask(3))
	main2 := app.AddMainTask("4 or 5", func(ctx context.Context) error {
		atomic.AddInt32(&count, 1)
		return nil
	}, RunAfter(main1))
	main3 := app.AddMainTask("4 or 5", func(ctx context.Context) error {
		atomic.AddInt32(&count, 1)
		return nil
	}, RunAfter(main1))
	app.AddMainTask("6", createTask(6), RunAfter(main2, main3))

	cleanup := app.AddCleanupTask("7", createTask(7))
	app.AddCleanupTask("8", createTask(8), RunAfter(cleanup))

	app.Run()

	if count != 8 {
		t.Fatalf("want %d got %d", 8, count)
	}
}

func TestIntercept(t *testing.T) {
	app := New()

	var count int
	createInterceptor := func(want int) Interceptor {
		return func(ctx context.Context, taskContext TaskContext, task Task) error {
			count++
			if count != want {
				t.Errorf("want %d got %d", want, count)
			}
			return task(ctx)
		}
	}

	app.AddMainTask("main", func(ctx context.Context) error {
		return nil
	},
		ChainInterceptors(
			createInterceptor(1),
			createInterceptor(2),
			createInterceptor(3),
		),
		createInterceptor(4),
		ChainInterceptors(
			createInterceptor(5),
			createInterceptor(6),
			createInterceptor(7),
		),
	)

	app.Run()

	if count != 7 {
		t.Fatalf("want %d got %d", 7, count)
	}
}

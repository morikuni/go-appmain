# go-appmain

[![test](https://github.com/morikuni/go-appmain/workflows/test/badge.svg?branch=main)](https://github.com/morikuni/go-appmain/actions?query=branch%3Amain)
[![Go Reference](https://pkg.go.dev/badge/github.com/morikuni/go-appmain.svg)](https://pkg.go.dev/github.com/morikuni/go-appmain)
[![Go Report Card](https://goreportcard.com/badge/github.com/morikuni/go-appmain)](https://goreportcard.com/report/github.com/morikuni/go-appmain)
[![codecov](https://codecov.io/gh/morikuni/go-appmain/branch/main/graph/badge.svg)](https://codecov.io/gh/morikuni/go-appmain)

`appmain` simplifies your main function.

Split your application into **init**, **main** and **cleanup** tasks, and `appmain` runs them
in the right order, handles signals for graceful shutdown, and turns the result into an exit code.

## Features

- Graceful shutdown by signals (SIGINT and SIGTERM by default).
- Easy management of task dependencies with `RunAfter`.
- Common operations (logging, metrics, ...) for every task with interceptors.
- Flexible error handling with `ErrorStrategy`.

## Installation

```sh
go get github.com/morikuni/go-appmain
```

## Usage

```go
package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"
	"os"

	"github.com/morikuni/go-appmain"
)

func main() {
	app := appmain.New(
		// Log every task for example.
		appmain.DefaultTaskOptions(
			appmain.Interceptor(func(ctx context.Context, tc appmain.TaskContext, task appmain.Task) error {
				log.Println("start:", tc.Name())
				err := task(ctx)
				log.Println("end:", tc.Name(), err)
				return err
			}),
		),
	)

	var db *sql.DB
	openDB := app.AddInitTask("open db", func(ctx context.Context) error {
		var err error
		db, err = sql.Open("driver", "dsn")
		return err
	})

	server := &http.Server{Addr: ":8080"}
	serverTask := app.AddMainTask("serve http", func(ctx context.Context) error {
		if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})

	app.AddCleanupTask("shutdown http server", func(ctx context.Context) error {
		return server.Shutdown(ctx)
	})
	app.AddCleanupTask("close db", func(ctx context.Context) error {
		// Cleanup tasks run even if "open db" fails.
		if openDB.Err() != nil {
			return nil
		}
		return db.Close()
	}, appmain.RunAfter(openDB, serverTask)) // Close the db after it is opened and the server stops.

	os.Exit(app.Run())
}
```

## Lifecycle

```
init tasks ──(all succeeded)──▶ main tasks ──(all done)──▶ cleanup tasks ──▶ exit
    │                               │                            ▲
    │                               └──── error or signal ───────┤
    │                                                            │
    └──── error or signal (main tasks are skipped) ──────────────┘
```

| Phase   | Added by         | Description |
|---------|------------------|-------------|
| init    | `AddInitTask`    | Run concurrently first. Main tasks start only after all init tasks complete successfully. |
| main    | `AddMainTask`    | Run concurrently. If init tasks fail or are interrupted, main tasks are skipped and `TaskContext.Err()` returns `ErrSkipped`. |
| cleanup | `AddCleanupTask` | **Always** run, even when init or main tasks fail or a signal is received. |

### Signals

- **1st signal** during init/main tasks: the `context.Context` of the running tasks is canceled and cleanup tasks start immediately.
- **1st signal** during cleanup tasks: the `context.Context` of the cleanup tasks is canceled.
- **2nd signal**: `Run` returns immediately without waiting for the remaining tasks, with exit code `128 + signal number`.

To limit the time for cleanup tasks, use `appmain.CleanupTimeout(d)`. When it elapses, the `context.Context` of cleanup tasks
is canceled and `Run` returns immediately with exit code `1`. It is useful when the process is killed after a grace period,
such as `terminationGracePeriodSeconds` of Kubernetes.

Signals are handled only while `Run` is running.

`app.RunContext(ctx)` is like `Run`, but canceling `ctx` also starts the shutdown, as if the first signal is received.
The tasks can read the values of `ctx`, and cleanup tasks are not canceled by `ctx`.
The handled signals can be changed with `appmain.NotifySignal(sigs...)`. `appmain.NotifySignal()` with no arguments disables signal handling.

### Error strategy

When a task returns an error (or panics), the `ErrorStrategy` decides what to do:

| Decision   | Behavior |
|------------|----------|
| `Continue` | Keep running the other tasks. |
| `Shutdown` | Cancel the tasks in the same phase and exit with code `0`. |
| `Exit`     | Cancel the tasks in the same phase and exit with code `1`. |

`DefaultErrorStrategy` returns `Exit` for init and main tasks, and `Continue` for cleanup tasks.

When the App cancels a task (for example, by a signal), the task can simply return `ctx.Err()`.
It is not treated as an error, and `ErrorStrategy` is not called for it.

`ErrorStrategy` may be called concurrently, since cleanup tasks start while the canceled tasks are still stopping.

If a task panics, `TaskContext.Err()` returns `*appmain.PanicError`, which holds the value passed to `panic` and the stack trace:

```go
var pe *appmain.PanicError
if errors.As(tc.Err(), &pe) {
	log.Printf("%s panicked: %v\n%s", tc.Name(), pe.Value, pe.Stack)
}
```

```go
app := appmain.New(appmain.ErrorStrategy(func(tc appmain.TaskContext) appmain.Decision {
	log.Printf("%s failed: %v", tc.Name(), tc.Err())
	return appmain.DefaultErrorStrategy(tc)
}))
```

## Options

| Option                         | For        | Description |
|--------------------------------|------------|-------------|
| `ErrorStrategy(func)`          | `New`      | Decide the behavior when a task fails. |
| `DefaultTaskOptions(opts...)`  | `New`      | Apply task options to every task. |
| `NotifySignal(sigs...)`        | `New`      | Change the signals to handle. |
| `CleanupTimeout(d)`            | `New`      | Limit the time for cleanup tasks. |
| `RunAfter(tasks...)`           | `Add*Task` | Start the task after the given tasks complete (whether they succeed or not). |
| `Interceptor(func)`            | `Add*Task` | Wrap the task execution. Multiple interceptors run in the given order. |

`RunAfter` can refer to tasks in the same phase, and cleanup tasks can also refer to init and main tasks.

## Testing

`app.SendSignal(sig)` emulates receiving a signal, which is useful to test the shutdown behavior of your app.

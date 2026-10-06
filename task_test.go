package appmain

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestPanicError(t *testing.T) {
	errPanic := errors.New("panic error")

	app := New(ErrorStrategy(func(TaskContext) Decision { return Continue }))
	stringPanic := app.AddMainTask("", func(ctx context.Context) error {
		panic("boom")
	})
	errorPanic := app.AddMainTask("", func(ctx context.Context) error {
		panic(errPanic)
	})

	code := app.Run()
	equal(t, code, 0)

	var pe *PanicError
	if !errors.As(stringPanic.Err(), &pe) {
		t.Fatalf("want *PanicError got %T", stringPanic.Err())
	}
	equal(t, pe.Value, "boom")
	equal(t, pe.Error(), "panic: boom")
	if !strings.Contains(string(pe.Stack), "TestPanicError") {
		t.Fatalf("stack does not contain the panicked function:\n%s", pe.Stack)
	}
	equal(t, errors.Unwrap(pe), nil)

	if !errors.Is(errorPanic.Err(), errPanic) {
		t.Fatalf("want %v got %v", errPanic, errorPanic.Err())
	}
}

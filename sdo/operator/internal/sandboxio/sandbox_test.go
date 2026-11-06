package sandboxio_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"sdo.dev/operator/internal/sandboxio"
)

// Handlers run in a child of this test binary: Register serves them from init
// when the binary is started as their child.
func init() {
	sandboxio.Register("test-echo", func(in []byte) []byte { return append([]byte("echo:"), in...) })
	sandboxio.Register("test-hog", func([]byte) []byte {
		var held [][]byte
		for {
			held = append(held, bytes.Repeat([]byte("x"), 64<<20))
		}
	})
	sandboxio.Register("test-spin", func([]byte) []byte {
		for {
		}
	})
	sandboxio.Register("test-flood", func([]byte) []byte { return bytes.Repeat([]byte("y"), 2<<20) })
	sandboxio.Register("test-panic", func([]byte) []byte { panic("handler bug") })
}

var small = sandboxio.Limits{Memory: 128 << 20, Output: 1 << 20, Procs: 1}

func TestChildAnswersTheRequest(t *testing.T) {
	out, err := sandboxio.Run(t.Context(), "test-echo", []byte("ping"), small)
	if err != nil || string(out) != "echo:ping" {
		t.Fatalf("got %q, %v", out, err)
	}
}

func TestChildFailuresAreTyped(t *testing.T) {
	for name, kind := range map[string]sandboxio.Kind{
		"test-hog":   sandboxio.KindMemory,
		"test-flood": sandboxio.KindOutput,
		"test-panic": sandboxio.KindCrash,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := sandboxio.Run(t.Context(), name, nil, small)
			var serr *sandboxio.Error
			if !errors.As(err, &serr) || serr.Kind != kind || !errors.Is(err, sandboxio.ErrChild) {
				t.Fatalf("want a %s child error, got %v", kind, err)
			}
		})
	}
}

func TestContextEndKillsTheChild(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	_, err := sandboxio.Run(ctx, "test-spin", nil, small)
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, sandboxio.ErrChild) {
		t.Fatalf("want the context's error, got %v", err)
	}
}

func TestUnregisteredHandlerIsRefused(t *testing.T) {
	if _, err := sandboxio.Run(t.Context(), "nope", nil, small); err == nil {
		t.Fatal("ran an unregistered handler")
	}
}

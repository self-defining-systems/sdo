package telemetrytest

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"sdo.dev/operator/telemetry"
)

// testTraceClient is the conformance suite of a trace source: its client lists
// traces, reports an empty store as an empty listing and any backend fault or
// cancelled context as an error.
func testTraceClient(t *testing.T, h Harness) {
	t.Helper()
	reader := func(t *testing.T, b Behavior) telemetry.TraceReader {
		r, ok := h.New(t, b).(telemetry.TraceReader)
		if !ok {
			t.Fatal("telemetrytest: a traces client must implement telemetry.TraceReader")
		}
		return r
	}
	list := func(t *testing.T, b Behavior) ([]telemetry.TraceSummary, error) {
		return reader(t, b).Traces(context.Background(), telemetry.TraceQuery{Lookback: time.Hour, End: at})
	}
	t.Run("data", func(t *testing.T) {
		got, err := list(t, BehaviorData)
		if err != nil || !reflect.DeepEqual(got, h.WantTraces) {
			t.Errorf("traces = %+v, %v; want %+v", got, err, h.WantTraces)
		}
	})
	t.Run("no-data-is-empty-not-error", func(t *testing.T) {
		if got, err := list(t, BehaviorEmpty); err != nil || len(got) != 0 {
			t.Errorf("empty store = %+v, %v", got, err)
		}
	})
	for _, b := range []Behavior{BehaviorServerError, BehaviorMalformed} {
		t.Run(b.String()+"-is-error", func(t *testing.T) {
			if got, err := list(t, b); err == nil {
				t.Errorf("%s must be an error, got %+v", b, got)
			}
		})
	}
	t.Run("honors-cancelled-context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := reader(t, BehaviorData).Traces(ctx, telemetry.TraceQuery{End: at}); err == nil {
			t.Error("a cancelled context must be an error")
		}
	})
	t.Run("honors-context-deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		done := make(chan error, 1)
		r := reader(t, BehaviorHang)
		go func() { _, err := r.Traces(ctx, telemetry.TraceQuery{End: at}); done <- err }()
		wait, stop := context.WithTimeout(context.Background(), h.grace()+time.Second)
		defer stop()
		select {
		case err := <-done:
			if err == nil {
				t.Error("a hung backend past the deadline must be an error")
			}
		case <-wait.Done():
			t.Error(errors.New("client ignored the context deadline"))
		}
	})
}

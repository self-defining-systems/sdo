package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock is a settable clock for phase-timing tests.
type fakeClock struct{ nanos atomic.Int64 }

func (c *fakeClock) Now() time.Time          { return time.Unix(0, c.nanos.Load()).UTC() }
func (c *fakeClock) Advance(d time.Duration) { c.nanos.Add(int64(d)) }

// loggedRecords redirects the default logger to JSON lines for one test and
// returns the decoded records with the given message.
func loggedRecords(t *testing.T, message string) func() []map[string]any {
	t.Helper()
	var buffer bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buffer, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return func() []map[string]any {
		var records []map[string]any
		for _, line := range strings.Split(strings.TrimSpace(buffer.String()), "\n") {
			var record map[string]any
			if json.Unmarshal([]byte(line), &record) == nil && record["msg"] == message {
				records = append(records, record)
			}
		}
		return records
	}
}

// phaseMillis reads the phase_ms group of a decoded record.
func phaseMillis(t *testing.T, record map[string]any) map[string]float64 {
	t.Helper()
	group, ok := record["phase_ms"].(map[string]any)
	if !ok {
		t.Fatalf("record has no phase_ms group: %v", record)
	}
	phases := map[string]float64{}
	for name, value := range group {
		phases[name] = value.(float64)
	}
	return phases
}

// A phase is charged only the time its own code took: a nested phase's time
// belongs to the nested phase, so the phases of one hold sum to its length.
func TestPhaseTraceChargesNestedPhasesExclusively(t *testing.T) {
	clock := &fakeClock{}
	trace := newPhaseTrace(clock.Now)
	endOuter := trace.begin("snapshot")
	clock.Advance(time.Second)
	endInner := trace.begin("cache")
	clock.Advance(3 * time.Second)
	endInner()
	clock.Advance(500 * time.Millisecond)
	endOuter()
	endAgain := trace.begin("cache")
	clock.Advance(time.Second)
	endAgain()

	got := trace.durations()
	if got["snapshot"] != 1500*time.Millisecond || got["cache"] != 4*time.Second {
		t.Fatalf("durations = %v, want snapshot 1.5s and cache 4s (accumulated over both calls)", got)
	}
	if names := trace.names(); strings.Join(names, ",") != "snapshot,cache" {
		t.Fatalf("names = %v, want phases in the order they began", names)
	}
}

func TestPhaseTraceIsInertWithoutATrace(t *testing.T) {
	var trace *phaseTrace
	trace.begin("snapshot")()
	measurePhase(context.Background(), "snapshot")()
	if attrs := trace.logAttrs(time.Second); len(attrs) != 0 {
		t.Fatalf("a nil trace logged %v", attrs)
	}
}

// The logged attributes name every phase in milliseconds and the time no
// phase accounts for, so a long hold is attributed or visibly not.
func TestPhaseTraceLogsEveryPhaseAndTheUnattributedRemainder(t *testing.T) {
	read := loggedRecords(t, "phases")
	clock := &fakeClock{}
	trace := newPhaseTrace(clock.Now)
	trace.begin("a")()
	end := trace.begin("b")
	clock.Advance(2 * time.Second)
	end()
	slog.Info("phases", trace.logAttrs(5*time.Second)...)
	records := read()
	if len(records) != 1 {
		t.Fatalf("records = %v", records)
	}
	phases := phaseMillis(t, records[0])
	if phases["a"] != 0 || phases["b"] != 2000 {
		t.Fatalf("phase_ms = %v", phases)
	}
	if records[0]["unattributed_ms"] != float64(3000) {
		t.Fatalf("unattributed_ms = %v, want 3000", records[0]["unattributed_ms"])
	}
}

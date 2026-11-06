// Package telemetrytest holds the conformance suite every telemetry adapter
// must pass and a deterministic in-memory Fake for resolver and runtime tests.
// It mirrors testing/fstest: an adapter's own test supplies a factory that
// fronts a fake backend serving small synthetic response bodies (never
// captures), and TestClient checks the client against the seam's contract.
package telemetrytest

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/telemetry"
)

// Behavior is how the fake backend behind a client answers.
type Behavior int

const (
	// BehaviorData serves the harness's Want result.
	BehaviorData Behavior = iota
	// BehaviorEmpty serves a valid "no data" response.
	BehaviorEmpty
	// BehaviorServerError serves an HTTP 5xx or the protocol's error status.
	BehaviorServerError
	// BehaviorMalformed serves a body the adapter cannot decode.
	BehaviorMalformed
	// BehaviorTooLarge serves more series or lines than the package bounds, and
	// for metrics also non-decodable bulk beyond the response size cap when the
	// adapter has one.
	BehaviorTooLarge
	// BehaviorNonFinite serves NaN and Inf sample values (metrics only).
	BehaviorNonFinite
	// BehaviorHang never answers until the request context ends or the test
	// ends. The factory's backend must unblock itself on t.Cleanup.
	BehaviorHang
)

func (b Behavior) String() string {
	names := [...]string{"data", "empty", "server-error", "malformed", "too-large", "non-finite", "hang"}
	if b < 0 || int(b) >= len(names) {
		return fmt.Sprintf("behavior(%d)", int(b))
	}
	return names[b]
}

// Harness describes one client under test.
type Harness struct {
	Kind telemetry.Kind
	// Query is a valid query for Kind that the backend answers.
	Query telemetry.Query
	// Want is the exact normalized result of Query under BehaviorData.
	Want telemetry.Result
	// New returns a client whose backend behaves as b. It owns cleanup via t.
	New func(t *testing.T, b Behavior) telemetry.Client
	// HangGrace bounds how long a cancelled call may take to return. Zero is 2s.
	HangGrace time.Duration
}

var at = time.Unix(1_700_000_000, 0).UTC()

type check struct {
	name string
	run  func(t *testing.T, h Harness) error
}

func checks() []check {
	return []check{
		{"data", checkData},
		{"no-data-is-empty-not-error", checkEmpty},
		{"server-error-is-error", expectError(BehaviorServerError)},
		{"malformed-is-error", expectError(BehaviorMalformed)},
		{"bounds", checkBounds},
		{"non-finite", checkNonFinite},
		{"honors-context-deadline", checkDeadline},
		{"honors-cancelled-context", checkCancelled},
		{"concurrent", checkConcurrent},
	}
}

// TestClient runs the conformance suite as subtests.
func TestClient(t *testing.T, h Harness) {
	t.Helper()
	if h.New == nil {
		t.Fatal("telemetrytest: Harness.New is required")
	}
	if err := h.Query.Validate(); err != nil {
		t.Fatalf("telemetrytest: Harness.Query is invalid: %v", err)
	}
	if h.Query.Kind != h.Kind {
		t.Fatalf("telemetrytest: Harness.Query kind %q is not the harness kind %q", h.Query.Kind, h.Kind)
	}
	if err := h.Want.Validate(h.Kind); err != nil {
		t.Fatalf("telemetrytest: Harness.Want violates the result contract: %v", err)
	}
	for _, c := range checks() {
		t.Run(c.name, func(t *testing.T) {
			if err := c.run(t, h); err != nil {
				t.Error(err)
			}
		})
	}
}

func checkData(t *testing.T, h Harness) error {
	res, err := h.New(t, BehaviorData).Query(context.Background(), h.Query, at)
	if err != nil {
		return fmt.Errorf("data: %w", err)
	}
	if err := res.Validate(h.Kind); err != nil {
		return err
	}
	if !equalResults(res, h.Want) {
		return fmt.Errorf("data: result %+v differs from want %+v", res, h.Want)
	}
	return nil
}

func equalResults(a, b telemetry.Result) bool {
	if a.Truncated != b.Truncated || len(a.Series) != len(b.Series) || len(a.Lines) != len(b.Lines) {
		return false
	}
	for i := range a.Series {
		if !reflect.DeepEqual(a.Series[i].Labels, b.Series[i].Labels) || len(a.Series[i].Samples) != len(b.Series[i].Samples) {
			return false
		}
		for j, s := range a.Series[i].Samples {
			w := b.Series[i].Samples[j]
			if !s.At.Equal(w.At) || s.Value != w.Value {
				return false
			}
		}
	}
	for i := range a.Lines {
		if !a.Lines[i].At.Equal(b.Lines[i].At) || a.Lines[i].Text != b.Lines[i].Text || !reflect.DeepEqual(a.Lines[i].Labels, b.Lines[i].Labels) {
			return false
		}
	}
	return true
}

func checkEmpty(t *testing.T, h Harness) error {
	res, err := h.New(t, BehaviorEmpty).Query(context.Background(), h.Query, at)
	if err != nil {
		return fmt.Errorf("no data must be an empty result, not an error: %w", err)
	}
	if !res.Empty() {
		return fmt.Errorf("no data must be an empty result, got %+v", res)
	}
	return nil
}

func expectError(b Behavior) func(*testing.T, Harness) error {
	return func(t *testing.T, h Harness) error {
		res, err := h.New(t, b).Query(context.Background(), h.Query, at)
		if err == nil {
			return fmt.Errorf("%s must be an error, got result %+v (an empty result would read as healthy)", b, res)
		}
		return nil
	}
}

func checkBounds(t *testing.T, h Harness) error {
	res, err := h.New(t, BehaviorTooLarge).Query(context.Background(), h.Query, at)
	if err != nil {
		return nil // refusing an oversized answer is allowed
	}
	if err := res.Validate(h.Kind); err != nil {
		return fmt.Errorf("oversized answer escaped the bounds: %w", err)
	}
	if !res.Truncated {
		return errors.New("oversized answer was cut without reporting Truncated")
	}
	if h.Kind == telemetry.KindLogs && len(res.Lines) > h.Query.EffectiveLimit() {
		return fmt.Errorf("result has %d lines over the query limit %d", len(res.Lines), h.Query.EffectiveLimit())
	}
	return nil
}

func checkNonFinite(t *testing.T, h Harness) error {
	if h.Kind != telemetry.KindMetrics {
		return nil
	}
	res, err := h.New(t, BehaviorNonFinite).Query(context.Background(), h.Query, at)
	if err != nil {
		return nil
	}
	for _, s := range res.Series {
		for _, p := range s.Samples {
			if math.IsNaN(p.Value) || math.IsInf(p.Value, 0) {
				return fmt.Errorf("series %s carries a non-finite value", metrics.LabelString(s.Labels))
			}
		}
	}
	return nil
}

func (h Harness) grace() time.Duration {
	if h.HangGrace > 0 {
		return h.HangGrace
	}
	return 2 * time.Second
}

func checkDeadline(t *testing.T, h Harness) error {
	c := h.New(t, BehaviorHang)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	return mustReturnCtxError(ctx, c, h)
}

func checkCancelled(t *testing.T, h Harness) error {
	c := h.New(t, BehaviorData)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return mustReturnCtxError(ctx, c, h)
}

func mustReturnCtxError(ctx context.Context, c telemetry.Client, h Harness) error {
	type out struct {
		res telemetry.Result
		err error
	}
	done := make(chan out, 1)
	go func() {
		res, err := c.Query(ctx, h.Query, at)
		done <- out{res, err}
	}()
	wait, stop := context.WithTimeout(context.Background(), h.grace()+100*time.Millisecond)
	defer stop()
	// The deadline case waits for ctx to expire; the cancelled case is already done.
	select {
	case o := <-done:
		if o.err == nil {
			return fmt.Errorf("a query under an ended context returned %+v without an error", o.res)
		}
		if !errors.Is(o.err, context.Canceled) && !errors.Is(o.err, context.DeadlineExceeded) {
			return fmt.Errorf("a query under an ended context must wrap the context error, got %w", o.err)
		}
		return nil
	case <-wait.Done():
		return errors.New("client ignored its context: no return after the deadline plus grace")
	}
}

func checkConcurrent(t *testing.T, h Harness) error {
	c := h.New(t, BehaviorData)
	const n = 8
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := c.Query(context.Background(), h.Query, at)
			if err == nil && !equalResults(res, h.Want) {
				err = errors.New("concurrent query returned a different result")
			}
			errs[i] = err
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// Violations runs the suite against h and returns the failing check names with
// their errors. It exists so the suite itself can be tested against
// deliberately broken clients; adapter tests use TestClient.
func Violations(t *testing.T, h Harness) map[string]error {
	t.Helper()
	out := map[string]error{}
	for _, c := range checks() {
		if err := c.run(t, h); err != nil {
			out[c.name] = err
		}
	}
	return out
}

// CheckNames lists the suite's check names in order.
func CheckNames() []string {
	var names []string
	for _, c := range checks() {
		names = append(names, c.name)
	}
	return names
}

// Fake is a deterministic in-memory client that honors the full contract. It
// is also the reference for what the conformance suite expects.
type Fake struct {
	kind telemetry.Kind
	want telemetry.Result

	mu       sync.Mutex
	behavior Behavior
	alerts   []metrics.Alert
	calls    []telemetry.Query
}

// NewFake serves want for kind under BehaviorData.
func NewFake(kind telemetry.Kind, want telemetry.Result) *Fake {
	return &Fake{kind: kind, want: want}
}

// Set changes how the fake answers.
func (f *Fake) Set(b Behavior) {
	f.mu.Lock()
	f.behavior = b
	f.mu.Unlock()
}

// SetAlerts makes the fake answer Alerts.
func (f *Fake) SetAlerts(a []metrics.Alert) {
	f.mu.Lock()
	f.alerts = a
	f.mu.Unlock()
}

// Calls returns the queries received, in order.
func (f *Fake) Calls() []telemetry.Query {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]telemetry.Query(nil), f.calls...)
}

// Query implements telemetry.Client.
func (f *Fake) Query(ctx context.Context, q telemetry.Query, _ time.Time) (telemetry.Result, error) {
	f.mu.Lock()
	f.calls = append(f.calls, q)
	b := f.behavior
	f.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return telemetry.Result{}, err
	}
	switch b {
	case BehaviorData:
		return f.want, nil
	case BehaviorEmpty:
		return telemetry.Result{}, nil
	case BehaviorServerError:
		return telemetry.Result{}, errors.New("fake backend: 503")
	case BehaviorMalformed:
		return telemetry.Result{}, errors.New("fake backend: malformed body")
	case BehaviorHang:
		<-ctx.Done()
		return telemetry.Result{}, ctx.Err()
	case BehaviorTooLarge:
		return f.tooLarge(q), nil
	case BehaviorNonFinite:
		return f.nonFinite(), nil
	}
	return telemetry.Result{}, fmt.Errorf("fake: unknown behavior %d", int(b))
}

func (f *Fake) tooLarge(q telemetry.Query) telemetry.Result {
	if f.kind == telemetry.KindLogs {
		n := q.EffectiveLimit()
		lines := make([]telemetry.Line, n)
		for i := range lines {
			lines[i] = telemetry.Line{At: at.Add(time.Duration(i) * time.Second), Text: "line"}
		}
		return telemetry.Result{Lines: lines, Truncated: true}
	}
	series := make([]metrics.Series, telemetry.MaxSeries)
	for i := range series {
		series[i] = metrics.Series{Labels: map[string]string{"n": fmt.Sprint(i)}, Samples: []metrics.Sample{{At: at, Value: 1}}}
	}
	return telemetry.Result{Series: series, Truncated: true}
}

func (f *Fake) nonFinite() telemetry.Result {
	// Non-finite samples are dropped, and so is a series left without samples.
	var series []metrics.Series
	for _, s := range f.want.Series {
		var kept []metrics.Sample
		for _, p := range s.Samples {
			if !math.IsNaN(p.Value) && !math.IsInf(p.Value, 0) {
				kept = append(kept, p)
			}
		}
		if len(kept) > 0 {
			series = append(series, metrics.Series{Labels: s.Labels, Samples: kept})
		}
	}
	return telemetry.Result{Series: series}
}

// Alerts implements telemetry.AlertReader.
func (f *Fake) Alerts(ctx context.Context) ([]metrics.Alert, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]metrics.Alert(nil), f.alerts...), nil
}

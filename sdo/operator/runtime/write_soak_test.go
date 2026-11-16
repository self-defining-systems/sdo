package runtime

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"

	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk/traffic"
)

type soakProber struct {
	mu      sync.Mutex
	windows map[string]traffic.Window
	bursts  []prober.BurstRequest
	result  prober.BurstResult
	err     error
}

func (p *soakProber) Windows(context.Context) (map[string]traffic.Window, error) {
	return p.windows, nil
}
func (p *soakProber) Burst(_ context.Context, request prober.BurstRequest) (prober.BurstResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.bursts = append(p.bursts, request)
	if p.err != nil || p.result.Workload != "" {
		return p.result, p.err
	}
	return prober.BurstResult{Healthy: true}, nil
}
func (*soakProber) Reset(context.Context) error { return nil }

func soakWindow(effects map[string]traffic.SideEffect) traffic.Window {
	var window traffic.Window
	for id, effect := range effects {
		window.Scenarios = append(window.Scenarios, traffic.ScenarioObservations{Scenario: traffic.Descriptor{ID: id, SideEffect: effect}})
	}
	return window
}

// A watch that soaks writes runs only the write scenarios, the given number of
// rounds, so records accumulate in the lists the read probes use.
func TestSoakWritesRunsOnlyWriteScenariosEachRound(t *testing.T) {
	api := &soakProber{windows: map[string]traffic.Window{
		"probe-main": soakWindow(map[string]traffic.SideEffect{
			"read-list":  traffic.SideEffectRead,
			"create-one": traffic.SideEffectWriteWithCleanup,
		}),
		"probe-reads": soakWindow(map[string]traffic.SideEffect{"read-only": traffic.SideEffectRead}),
	}}
	if err := soakWrites(context.Background(), api, 3, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	want := []prober.BurstRequest{{Workload: "probe-main", Scenarios: []string{"create-one"}}}
	want = append(want, want[0], want[0])
	if !reflect.DeepEqual(api.bursts, want) {
		t.Fatalf("bursts = %+v, want %+v", api.bursts, want)
	}
}

func TestSoakWritesWithZeroRoundsSendsNothing(t *testing.T) {
	api := &soakProber{windows: map[string]traffic.Window{"w": soakWindow(map[string]traffic.SideEffect{"c": traffic.SideEffectIdempotentWrite})}}
	if err := soakWrites(context.Background(), api, 0, &bytes.Buffer{}); err != nil || len(api.bursts) != 0 {
		t.Fatalf("err=%v bursts=%v", err, api.bursts)
	}
}

// A failed write round stops that workload's soak (the detectors already see the
// failure); a burst error is reported, not fatal.
func TestSoakWritesStopsAfterAFailedRoundAndReportsErrors(t *testing.T) {
	windows := map[string]traffic.Window{"w": soakWindow(map[string]traffic.SideEffect{"c": traffic.SideEffectIdempotentWrite})}
	failed := &soakProber{windows: windows, result: prober.BurstResult{Workload: "w", Healthy: false}}
	if err := soakWrites(context.Background(), failed, 5, &bytes.Buffer{}); err != nil || len(failed.bursts) != 1 {
		t.Fatalf("err=%v bursts=%d, want one round", err, len(failed.bursts))
	}
	broken := &soakProber{windows: windows, err: errors.New("boom")}
	var out bytes.Buffer
	if err := soakWrites(context.Background(), broken, 5, &out); err != nil || len(broken.bursts) != 1 || !strings.Contains(out.String(), "boom") {
		t.Fatalf("err=%v bursts=%d out=%q", err, len(broken.bursts), out.String())
	}
}

func TestSoakWritesSkipsLinkProbes(t *testing.T) {
	window := soakWindow(map[string]traffic.SideEffect{"c": traffic.SideEffectIdempotentWrite})
	window.Links = []traffic.LinkObservations{{}}
	api := &soakProber{windows: map[string]traffic.Window{"links": window}}
	if err := soakWrites(context.Background(), api, 2, &bytes.Buffer{}); err != nil || len(api.bursts) != 0 {
		t.Fatalf("err=%v bursts=%v", err, api.bursts)
	}
}

func TestEvaluateOnceRejectsBadWriteSoak(t *testing.T) {
	for _, args := range [][]string{
		{"--evaluate-once", "--namespace", "shop", "--write-soak", "-1", "--watch", "1s"},
		{"--evaluate-once", "--namespace", "shop", "--write-soak", "3"},
	} {
		err := RunWithOptions(context.Background(), nil, RuntimeOptions{Args: args, Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}})
		if err == nil || !strings.Contains(err.Error(), "--write-soak") {
			t.Fatalf("args %v: err = %v, want a --write-soak error", args, err)
		}
	}
}

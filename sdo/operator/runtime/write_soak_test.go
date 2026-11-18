package runtime

import (
	"bytes"
	"context"
	"reflect"
	"sync"
	"testing"

	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk/traffic"
)

type soakProber struct {
	mu      sync.Mutex
	windows map[string]traffic.Window
	bursts  []prober.BurstRequest
}

func (p *soakProber) Windows(context.Context) (map[string]traffic.Window, error) {
	return p.windows, nil
}
func (p *soakProber) Burst(_ context.Context, request prober.BurstRequest) (prober.BurstResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.bursts = append(p.bursts, request)
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

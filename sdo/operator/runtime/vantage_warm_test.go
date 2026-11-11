package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/vantage"
)

// startingProvider answers with a helper still starting for its first rounds,
// then with a dial.
type startingProvider struct{ calls, startingFor int }

func (p *startingProvider) Snapshot(context.Context) (sdk.DetectionContext, error) {
	p.calls++
	reading := vantage.Reading{Pod: "api-0", Kind: vantage.TCPConnect, Dials: []vantage.Dial{{OK: true}}}
	if p.calls <= p.startingFor {
		reading = vantage.Reading{Pod: "api-0", Kind: vantage.TCPConnect, Error: errVantageHelperStarting.Error()}
	}
	results := vantageResults{"client.api.db.5432": {Readings: []vantage.Reading{reading}}}
	return inputs.New(sdktest.Snapshot{NamespaceName: "app"}).WithVantage(results), nil
}

// A one-shot evaluation attaches the vantage helpers on its first read, so it
// waits (bounded) until no helper is starting before it judges.
func TestAwaitVantageHelpersWaitsUntilNoHelperIsStarting(t *testing.T) {
	provider := &startingProvider{startingFor: 2}
	var slept time.Duration
	awaitVantageHelpers(context.Background(), provider, time.Minute, func(d time.Duration) { slept += d })
	if provider.calls != 3 {
		t.Fatalf("snapshots = %d, want 3 (two starting, one ready)", provider.calls)
	}
	bounded := &startingProvider{startingFor: 1000}
	slept = 0
	awaitVantageHelpers(context.Background(), bounded, 10*time.Second, func(d time.Duration) { slept += d })
	if slept > 10*time.Second || bounded.calls > 10 {
		t.Fatalf("the wait is bounded: slept %s over %d snapshots", slept, bounded.calls)
	}
}

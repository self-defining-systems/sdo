package runtime

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// blockedProber never answers, like a dial to a port the repaired Service no longer exposes.
type blockedProber struct{}

func (blockedProber) Probe(ctx context.Context, _ sdk.ObjectRef, _ int32, _ *program.HTTPProbe) (program.ProbeResult, error) {
	<-ctx.Done()
	return program.ProbeResult{}, ctx.Err()
}

// A closure replay of an open-phase probe must not hold the controller for the live probe's full budget.
func TestClosureReplayProbeIsBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		live := LiveReadView{Carrier: inputs.New(sdktest.Snapshot{NamespaceName: "toy"}), Prober: blockedProber{}}
		start := time.Now()
		_, err := boundedProbeView{live}.ProbeTCP(sdk.ObjectRef{Kind: "Service", Namespace: "toy", Name: "backend"}, 9999)
		if err == nil {
			t.Fatal("blocked probe unexpectedly succeeded")
		}
		if elapsed := time.Since(start); elapsed > closureReplayProbeTimeout {
			t.Fatalf("closure replay probe held the controller for %v", elapsed)
		}
	})
}

//go:build integration

package bench

import (
	"context"
	"testing"
)

// docs/contract.md: an unsupported workload verb is never a failure; the platform skips it. A workload with no
// fingerprint, no image delivery and nothing to recover answers exit 64 to those verbs, and the build goes on.
func TestUnsupportedWorkloadVerbsAreSkippedNotFailed(t *testing.T) {
	r := newRunner(t, presentCluster)
	r.wl = adapter(t, `exit 64`)
	p := &Prepared{Namespace: "ns", AppPack: "pack"}
	ctx := context.Background()
	if !r.recordFresh(ctx, p) {
		t.Errorf("an unsupported app-snapshot write failed the build: %s", r.res.Reason)
	}
	if !r.recoverLane(ctx, p) {
		t.Errorf("an unsupported app-reset recover took the lane down: %s %s", r.res.Reason, r.res.LaneDown)
	}
	if why := r.appMatchesFresh(ctx, p); why == "" {
		t.Error("an unsupported fingerprint verify claimed the app copy matches the fresh deploy")
	}
}

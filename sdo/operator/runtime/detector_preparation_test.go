//go:build integration

package runtime

import (
	"context"
	"testing"
	"time"

	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memoryhost"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// Publishing a frozen view is controller-owned input preparation, not execution
// of an authored detector. A slow publication must not spend that detector's
// entire execution budget before its first RPC.
type slowPreparationHost struct {
	speedHost
	delay time.Duration
}

func (h *slowPreparationHost) Capture(ctx context.Context, view sdk.DetectionContext) (memoryhost.Host, uint64, func(), error) {
	select {
	case <-time.After(h.delay):
	case <-ctx.Done():
		return nil, 0, nil, ctx.Err()
	}
	return h, 1, func() {}, nil
}

func (h *slowPreparationHost) Evaluate(ctx context.Context, r *pb.EvaluateRequest) (*pb.EvaluateResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return h.speedHost.Evaluate(ctx, r)
}

func TestRemotePreparationDoesNotSpendDetectorExecutionBudget(t *testing.T) {
	host := &slowPreparationHost{delay: minDetectorDeadline + 20*time.Millisecond}
	remote := &RemoteMemory{rpc: host, memoryCommit: "accepted"}
	detector := &remoteDetector{Registration: memoryhost.Registration{DetectorSpec: sdk.DetectorSpec{ID: "healthy"}}, remote: remote}
	view := NewCycleReadView(program.ReadFrom(sdktest.Snapshot{NamespaceName: "app"}))
	result := evaluateDetectors(context.Background(), []sdk.Detector{detector}, view)["healthy"]
	if result.err != nil {
		t.Fatalf("input preparation consumed execution budget: %v", result.err)
	}
}

func TestRemotePreparationHonorsParentCancellation(t *testing.T) {
	host := &slowPreparationHost{delay: time.Hour}
	remote := &RemoteMemory{rpc: host, memoryCommit: "accepted"}
	detector := &remoteDetector{Registration: memoryhost.Registration{DetectorSpec: sdk.DetectorSpec{ID: "healthy"}}, remote: remote}
	view := NewCycleReadView(program.ReadFrom(sdktest.Snapshot{NamespaceName: "app"}))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result := evaluateDetectors(ctx, []sdk.Detector{detector}, view)["healthy"]
	if result.err != context.Canceled {
		t.Fatalf("preparation ignored parent cancellation: %v", result.err)
	}
}

package memoryhost

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/sdktest"
)

// stalledHost answers nothing until its caller gives up, like a slow but healthy host.
type stalledHost struct{ *mirrorTestHost }

func (h *stalledHost) Evaluate(ctx context.Context, _ *pb.EvaluateRequest) (*pb.EvaluateResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func activeStalledHost(t *testing.T, m *Manager) *stalledHost {
	t.Helper()
	ctx := context.Background()
	if err := m.Publish(ctx, sdktest.Snapshot{NamespaceName: "toy"}); err != nil {
		t.Fatal(err)
	}
	h := &stalledHost{newMirrorTestHost(t, "accepted")}
	if err := m.Prepare(ctx, h); err != nil {
		t.Fatal(err)
	}
	if err := m.Activate(ctx, h, "accepted", "digest"); err != nil {
		t.Fatal(err)
	}
	return h
}

// A caller that abandons an evaluation (for example when an incident opens and
// the detection iteration is cancelled) bounds that call, not the host's life.
func TestEvaluateCallerCancelPreservesAcceptedHost(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := NewManager(30 * time.Second)
		defer m.Close()
		h := activeStalledHost(t, m)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, _ = m.Evaluate(ctx, &pb.EvaluateRequest{DetectorIds: []string{"d"}})
		if m.active != Host(h) {
			t.Fatal("caller cancellation evicted the healthy accepted host")
		}
	})
}

func TestCapturedEvaluateCallerCancelPreservesAcceptedHost(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := NewManager(30 * time.Second)
		defer m.Close()
		h := activeStalledHost(t, m)
		view := sdktest.Snapshot{NamespaceName: "toy"}
		leased, seq, release, err := m.Capture(context.Background(), view)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if _, err := leased.Evaluate(ctx, &pb.EvaluateRequest{MirrorSequence: seq}); err == nil {
			t.Fatal("stalled host unexpectedly answered")
		}
		release()
		if _, _, again, err := m.Capture(context.Background(), view); err != nil {
			t.Fatalf("caller cancellation evicted the healthy accepted host: %v", err)
		} else {
			again()
		}
		if m.active != Host(h) {
			t.Fatal("accepted host replaced")
		}
	})
}

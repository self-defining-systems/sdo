package memoryhost

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

type fakeHost struct {
	ready    chan struct{}
	identity *pb.ReadyResponse
	fail     bool
	mu       sync.Mutex
	closed   bool
}

func (h *fakeHost) Ready(ctx context.Context) (*pb.ReadyResponse, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-h.ready:
		return h.identity, nil
	}
}
func (h *fakeHost) Evaluate(ctx context.Context, r *pb.EvaluateRequest) (*pb.EvaluateResponse, error) {
	if h.fail {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &pb.EvaluateResponse{Results: []*pb.MemoryDetectorResult{{DetectorId: h.identity.MemoryCommit}}}, nil
}
func (h *fakeHost) Close() error { h.mu.Lock(); defer h.mu.Unlock(); h.closed = true; return nil }
func readyHost(commit string) *fakeHost {
	ch := make(chan struct{})
	close(ch)
	return &fakeHost{ready: ch, identity: &pb.ReadyResponse{MemoryCommit: commit, BinaryDigest: "sha256:abc", MirrorComplete: true, MirrorSequence: 1}}
}
func TestSwapKeepsOldUntilMirrorReady(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := NewManager(time.Second)
		defer m.Close()
		old := readyHost("old")
		if err := m.Swap(context.Background(), old, "old", "sha256:abc", 1); err != nil {
			t.Fatal(err)
		}
		next := readyHost("next")
		next.ready = make(chan struct{})
		done := make(chan error, 1)
		go func() { done <- m.Swap(context.Background(), next, "next", "sha256:abc", 1) }()
		r, err := m.Evaluate(context.Background(), &pb.EvaluateRequest{DetectorIds: []string{"x"}})
		if err != nil || r.Results[0].DetectorId != "old" {
			t.Fatalf("old host unavailable: %v %v", r, err)
		}
		close(next.ready)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		r, err = m.Evaluate(context.Background(), &pb.EvaluateRequest{})
		if err != nil || r.Results[0].DetectorId != "next" {
			t.Fatal(r, err)
		}
		old.mu.Lock()
		defer old.mu.Unlock()
		if !old.closed {
			t.Fatal("old child leaked")
		}
	})
}
func TestUnavailableNeverClears(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := NewManager(10 * time.Millisecond)
		defer m.Close()
		h := readyHost("hung")
		h.fail = true
		if err := m.Swap(context.Background(), h, "hung", "sha256:abc", 1); err != nil {
			t.Fatal(err)
		}
		r, err := m.Evaluate(context.Background(), &pb.EvaluateRequest{DetectorIds: []string{"health"}})
		if err != nil || len(r.Results) != 1 || r.Results[0].Error == "" || len(r.Results[0].Findings) != 0 {
			t.Fatal(r, err)
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		if !h.closed {
			t.Fatal("hung child not terminated")
		}
	})
}
func TestIdentityAndMirrorMismatchRejectCandidate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, mutate := range []func(*pb.ReadyResponse){func(r *pb.ReadyResponse) { r.MemoryCommit = "wrong" }, func(r *pb.ReadyResponse) { r.BinaryDigest = "wrong" }, func(r *pb.ReadyResponse) { r.MirrorComplete = false }, func(r *pb.ReadyResponse) { r.MirrorSequence = 0 }} {
			m := NewManager(time.Second)
			h := readyHost("next")
			mutate(h.identity)
			if err := m.Swap(context.Background(), h, "next", "sha256:abc", 1); err == nil {
				t.Fatal("accepted mismatched candidate")
			}
			m.Close()
		}
	})
}

func TestFailedHostRestarts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := NewManager(50 * time.Millisecond)
		defer m.Close()
		h := readyHost("revision")
		h.fail = true
		if err := m.Swap(context.Background(), h, "revision", "sha256:abc", 1); err != nil {
			t.Fatal(err)
		}
		restarted := make(chan struct{})
		m.SetRecovery(func(context.Context) (Host, string, string, uint64, error) {
			close(restarted)
			return readyHost("revision"), "revision", "sha256:abc", 1, nil
		})
		got, _ := m.Evaluate(context.Background(), &pb.EvaluateRequest{DetectorIds: []string{"health"}})
		if got.Results[0].Error == "" {
			t.Fatal("failure cleared detector")
		}
		select {
		case <-restarted:
		case <-time.After(time.Second):
			t.Fatal("restart was not attempted")
		}
		// Swap waits for an in-flight restart to publish; subsequent evaluations use
		// the replacement rather than the broken child.
		m.swap.Lock()
		m.swap.Unlock()
		deadline := time.After(time.Second)
		for {
			got, _ = m.Evaluate(context.Background(), &pb.EvaluateRequest{DetectorIds: []string{"health"}})
			if got.Results[0].Error == "" {
				break
			}
			select {
			case <-deadline:
				t.Fatal("replacement never served")
			default:
				time.Sleep(time.Millisecond)
			}
		}
	})
}

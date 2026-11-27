package memoryhost

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// gatedDetector blocks its first evaluation until release closes; later ones
// answer at once. ignoreContext makes the first one outlive its caller.
type gatedDetector struct {
	release       chan struct{}
	calls         *atomic.Int32
	ignoreContext bool
}

func (d gatedDetector) Spec() sdk.DetectorSpec { return sdk.DetectorSpec{ID: "gated"} }
func (d gatedDetector) Detect(ctx context.Context, _ sdk.DetectionContext) ([]sdk.Finding, error) {
	if d.calls.Add(1) == 1 {
		if d.ignoreContext {
			<-d.release
		} else {
			select {
			case <-d.release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	return []sdk.Finding{{DetectorID: "gated", RuleID: "state", Status: sdk.FindingActive, Severity: sdk.SeverityCritical, Fingerprint: "now"}}, nil
}

func gatedServer(t *testing.T, detector gatedDetector) *Server {
	t.Helper()
	s, err := NewServer("c", "d", []sdk.Detector{detector})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Apply(&pb.MirrorRequest{Sequence: 1, ResetMirror: true, Complete: true, Namespace: "app"}); err != nil {
		t.Fatal(err)
	}
	return s
}

// An evaluation that arrives while a scheduled one of the same detector runs
// waits for it and then answers from its own view: it never fails as busy.
func TestEvaluateCoalescesWithARunningEvaluation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		detector := gatedDetector{release: make(chan struct{}), calls: &atomic.Int32{}}
		s := gatedServer(t, detector)
		request := &pb.EvaluateRequest{MirrorSequence: 1, DetectorIds: []string{"gated"}}
		go func() { _, _ = s.Evaluate(context.Background(), request) }()
		synctest.Wait()
		answered := make(chan *pb.EvaluateResponse, 1)
		go func() {
			response, err := s.Evaluate(context.Background(), request)
			if err != nil {
				t.Error(err)
			}
			answered <- response
		}()
		synctest.Wait()
		select {
		case response := <-answered:
			close(detector.release)
			t.Fatalf("the second evaluation answered beside the running one: %v", response)
		default:
		}
		close(detector.release)
		response := <-answered
		if got := response.Results[0]; got.Error != "" || len(got.Findings) != 1 {
			t.Fatalf("the coalesced evaluation must answer, got %v", got)
		}
		if got := detector.calls.Load(); got != 2 {
			t.Fatalf("the detector ran %d times, want the running one and its own", got)
		}
	})
}

// A run its caller abandoned (a detector ignoring its context) is never waited
// for: the next evaluation answers busy at once, as before.
func TestEvaluateDoesNotWaitForAnAbandonedRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		detector := gatedDetector{release: make(chan struct{}), calls: &atomic.Int32{}, ignoreContext: true}
		defer close(detector.release)
		s := gatedServer(t, detector)
		request := &pb.EvaluateRequest{MirrorSequence: 1, DetectorIds: []string{"gated"}}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		first, err := s.Evaluate(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(first.Results[0].Error, "abandoned") {
			t.Fatalf("the hung run must be abandoned at its deadline: %v", first.Results[0])
		}
		started := time.Now()
		second, err := s.Evaluate(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(second.Results[0].Error, "still running") || time.Since(started) != 0 {
			t.Fatalf("an abandoned run must answer busy at once: %v after %s", second.Results[0], time.Since(started))
		}
	})
}

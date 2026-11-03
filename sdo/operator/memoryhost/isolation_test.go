package memoryhost

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// A failing detector must not delay the others or host recovery: no lock or
// lease may be held across a call into detector code.

// scriptedHost answers per detector: "hang" waits for the call's context, "boom"
// fails the transport, anything else is answered.
type scriptedHost struct{ *mirrorTestHost }

func (h *scriptedHost) Evaluate(ctx context.Context, r *pb.EvaluateRequest) (*pb.EvaluateResponse, error) {
	switch r.GetDetectorIds()[0] {
	case "hang":
		<-ctx.Done()
		return nil, ctx.Err()
	case "boom":
		return nil, errors.New("transport broke")
	}
	return h.mirrorTestHost.Evaluate(ctx, r)
}

func activeScriptedHost(t *testing.T, m *Manager, commit string) *scriptedHost {
	t.Helper()
	ctx := context.Background()
	if err := m.Publish(ctx, sdktest.Snapshot{NamespaceName: "toy"}); err != nil {
		t.Fatal(err)
	}
	h := &scriptedHost{newMirrorTestHost(t, commit)}
	if err := m.Prepare(ctx, h); err != nil {
		t.Fatal(err)
	}
	if err := m.Activate(ctx, h, commit, "digest"); err != nil {
		t.Fatal(err)
	}
	return h
}

// evaluateCaptured is one controller detector evaluation.
func evaluateCaptured(m *Manager, view sdk.DetectionContext, detector string) error {
	host, sequence, release, err := m.Capture(context.Background(), view)
	if err != nil {
		return err
	}
	defer release()
	_, err = host.Evaluate(context.Background(), &pb.EvaluateRequest{DetectorIds: []string{detector}, MirrorSequence: sequence})
	return err
}

func TestHungEvaluationDoesNotDelayOtherDetectors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := NewManager(10 * time.Second)
		defer m.Close()
		activeScriptedHost(t, m, "accepted")
		view := sdktest.Snapshot{NamespaceName: "toy"}
		hung := make(chan error, 1)
		go func() { hung <- evaluateCaptured(m, view, "hang") }()
		synctest.Wait()
		start := time.Now()
		for _, id := range []string{"a", "b", "c"} {
			if err := evaluateCaptured(m, view, id); err != nil {
				t.Fatalf("%s: %v", id, err)
			}
		}
		if waited := time.Since(start); waited != 0 {
			t.Fatalf("detectors waited %s behind a hung evaluation", waited)
		}
		if err := <-hung; err == nil {
			t.Fatal("hung evaluation answered")
		}
	})
}

func TestHostFailureRecoversWhileAnotherEvaluationHangs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		m := NewManager(10 * time.Second)
		defer m.Close()
		activeScriptedHost(t, m, "accepted")
		replacement := &scriptedHost{newMirrorTestHost(t, "accepted")}
		m.SetRecovery(func(ctx context.Context) (Host, string, string, uint64, error) {
			if err := m.Prepare(ctx, replacement); err != nil {
				return replacement, "", "", 0, err
			}
			return replacement, "accepted", "digest", 1, nil
		})
		view := sdktest.Snapshot{NamespaceName: "toy"}
		hung := make(chan error, 1)
		go func() { hung <- evaluateCaptured(m, view, "hang") }()
		synctest.Wait()
		start := time.Now()
		if err := evaluateCaptured(m, view, "boom"); err == nil {
			t.Fatal("transport failure was cleared")
		}
		// A recovering manager answers "unavailable" until the replacement is in.
		for {
			host, _, release, err := m.Capture(ctx, view)
			if err == nil {
				release()
				if host.(*capturedHost).Host != Host(replacement) {
					t.Fatal("served by the failed host")
				}
				break
			}
			time.Sleep(time.Millisecond)
		}
		if waited := time.Since(start); waited > time.Second {
			t.Fatalf("recovery waited %s behind a hung evaluation", waited)
		}
		<-hung
	})
}

func TestPublishDuringLeaseDoesNotWaitAndLeaseKeepsItsView(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		m := NewManager(10 * time.Second)
		defer m.Close()
		h := activeScriptedHost(t, m, "accepted")
		view := sdktest.Snapshot{NamespaceName: "toy", PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "toy"}}}}
		leased, sequence, release, err := m.Capture(ctx, view)
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		start := time.Now()
		if err := m.Publish(ctx, sdktest.Snapshot{NamespaceName: "toy"}); err != nil {
			t.Fatal(err)
		}
		if waited := time.Since(start); waited != 0 {
			t.Fatalf("publish waited %s for a lease", waited)
		}
		if _, err := leased.Evaluate(ctx, &pb.EvaluateRequest{DetectorIds: []string{"a"}, MirrorSequence: sequence}); err != nil {
			t.Fatalf("lease lost its frozen view: %v", err)
		}
		// The host never serves a view it was not asked for.
		if _, err := h.snapshot(sequence + 99); err == nil {
			t.Fatal("unknown sequence served")
		}
	})
}

func TestRetiredHostClosesWhenLastLeaseEnds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		m := NewManager(10 * time.Second)
		defer m.Close()
		old := activeScriptedHost(t, m, "old")
		view := sdktest.Snapshot{NamespaceName: "toy"}
		_, _, release, err := m.Capture(ctx, view)
		if err != nil {
			t.Fatal(err)
		}
		next := &scriptedHost{newMirrorTestHost(t, "next")}
		if err := m.Prepare(ctx, next); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		if err := m.Activate(ctx, next, "next", "digest"); err != nil {
			t.Fatal(err)
		}
		if waited := time.Since(start); waited != 0 {
			t.Fatalf("activation waited %s for a lease", waited)
		}
		if old.closed {
			t.Fatal("retired host closed under a lease")
		}
		release()
		if !old.closed {
			t.Fatal("retired host leaked after its last lease")
		}
	})
}

// blockingDetector ignores its context, like agent code stuck in a loop.
type blockingDetector struct {
	id      string
	stop    chan struct{}
	started chan struct{}
}

func (d blockingDetector) Spec() sdk.DetectorSpec { return sdk.DetectorSpec{ID: d.id} }
func (d blockingDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	d.started <- struct{}{}
	<-d.stop
	return nil, nil
}

func TestHungDetectorIsBoundedAndTheHostKeepsServing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		hung := blockingDetector{id: "hung", stop: make(chan struct{}), started: make(chan struct{}, 10)}
		s, err := NewServer("c", "d", []sdk.Detector{hung, testDetector{}})
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Apply(&pb.MirrorRequest{Sequence: 1, ResetMirror: true, Complete: true, Namespace: "toy"}); err != nil {
			t.Fatal(err)
		}
		call := func(id string) *pb.MemoryDetectorResult {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			response, err := s.Evaluate(ctx, &pb.EvaluateRequest{MirrorSequence: 1, DetectorIds: []string{id}})
			if err != nil {
				t.Fatalf("%s: a detector hang failed the host call: %v", id, err)
			}
			return response.Results[0]
		}
		start := time.Now()
		if got := call("hung"); got.Error == "" {
			t.Fatal("hung detector reported no error")
		}
		if waited := time.Since(start); waited >= 10*time.Second {
			t.Fatalf("hung detector used the whole transport budget: %s", waited)
		}
		// While it is still stuck, another evaluation neither starts a second copy
		// nor waits, and other detectors are unaffected.
		start = time.Now()
		if got := call("hung"); got.Error == "" {
			t.Fatal("stuck detector started again")
		}
		if got := call("synthetic"); got.Error != "" || len(got.Findings) != 1 {
			t.Fatalf("healthy detector affected: %+v", got)
		}
		if waited := time.Since(start); waited != 0 {
			t.Fatalf("evaluations waited %s behind a stuck detector", waited)
		}
		if len(hung.started) != 1 {
			t.Fatalf("stuck detector was started %d times", len(hung.started))
		}
		close(hung.stop)
		synctest.Wait()
		if got := call("hung"); got.Error != "" {
			t.Fatalf("detector did not recover after it returned: %+v", got)
		}
	})
}

func TestEvaluateServesRetainedEarlierSequence(t *testing.T) {
	s, err := NewServer("c", "d", []sdk.Detector{testDetector{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Apply(&pb.MirrorRequest{Sequence: 1, ResetMirror: true, Complete: true, Namespace: "toy"}); err != nil {
		t.Fatal(err)
	}
	for sequence := uint64(2); sequence <= 4; sequence++ {
		if err := s.Apply(&pb.MirrorRequest{Sequence: sequence, Complete: true}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Evaluate(context.Background(), &pb.EvaluateRequest{MirrorSequence: 2, DetectorIds: []string{"synthetic"}}); err != nil {
		t.Fatalf("retained sequence: %v", err)
	}
	for sequence := uint64(5); sequence <= 5+mirrorHistory; sequence++ {
		if err := s.Apply(&pb.MirrorRequest{Sequence: sequence, Complete: true}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Evaluate(context.Background(), &pb.EvaluateRequest{MirrorSequence: 2, DetectorIds: []string{"synthetic"}}); err == nil {
		t.Fatal("evaluated a sequence the host no longer holds")
	}
}

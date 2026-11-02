package memoryhost

import (
	"context"
	"encoding/json"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/capturefixture"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
	"testing/synctest"
	"time"
)

type mirrorTestHost struct {
	*Server
	closed bool
}

func (h *mirrorTestHost) Ready(ctx context.Context) (*pb.ReadyResponse, error) {
	return h.Server.Ready(ctx, nil)
}
func (h *mirrorTestHost) MirrorFrame(_ context.Context, frame *pb.MirrorRequest) error {
	return h.Apply(frame)
}
func (h *mirrorTestHost) Close() error { h.closed = true; return nil }
func newMirrorTestHost(t *testing.T, commit string) *mirrorTestHost {
	t.Helper()
	s, e := NewServer(commit, "digest", nil)
	if e != nil {
		t.Fatal(e)
	}
	return &mirrorTestHost{Server: s}
}
func TestMirrorCandidateBarrier(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		m := NewManager(time.Second)
		defer m.Close()
		if err := m.Publish(ctx, sdktest.Snapshot{NamespaceName: "toy"}); err != nil {
			t.Fatal(err)
		}
		one := newMirrorTestHost(t, "one")
		if err := m.Prepare(ctx, one); err != nil {
			t.Fatal(err)
		}
		if err := m.Activate(ctx, one, "one", "digest"); err != nil {
			t.Fatal(err)
		}
		view := sdktest.Snapshot{NamespaceName: "toy", PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "toy"}}}, SecretList: []sdk.SecretMetadata{{Name: "secret", Namespace: "toy"}}}
		leased, seq, release, err := m.Capture(ctx, view)
		if err != nil {
			t.Fatal(err)
		}
		if leased.(*capturedHost).Host != one || seq != 2 {
			t.Fatalf("capture: %v %d", leased, seq)
		}
		release()
		two := newMirrorTestHost(t, "two")
		if err = m.Prepare(ctx, two); err != nil {
			t.Fatal(err)
		}
		view.PodList = nil
		_, _, release, err = m.Capture(ctx, view)
		if err != nil {
			t.Fatal(err)
		}
		release()
		if err = m.Activate(ctx, two, "two", "digest"); err != nil {
			t.Fatal(err)
		}
		if !one.closed {
			t.Fatal("old host not retired")
		}
		got, err := two.snapshot(2)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Pods()) != 0 || len(got.Secrets()) != 1 {
			t.Fatalf("candidate did not catch deletion: %+v", got)
		}
		bad := newMirrorTestHost(t, "bad")
		if err = m.Prepare(ctx, bad); err != nil {
			t.Fatal(err)
		}
		if err = m.Activate(ctx, bad, "wrong", "digest"); err == nil {
			t.Fatal("accepted wrong revision")
		}
		if !bad.closed {
			t.Fatal("candidate leaked")
		}
		h, _, release, err := m.Capture(ctx, view)
		if err != nil {
			t.Fatal(err)
		}
		release()
		if h.(*capturedHost).Host != two {
			t.Fatal("failed candidate replaced active")
		}
	})
}

func TestCapturedMirrorLeaseSurvivesRetirement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		m := NewManager(time.Second)
		defer m.Close()
		view := sdktest.Snapshot{NamespaceName: "toy"}
		if err := m.Publish(ctx, view); err != nil {
			t.Fatal(err)
		}
		old := newMirrorTestHost(t, "old")
		next := newMirrorTestHost(t, "next")
		if err := m.Prepare(ctx, old); err != nil {
			t.Fatal(err)
		}
		if err := m.Activate(ctx, old, "old", "digest"); err != nil {
			t.Fatal(err)
		}
		if err := m.Prepare(ctx, next); err != nil {
			t.Fatal(err)
		}
		leased, seq, release, err := m.Capture(ctx, view)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		started := make(chan struct{})
		go func() { close(started); done <- m.Activate(ctx, next, "next", "digest") }()
		<-started
		if old.closed {
			t.Fatal("retirement killed a leased call")
		}
		if _, err := leased.Evaluate(ctx, &pb.EvaluateRequest{MirrorSequence: seq}); err != nil {
			t.Fatal(err)
		}
		release()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if !old.closed {
			t.Fatal("retired host leaked")
		}
	})
}

func TestMirrorAllSnapshotKindsAndDeletes(t *testing.T) {
	fields := map[string]any{"namespace": "toy"}
	for kind, field := range snapshotFields {
		object := map[string]any{"apiVersion": kindAPIVersion(kind), "kind": kind, "metadata": map[string]any{"name": "synthetic", "namespace": "toy"}}
		if kind == "Node" {
			object["metadata"] = map[string]any{"name": "synthetic"}
		}
		if kind == "Secret" {
			object = map[string]any{"name": "synthetic", "namespace": "toy", "type": "Opaque", "dataKeys": []any{"canary-key"}}
		}
		fields[field] = []any{object}
	}
	data := capturefixture.Raw(t, "mirror coverage feeds every informer kind as a raw view, not a capture boundary", func() []byte {
		raw, err := json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}())
	var view sdktest.Snapshot
	var err error
	if err = json.Unmarshal(data, &view); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	m := NewManager(time.Second)
	defer m.Close()
	if err = m.Publish(ctx, view); err != nil {
		t.Fatal(err)
	}
	host := newMirrorTestHost(t, "all")
	if err = m.Prepare(ctx, host); err != nil {
		t.Fatal(err)
	}
	if err = m.Activate(ctx, host, "all", "digest"); err != nil {
		t.Fatal(err)
	}
	got, err := host.snapshot(1)
	if err != nil {
		t.Fatal(err)
	}
	objects, err := mirrorObjects(got)
	if err != nil || len(objects) != len(snapshotFields) {
		t.Fatal("lost snapshot kinds", len(objects), err)
	}
	if err = m.Publish(ctx, sdktest.Snapshot{NamespaceName: "toy"}); err != nil {
		t.Fatal(err)
	}
	got, err = host.snapshot(2)
	if err != nil {
		t.Fatal(err)
	}
	objects, err = mirrorObjects(got)
	if err != nil || len(objects) != 0 {
		t.Fatal("deletions lost", len(objects), err)
	}
	if err = m.Publish(ctx, sdktest.Snapshot{NamespaceName: "foreign"}); err == nil {
		t.Fatal("namespace isolation lost")
	}
}

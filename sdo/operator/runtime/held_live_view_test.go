package runtime

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/servicehealth"
)

type absentSource struct{}

func (absentSource) AcceptedManifest(context.Context, string, sdk.ObjectRef) (*unstructured.Unstructured, error) {
	return nil, errors.New("not in accepted source")
}
func (absentSource) Revision(context.Context, sdk.ObjectRef, string) (*unstructured.Unstructured, error) {
	return nil, errors.New("not in accepted source")
}

const liveViewCommit = "accepted-commit"

var liveViewNow = time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)

func liveViewFor(snapshot sdk.DetectionContext) program.ReadView {
	return LiveReadView{Carrier: inputs.New(snapshot), Context: context.Background(), Source: absentSource{}, SourceCommit: liveViewCommit}
}

// requireLiveView is the class guard: whatever the controller hands to drift
// classification, detectors or the routing sweep must still be the live read
// view built last, with the accepted source and commit it was built with. A
// decorator that flattens it into plain observation inputs fails here.
func requireLiveView(t *testing.T, where string, view sdk.DetectionContext) {
	t.Helper()
	read, ok := view.(program.ReadView)
	if !ok {
		t.Fatalf("%s: snapshot is %T, not a read view", where, view)
	}
	live, ok := routingLiveView(read)
	if !ok {
		t.Fatalf("%s: %T does not unwrap to the live read view (routing_probe=unavailable:read_view_not_live)", where, read)
	}
	if live.Source == nil || live.SourceCommit != liveViewCommit {
		t.Fatalf("%s: live view lost its accepted source: source=%v commit=%q", where, live.Source, live.SourceCommit)
	}
	if _, err := read.DiffAgainstSource(sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}); err != nil && strings.Contains(err.Error(), "accepted source is unavailable") {
		t.Fatalf("%s: %v", where, err)
	}
}

// While an incident is open the held keys and the install reachability
// reference decorate the snapshot, and the live read view must survive them.
func TestOpenIncidentSnapshotsKeepTheLiveReadView(t *testing.T) {
	reference := &servicehealth.ReachabilityBaseline{}
	base := sdktest.Snapshot{NamespaceName: "app", Reachability: &servicehealth.ReachabilityWindow{}}
	provider := ReadSnapshotProvider{Base: fixedProvider{base}, View: liveViewFor}
	c, err := NewController(testControllerConfig(), nil, provider, &recordingDispatcher{}, liveViewNow)
	if err != nil {
		t.Fatal(err)
	}
	c.attachOnboardingReference(reference)
	var reached []sdk.DetectionContext
	c.ConfigureDrift(DriftControllerConfig{View: func(snapshot sdk.DetectionContext) program.ReadView {
		reached = append(reached, snapshot)
		requireLiveView(t, "processDrift", snapshot)
		return snapshot.(program.ReadView)
	}})
	check := func(label string, wantHeld bool) {
		t.Helper()
		snapshot, err := c.provider.Snapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		requireLiveView(t, label, snapshot)
		if got := len(sdk.HeldFingerprints(snapshot, "request-metrics")) == 1; got != wantHeld {
			t.Fatalf("%s: held keys carried = %v, want %v", label, got, wantHeld)
		}
		if window, ok := servicehealth.ReachabilityFrom(snapshot); !ok || window.Baseline != reference {
			t.Fatalf("%s: install reachability reference not carried (ok=%v)", label, ok)
		}
		if memoryCycle(snapshot) == nil {
			t.Fatalf("%s: memory host edge cannot reach the cycle view", label)
		}
		c.processDrift(context.Background(), liveViewNow, snapshot, []ObjectChange{{New: &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"namespace": "app", "name": "web"}}}}})
	}
	check("no incident", false)

	c.incidentOpen = true
	c.currentIncidentRequest = &IncidentRequest{OpeningKeys: []string{FindingStateKey("request-metrics", "requestmetrics/errors/op")}}
	c.publishHeldLocked()
	check("incident open", true)

	// A memory activation swaps the provider; the decorations must follow it.
	next := ReadSnapshotProvider{Base: fixedProvider{base}, View: liveViewFor}
	if err := c.ReconfigureMemory(&RemoteMemory{}, next, liveViewNow); err != nil {
		t.Fatal(err)
	}
	check("incident open after memory activation", true)
	if len(reached) != 3 {
		t.Fatalf("drift saw %d views, want 3", len(reached))
	}
}

// A provider that already built the read view must not be wrapped by the held
// keys: that would flatten it, so the composition error is loud.
func TestHeldKeysRefuseToFlattenABuiltReadView(t *testing.T) {
	var held atomicKeys
	held.Store(&[]string{FindingStateKey("request-metrics", "requestmetrics/errors/op")})
	built := ReadSnapshotProvider{Base: fixedProvider{sdktest.Snapshot{NamespaceName: "app"}}, View: liveViewFor}
	if _, err := (heldSnapshotProvider{Base: built, held: &held}).Snapshot(context.Background()); err == nil {
		t.Fatal("held keys wrapped a built read view")
	}
}

type atomicKeys = atomic.Pointer[[]string]

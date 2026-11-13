package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memoryhost"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/appstate/carried"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/sdktest"
)

// readerDetector builds the controller's proxy of a registered reader.
func readerDetector(t *testing.T, id string) sdk.Detector {
	t.Helper()
	remote := &RemoteMemory{}
	return &remoteDetector{Registration: memoryhost.Registration{DetectorSpec: sdk.DetectorSpec{ID: id, Class: sdk.DetectorClassHealth}, AppStateReader: true}, remote: remote}
}

func viewWith(results map[string]carried.Result) sdk.DetectionContext {
	return inputs.New(sdktest.Snapshot{NamespaceName: "app"}).WithAppState(appStateResults(results))
}

func TestFailingReaderSurfacesAsAnInfoClueNotAnError(t *testing.T) {
	d := readerDetector(t, "settings")
	failed := carried.Result{Reader: "settings", Recent: []carried.Snapshot{{Error: "store down"}}}
	findings, err := d.Detect(context.Background(), viewWith(map[string]carried.Result{"settings": failed}))
	if err != nil {
		t.Fatalf("a failing reader must not be a detector error: %v", err)
	}
	if len(findings) != 1 || findings[0].Severity != sdk.SeverityInfo || findings[0].Status != sdk.FindingActive || findings[0].DetectorID != "settings" {
		t.Fatalf("findings = %+v", findings)
	}
}

func TestHealthyOrNotYetReadReaderReportsNothing(t *testing.T) {
	d := readerDetector(t, "settings")
	for name, view := range map[string]sdk.DetectionContext{
		"no result yet": viewWith(nil),
		"healthy":       viewWith(map[string]carried.Result{"settings": {Recent: []carried.Snapshot{{}}}}),
	} {
		if findings, err := d.Detect(context.Background(), view); err != nil || len(findings) != 0 {
			t.Errorf("%s: findings=%v err=%v", name, findings, err)
		}
	}
}

func TestAppStateFindingsAreCluesNeverOpeners(t *testing.T) {
	c := &Controller{}
	for _, id := range []string{"settings", "any-consumer-of-app-state"} {
		finding := sdk.Finding{DetectorID: id, RuleID: "app-state.read-failed", Status: sdk.FindingActive, Severity: sdk.SeverityInfo}
		if c.opens(finding) {
			t.Errorf("a finding of %q opens an incident", id)
		}
	}
}

func TestObservationCarriesTheReadersADetectorConsumes(t *testing.T) {
	consumer := memoryhost.Registration{DetectorSpec: sdk.DetectorSpec{ID: "uses-settings"}, AppStateReaders: []string{"settings"}}
	remote := &RemoteMemory{registrations: []memoryhost.Registration{consumer}}
	view := viewWith(map[string]carried.Result{"settings": {Reader: "settings"}, "unconsumed": {Reader: "unconsumed"}})
	observation, err := remote.buildObservation(view)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := observation.AppState["settings"]; !ok || len(observation.AppState) != 1 {
		t.Fatalf("app_state = %v, want only the consumed reader", observation.AppState)
	}
}

func TestRemoteReadStateDecodesValuesAndFailures(t *testing.T) {
	rpc := &stateRPC{response: &pb.ReadStateResponse{Values: map[string]string{"a": "1"}}}
	remote := &RemoteMemory{rpc: rpc}
	values, err := remote.ReadState(context.Background(), "settings")
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := values.Get("a"); v != "1" || rpc.asked != "settings" {
		t.Fatalf("values=%v asked=%q", values, rpc.asked)
	}
	rpc.response = &pb.ReadStateResponse{Error: "store down"}
	if _, err := remote.ReadState(context.Background(), "settings"); err == nil || err.Error() != "store down" {
		t.Fatalf("err = %v", err)
	}
	rpc.response = &pb.ReadStateResponse{Values: map[string]string{"admin_password": "x"}}
	values, err = remote.ReadState(context.Background(), "settings")
	if err != nil {
		t.Fatalf("prototype: no secret filtering, got %v", err)
	}
	if v, _ := values.Get("admin_password"); v != "x" {
		t.Fatalf("values = %v", values)
	}
}

type stateRPC struct {
	MirroredMemoryRPC
	response *pb.ReadStateResponse
	asked    string
}

func (s *stateRPC) ReadState(_ context.Context, r *pb.ReadStateRequest) (*pb.ReadStateResponse, error) {
	s.asked = r.ReaderId
	return s.response, nil
}

func TestControllerFeedsHealthToTheAppStateStore(t *testing.T) {
	store := NewAppStateStore(DefaultStateSettle)
	store.Retain(map[string][]string{"r": nil})
	c := &Controller{AppState: store}
	c.observeAppState(storeT0, true)
	store.Record("r", stateSnap(time.Second, "a", "1"))
	store.Record("r", stateSnap(DefaultStateSettle, "a", "1"))
	c.observeAppState(storeT0.Add(DefaultStateSettle), true)
	if r, _ := store.Result("r"); r.Healthy == nil {
		t.Fatal("a stretch healthy for the settle window must promote last-healthy")
	}
}

func TestObservationOfMaxedOutReadersStaysUnderTheWireLimit(t *testing.T) {
	big := func(at int) carried.Snapshot {
		var v carried.Values
		for i := 0; i < carried.MaxKeys; i++ {
			v.Set(strings.Repeat("k", 20)+string(rune('a'+i%26))+string(rune('a'+i/26)), strings.Repeat("v", carried.MaxValueBytes))
		}
		return carried.Snapshot{At: storeT0.Add(time.Duration(at) * time.Minute), Values: v}
	}
	var ring []carried.Snapshot
	for i := 0; i < 3; i++ {
		ring = append(ring, big(i))
	}
	healthy := big(-1)
	full := carried.Result{Reader: "r", Recent: ring, Healthy: &healthy, HealthyHistory: ring}
	registrations := []memoryhost.Registration{{DetectorSpec: sdk.DetectorSpec{ID: "uses"}}}
	results := map[string]carried.Result{}
	for i := 0; i < carried.MaxReaders; i++ {
		id := "r" + string(rune('a'+i))
		results[id] = full
		registrations[0].AppStateReaders = append(registrations[0].AppStateReaders, id)
	}
	remote := &RemoteMemory{registrations: registrations}
	observation, err := remote.buildObservation(viewWith(results))
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, v := range observation.AppState {
		raw, _ := v.MarshalJSON()
		total += len(raw)
	}
	if total > 8<<20 {
		t.Fatalf("app_state is %d bytes for 16 readers, far over the 16 MiB message limit's share", total)
	}
	for id, v := range observation.AppState {
		raw, _ := v.MarshalJSON()
		if !strings.Contains(string(raw), `"recent"`) {
			t.Fatalf("%s lost its newest snapshot", id)
		}
	}
}

package memoryhost

import (
	"context"
	"google.golang.org/protobuf/types/known/structpb"
	"testing"

	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/appstate"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// stateReader is a health detector that is also an app-state reader.
type stateReader struct {
	id     string
	values map[string]string
	noisy  []string
}

func (r stateReader) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: r.id, Class: sdk.DetectorClassHealth}
}
func (stateReader) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return nil, nil
}
func (r stateReader) NoisyKeys() []string { return r.noisy }
func (r stateReader) ReadState(_ context.Context, view appstate.View) (appstate.Values, error) {
	var out appstate.Values
	obs, err := view.Read(program.Query{Probe: &program.ProbeQuery{
		To:  sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: view.Namespace(), Name: "admin"},
		Req: program.HTTPProbe{Method: "GET", Path: "/settings", Fields: map[string]string{"limit": "/limit"}},
	}})
	if err != nil {
		return out, err
	}
	out.Set("limit", obs.Rows[0]["limit"].(string))
	for k, v := range r.values {
		out.Set(k, v)
	}
	return out, nil
}

type liveReads struct {
	program.ReadView
	namespace string
}

func (l liveReads) Read(program.Query) (program.Observation, error) {
	return program.Observation{Rows: []program.Row{{"limit": "10"}}}, nil
}

func readStateServer(t *testing.T, detectors ...sdk.Detector) *Server {
	t.Helper()
	server, err := NewServer("accepted", "binary", detectors)
	if err != nil {
		t.Fatal(err)
	}
	server.WithReadFactory(func(_ context.Context, s sdktest.Snapshot) (program.ReadView, error) {
		return liveReads{ReadView: program.ReadFrom(s)}, nil
	})
	if err := server.Apply(&pb.MirrorRequest{Namespace: "app", Sequence: 1, Complete: true, ResetMirror: true}); err != nil {
		t.Fatal(err)
	}
	return server
}

func TestReadStateRunsTheReaderOverTheLiveReadFactory(t *testing.T) {
	server := readStateServer(t, stateReader{id: "settings", values: map[string]string{"mode": "a"}})
	response, err := server.ReadState(context.Background(), &pb.ReadStateRequest{ReaderId: "settings"})
	if err != nil || response.Error != "" {
		t.Fatalf("response=%v err=%v", response, err)
	}
	if response.Values["limit"] != "10" || response.Values["mode"] != "a" || len(response.Values) != 2 {
		t.Fatalf("values = %v", response.Values)
	}
}

func TestReadStateFailuresAreDataNotTransportErrors(t *testing.T) {
	server := readStateServer(t,
		stateReader{id: "leaky", values: map[string]string{"admin_password": "x"}},
		testDetector{})
	for _, id := range []string{"leaky", "synthetic", "absent"} {
		response, err := server.ReadState(context.Background(), &pb.ReadStateRequest{ReaderId: id})
		if err != nil || response.Error == "" || len(response.Values) != 0 {
			t.Errorf("%s: response=%v err=%v, want an error string and no values", id, response, err)
		}
	}
}

func TestReadStateNeedsAMirrorAndAReadFactory(t *testing.T) {
	server, err := NewServer("accepted", "binary", []sdk.Detector{stateReader{id: "settings"}})
	if err != nil {
		t.Fatal(err)
	}
	if r, err := server.ReadState(context.Background(), &pb.ReadStateRequest{ReaderId: "settings"}); err != nil || r.Error == "" {
		t.Fatalf("no mirror: %v %v", r, err)
	}
}

func TestRegistrationNamesReadersAndNoisyKeys(t *testing.T) {
	r := registration(stateReader{id: "settings", noisy: []string{"jitter*"}})
	if !r.AppStateReader || len(r.AppStateNoisy) != 1 {
		t.Fatalf("registration = %+v", r)
	}
	if registration(testDetector{}).AppStateReader {
		t.Fatal("an ordinary detector is not a reader")
	}
}

func TestObservationDecodesAppStateIntoTheSnapshot(t *testing.T) {
	var v appstate.Values
	v.Set("limit", "10")
	raw, err := encodeStruct(appstate.Result{Reader: "settings", Recent: []appstate.Snapshot{{Values: v}}})
	if err != nil {
		t.Fatal(err)
	}
	var snapshot sdktest.Snapshot
	if err := observe(&snapshot, &pb.MemoryObservation{AppState: map[string]*structpb.Struct{"settings": raw}}); err != nil {
		t.Fatal(err)
	}
	got, ok := snapshot.AppStateResult("settings")
	if !ok || len(got.Recent) != 1 {
		t.Fatalf("snapshot app state = %+v", snapshot.AppState)
	}
	if limit, _ := got.Recent[0].Values.Get("limit"); limit != "10" {
		t.Fatalf("limit = %q", limit)
	}
}

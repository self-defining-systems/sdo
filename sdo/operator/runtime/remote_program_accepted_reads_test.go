package runtime

import (
	"context"
	"fmt"
	"testing"

	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memoryhost"
	"sdo.dev/operator/memoryhost/hostio"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// acceptedReadsHost serves remote program RPCs from an in-process memory host
// wired with the production live read factory.
type acceptedReadsHost struct{ *memoryhost.Server }

func (acceptedReadsHost) MemoryCommit() string { return "accepted" }
func (acceptedReadsHost) Close() error         { return nil }
func (h acceptedReadsHost) Ready(ctx context.Context) (*pb.ReadyResponse, error) {
	return h.Server.Ready(ctx, &pb.ReadyRequest{})
}
func (h acceptedReadsHost) Capture(context.Context, sdk.DetectionContext) (memoryhost.Host, uint64, func(), error) {
	return h, 1, func() {}, nil
}

// acceptedReadsProgram reads accepted memory, as a learned program that
// refuses to touch a workload marked holds_data does.
type acceptedReadsProgram struct{}

func (acceptedReadsProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "accepted-reads", Params: []program.ParamSpec{{Name: "workload", Kind: "Deployment", Required: true}}}
}

func (acceptedReadsProgram) reads(v program.ReadView) (string, error) {
	appMap, err := v.AppMap()
	if err != nil {
		return "", err
	}
	checks, err := v.DataChecks()
	if err != nil {
		return "", err
	}
	if len(appMap.Components) != 1 || checks["synthetic-check"] != "query: one" {
		return "", fmt.Errorf("accepted memory differs: %+v %v", appMap, checks)
	}
	return appMap.Components[0].Workload, nil
}

func (p acceptedReadsProgram) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	if _, err := p.reads(v); err != nil {
		return program.Verdict{}, err
	}
	return program.Verdict{Result: program.Match, Evidence: []program.Evidence{{Fact: "accepted memory read"}}}, nil
}

func (p acceptedReadsProgram) Mitigate(_ context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	workload, err := p.reads(v)
	if err != nil {
		return program.Plan{}, err
	}
	return program.Plan{Rationale: "restart " + workload, Actions: []program.Action{program.RestartRollout{Ref: b.Objects["workload"], Reason: "synthetic"}}}, nil
}

// A learned program run by the memory host on a live cluster reads the
// accepted application map and data checks the controller holds; neither read
// may fall through to an empty replay recording.
func TestRemoteProgramReadsAcceptedMemoryOnTheLiveHost(t *testing.T) {
	server, err := memoryhost.NewServer("accepted", "binary", nil)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := program.NewRegistry([]program.Diagnoser{acceptedReadsProgram{}}, []program.Mitigator{acceptedReadsProgram{}})
	if err != nil {
		t.Fatal(err)
	}
	server.WithPrograms(registry).WithReadFactory(func(ctx context.Context, snapshot sdktest.Snapshot) (program.ReadView, error) {
		return hostio.NewLiveReadView(ctx, snapshot, hostio.ReadBackends{}), nil
	})
	if err := server.Apply(&pb.MirrorRequest{Namespace: "app", Sequence: 1, Complete: true, ResetMirror: true}); err != nil {
		t.Fatal(err)
	}
	remote := &RemoteMemory{rpc: acceptedReadsHost{server}, memoryCommit: "accepted"}
	p := &remoteProgram{spec: acceptedReadsProgram{}.Spec(), remote: remote}
	controller := sdktest.Snapshot{
		NamespaceName:      "app",
		ApplicationMap:     &program.AppMap{Components: []program.AppComponent{{Name: "store", Workload: "Deployment/store"}}},
		AcceptedDataChecks: map[string]string{"synthetic-check": "query: one"},
	}
	bindings := program.Bindings{Objects: program.ObjectBindings{"workload": {APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "store"}}}
	// The responder loop's views are per-cycle, as in production.
	for name, view := range map[string]program.ReadView{
		"cycle":     NewCycleReadView(program.ReadFrom(controller)),
		"recording": &sdktest.RecordingReadView{ReadView: NewCycleReadView(program.ReadFrom(controller))},
	} {
		t.Run(name, func(t *testing.T) {
			if verdict, err := p.Diagnose(context.Background(), view, bindings); err != nil || verdict.Result != program.Match {
				t.Fatalf("remote diagnose = %+v, %v", verdict, err)
			}
			plan, err := p.Mitigate(context.Background(), view, bindings)
			if err != nil {
				t.Fatalf("remote mitigate: %v", err)
			}
			if plan.Rationale != "restart Deployment/store" {
				t.Fatalf("plan rationale = %q", plan.Rationale)
			}
		})
	}
}

package runtime

import (
	"context"
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/memoryhost"
	"sdo.dev/operator/runtime/sim"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
)

// sourceDiffProgram is a learned-style program: it diagnoses and plans from
// DiffAgainstSource, which the memory host can only answer from diffs the
// controller froze for it.
type sourceDiffProgram struct{}

func (sourceDiffProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "source-diff", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}
}
func (sourceDiffProgram) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	diff, err := v.DiffAgainstSource(b.Objects["service"])
	if err != nil {
		return program.Verdict{Result: program.Unknown}, err
	}
	if diff.Changed {
		return program.Verdict{Result: program.Match}, nil
	}
	return program.Verdict{Result: program.NoMatch}, nil
}
func (sourceDiffProgram) Mitigate(_ context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	ref := b.Objects["service"]
	diff, err := v.DiffAgainstSource(ref)
	if err != nil {
		return program.Plan{}, err
	}
	if !diff.Changed {
		return program.Plan{}, errors.New("nothing differs from source")
	}
	return program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "spec.ports[0].targetPort", Value: program.FromSource(ref, "spec.ports[0].targetPort"), Reason: "restore accepted port"}}}, nil
}

// simSourceDiffWorld is one Service whose accepted source declares port 8080
// and targetPort 8080. The Service carries its acceptance record, and a field
// the API server defaulted after that record was taken (the port protocol).
type simSourceDiffWorld struct {
	w       *sim.World
	ref     sdk.ObjectRef
	remote  *RemoteMemory
	source  *unstructured.Unstructured
	client  func() LiveReadView
	program program.Diagnoser
}

func newSimSourceDiffWorld(t *testing.T, w *sim.World, targetPort int64) *simSourceDiffWorld {
	t.Helper()
	ctx := context.Background()
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "backend"}
	manifest := func(target int64) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"name": "backend", "namespace": "app"},
			"spec": map[string]any{"selector": map[string]any{"app": "backend"}, "ports": []any{map[string]any{"port": int64(8080), "targetPort": target}}},
		}}
	}
	source := manifest(8080)
	live := manifest(targetPort)
	live.SetUID(types.UID("uid-backend"))
	if err := RecordAcceptance(live, source); err != nil {
		t.Fatal(err)
	}
	// The server defaults this port after acceptance, so the record does not list it.
	ports, _, _ := unstructured.NestedSlice(live.Object, "spec", "ports")
	ports[0].(map[string]any)["protocol"] = "TCP"
	if err := unstructured.SetNestedSlice(live.Object, ports, "spec", "ports"); err != nil {
		t.Fatal(err)
	}
	w.Kube.Namespace = "app"
	w.Kube.Put(live)

	server, err := memoryhost.NewServer("accepted", "digest", nil)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := program.NewRegistry([]program.Diagnoser{sourceDiffProgram{}}, []program.Mitigator{sourceDiffProgram{}})
	if err != nil {
		t.Fatal(err)
	}
	server.WithPrograms(registry)
	ready, _ := server.Ready(ctx, nil)
	rpc, _ := memoryTestRPC(t, server)
	remote, err := NewRemoteMemory(ready, mirroredTestHost(t, ctx, rpc, "accepted", "digest", "app"))
	if err != nil {
		t.Fatal(err)
	}
	client, err := w.Client()
	if err != nil {
		t.Fatal(err)
	}
	diagnoser, _ := remote.Programs.Diagnoser("source-diff")
	return &simSourceDiffWorld{w: w, ref: ref, remote: remote, source: source, program: diagnoser, client: func() LiveReadView {
		snapshot, err := w.Cache.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return LiveReadView{Context: ctx, Carrier: inputs.New(snapshot), Client: client, Source: planFakeSource{source: source}, SourceCommit: "accepted"}
	}}
}

func (s *simSourceDiffWorld) bindings() program.Bindings {
	return program.Bindings{Objects: program.ObjectBindings{"service": s.ref}}
}

// The controller froze the diff the program asked for, with the same
// server-side normalization an in-process program gets: API defaults are not
// drift, so a repaired Service stops matching.
func TestSimRemoteProgramDiagnoseSeesTheRepairAfterTheWrite(t *testing.T) {
	sim.Run(t, 7, func(t *testing.T, w *sim.World) {
		ctx := context.Background()
		s := newSimSourceDiffWorld(t, w, 9999)
		before := program.RunDiagnose(ctx, s.program, NewCycleReadView(s.client()), s.bindings(), DefaultDiagnosisDeadline)
		if before.Result != program.Match {
			t.Fatalf("a drifted Service must match: %+v", before)
		}
		patch := []byte(`{"spec":{"ports":[{"port":8080,"targetPort":8080,"protocol":"TCP"}]}}`)
		if err := w.Kube.Patch(ctx, s.ref, types.MergePatchType, patch); err != nil {
			t.Fatal(err)
		}
		after := program.RunDiagnose(ctx, s.program, NewCycleReadView(s.client()), s.bindings(), DefaultDiagnosisDeadline)
		if after.Result != program.NoMatch {
			t.Fatalf("a repaired Service must stop matching on a fresh snapshot: %+v", after)
		}
	})
}

// Mitigate runs against the isolated mitigation view; its program asks for the
// same source diff and must get it.
func TestSimRemoteProgramMitigateReadsSourceDiffsThroughTheMitigationView(t *testing.T) {
	sim.Run(t, 8, func(t *testing.T, w *sim.World) {
		ctx := context.Background()
		s := newSimSourceDiffWorld(t, w, 9999)
		mitigator, _ := s.remote.Programs.Mitigator("source-diff")
		isolated := newMitigationReadView(ctx, NewCycleReadView(s.client()))
		observed, failure := program.CheckReads(isolated)
		plan, err := mitigator.Mitigate(ctx, program.ReadFrom(observed), s.bindings())
		if err != nil || failure() != nil || len(plan.Actions) != 1 {
			t.Fatalf("mitigation did not get the frozen diff: plan=%+v err=%v read failure=%v", plan, err, failure())
		}
	})
}

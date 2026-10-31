package memoryhost

import (
	"context"
	"google.golang.org/protobuf/types/known/structpb"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"testing"
)

type testDetector struct{}

func (testDetector) Spec() sdk.DetectorSpec { return sdk.DetectorSpec{ID: "synthetic"} }
func (testDetector) Detect(_ context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	return []sdk.Finding{{Summary: view.Namespace()}}, nil
}
func TestMirrorRequiresContiguousSequenceAndComplete(t *testing.T) {
	s, err := NewServer("commit", "sha256:abc", []sdk.Detector{testDetector{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Apply(&pb.MirrorRequest{Sequence: 2, ResetMirror: true, Namespace: "toy"}); err == nil {
		t.Fatal("sequence gap accepted")
	}
	if err := s.Apply(&pb.MirrorRequest{Sequence: 1, ResetMirror: true, Namespace: "toy"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Evaluate(context.Background(), &pb.EvaluateRequest{MirrorSequence: 1, DetectorIds: []string{"synthetic"}}); err == nil {
		t.Fatal("incomplete mirror evaluated")
	}
	if err := s.Apply(&pb.MirrorRequest{Sequence: 2, Complete: true}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Evaluate(context.Background(), &pb.EvaluateRequest{MirrorSequence: 2, DetectorIds: []string{"synthetic"}})
	if err != nil || got.Results[0].Findings[0].Summary != "toy" {
		t.Fatal(got, err)
	}
	if _, err := s.Evaluate(context.Background(), &pb.EvaluateRequest{MirrorSequence: 1}); err == nil {
		t.Fatal("stale mirror evaluated")
	}
}

func TestMirrorCannotStartWithoutReset(t *testing.T) {
	s, _ := NewServer("c", "d", nil)
	if err := s.Apply(&pb.MirrorRequest{Sequence: 1, Complete: true}); err == nil {
		t.Fatal("mirror completed without initial namespace")
	}
}

type unsafeProgram struct{}

func (unsafeProgram) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "unsafe"} }
func (unsafeProgram) Diagnose(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {
	return program.Verdict{Result: program.NoMatch}, nil
}
func (unsafeProgram) Mitigate(ctx context.Context, v program.ReadView, _ program.Bindings) (program.Plan, error) {
	_, _ = v.ProbeTCP(sdk.ObjectRef{Kind: "Service", Name: "missing", Namespace: "toy"}, 80)
	return program.Plan{Rationale: "ignored missing primitive"}, nil
}
func TestMitigateCannotIgnoreMissingPrimitive(t *testing.T) {
	registry, err := program.NewRegistry([]program.Diagnoser{unsafeProgram{}}, []program.Mitigator{unsafeProgram{}})
	if err != nil {
		t.Fatal(err)
	}
	s, _ := NewServer("c", "d", nil)
	s.WithPrograms(registry)
	if err = s.Apply(&pb.MirrorRequest{Sequence: 1, ResetMirror: true, Namespace: "toy", Complete: true}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Mitigate(context.Background(), &pb.MitigateRequest{Input: &pb.MemoryProgramInput{ProgramId: "unsafe", MirrorSequence: 1}})
	if err != nil || got.Error == "" || got.Plan != nil {
		t.Fatalf("unavailable primitive authorized plan: %v %v", got, err)
	}
}

func TestSecretMirrorAcceptsOnlySDKMetadata(t *testing.T) {
	for _, tc := range []struct {
		fields   map[string]any
		accepted bool
	}{
		{map[string]any{"name": "secret", "namespace": "toy", "type": "Opaque", "labels": map[string]any{"app": "toy"}, "dataKeys": []any{"password"}}, true},
		{map[string]any{"name": "secret", "namespace": "toy", "data": map[string]any{"password": "value"}}, false},
	} {
		s, _ := NewServer("c", "d", nil)
		body, _ := structpb.NewStruct(tc.fields)
		err := s.Apply(&pb.MirrorRequest{Sequence: 1, ResetMirror: true, Namespace: "toy", Complete: true, Upserts: []*pb.MemoryObject{{Ref: &pb.ObjectRef{ApiVersion: "v1", Kind: "Secret", Name: "secret", Namespace: "toy"}, Object: body}}})
		if (err == nil) != tc.accepted {
			t.Fatalf("accepted=%v, error=%v", tc.accepted, err)
		}
	}
}

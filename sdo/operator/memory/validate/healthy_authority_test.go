package validate

import (
	"context"
	"google.golang.org/protobuf/proto"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestReflectionBaselineUsesControllerCaptureInsteadOfAuthoredHealthy(t *testing.T) {
	root := goodTree(t)
	commit(t, root, "base")
	write(t, root, ".sdo/diagnostics/detectors/incidents/cache/detector.go", "package cache\n"+"\n// changed\n")
	capture := t.TempDir()
	if err := os.MkdirAll(filepath.Join(capture, "healthy"), 0700); err != nil {
		t.Fatal(err)
	}
	write(t, capture, "healthy/healthy.json", `{"namespace":"app","services":[]}`)
	authored := t.TempDir()
	got, err := selectCompileBaseline(context.Background(), Request{Root: root, Base: "HEAD", Actor: Responder, Phase: ReflectionPhase, CaptureDirectory: capture, HealthyBaseline: authored})
	if err != nil || got.HealthyBaseline != filepath.Join(capture, "healthy") {
		t.Fatalf("baseline=%q err=%v, want controller-owned capture", got.HealthyBaseline, err)
	}
}

func TestSuccessfulClosureCannotAuthorizeAnUnrelatedCause(t *testing.T) {
	root := goodTree(t)
	refs := []*contracts.ObjectRef{{Kind: "Service", Namespace: "app", Name: "consul"}}
	outcome := &contracts.OutcomeRecord{Classification: "success", ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Resources: refs}}, ReflectionEligibility: &contracts.ReflectionEligibility{Causes: []*contracts.ReflectionCauseWork{{Resources: refs, Items: []*contracts.ReflectionWorkItem{{ProgramId: "base", Disposition: "covered", Reason: "verified_equivalent_repair"}}}}}}
	at := time.Unix(100, 0)
	outcome.RepairActions = []*contracts.RepairActionReceipt{{ActionId: "sdo-act-1", Success: true, Resources: refs, StartedAt: timestamppb.New(at), CompletedAt: timestamppb.New(at.Add(time.Second))}}
	outcome.FinalHealthDetectorState = []*contracts.DetectorEvaluation{{DetectorId: "objective", Status: "clear", EvaluatedAt: timestamppb.New(at.Add(2 * time.Second))}}
	outcome.Findings = []*contracts.Finding{{DetectorId: "objective", Status: "active", Fingerprint: "original-fault", PrimaryResource: &contracts.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}}}
	paths := []string{".sdo/diagnostics/detectors/incidents/cache/detector.go"}
	ds, err := reflectionExecutableDiagnostics(root, outcome, paths)
	if err != nil || len(ds) != 1 || ds[0].Rule != RuleReflectionExecutableCause {
		t.Fatalf("unrelated Consul repair authorized: %v %v", ds, err)
	}
	outcome.Findings[0].PrimaryResource = refs[0]
	ds, err = reflectionExecutableDiagnostics(root, outcome, paths)
	if err != nil || len(ds) != 0 {
		t.Fatalf("finding-bound repair rejected: %v %v", ds, err)
	}
}

func TestVerifiedCauseRequiresFreshFindingClearanceForTheLastChange(t *testing.T) {
	at := time.Unix(100, 0)
	ref := &contracts.ObjectRef{Kind: "Service", Namespace: "app", Name: "consul"}
	baseline := &contracts.OutcomeRecord{Classification: "success", ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Resources: []*contracts.ObjectRef{ref}}}, Findings: []*contracts.Finding{{DetectorId: "routing", Status: "active", Fingerprint: "opening", PrimaryResource: ref}}, RepairActions: []*contracts.RepairActionReceipt{{ActionId: "controller", Success: true, Resources: []*contracts.ObjectRef{ref}, StartedAt: timestamppb.New(at), CompletedAt: timestamppb.New(at.Add(time.Second))}}, FinalHealthDetectorState: []*contracts.DetectorEvaluation{{DetectorId: "routing", Status: "clear", EvaluatedAt: timestamppb.New(at.Add(2 * time.Second))}}}
	if len(VerifiedReflectionCauses(baseline)) != 1 {
		t.Fatal("complete finding-bound proof rejected")
	}
	for name, mutate := range map[string]func(*contracts.OutcomeRecord){
		"unrelated finding": func(o *contracts.OutcomeRecord) {
			o.Findings[0].PrimaryResource = &contracts.ObjectRef{Kind: "Service", Namespace: "app", Name: "other"}
		},
		"no opening finding": func(o *contracts.OutcomeRecord) { o.Findings = nil },
		"failed mutation":    func(o *contracts.OutcomeRecord) { o.RepairActions[0].Success = false },
		"missing clearance":  func(o *contracts.OutcomeRecord) { o.FinalHealthDetectorState = nil },
		"stale clearance":    func(o *contracts.OutcomeRecord) { o.FinalHealthDetectorState[0].EvaluatedAt = timestamppb.New(at) },
		"detector error":     func(o *contracts.OutcomeRecord) { s := "unavailable"; o.FinalHealthDetectorState[0].Error = &s },
		"still firing": func(o *contracts.OutcomeRecord) {
			o.FinalHealthDetectorState[0].Status = "firing"
			o.FinalHealthDetectorState[0].Fingerprints = []string{"opening"}
		},
		"later mutation": func(o *contracts.OutcomeRecord) {
			action := proto.Clone(o.RepairActions[0]).(*contracts.RepairActionReceipt)
			action.CompletedAt = timestamppb.New(at.Add(3 * time.Second))
			o.RepairActions = append(o.RepairActions, action)
		},
		"ambiguous same-object causes": func(o *contracts.OutcomeRecord) {
			o.ConfirmedRootCauses = append(o.ConfirmedRootCauses, proto.Clone(o.ConfirmedRootCauses[0]).(*contracts.ConfirmedRootCause))
		},
	} {
		t.Run(name, func(t *testing.T) {
			outcome := proto.Clone(baseline).(*contracts.OutcomeRecord)
			mutate(outcome)
			if got := VerifiedReflectionCauses(outcome); len(got) != 0 {
				t.Fatalf("unverified cause authorized: %v", got)
			}
		})
	}
	baseline.DetectorHistory = baseline.FinalHealthDetectorState
	baseline.FinalHealthDetectorState = nil
	if len(VerifiedReflectionCauses(baseline)) != 1 {
		t.Fatal("post-response incident detector history ignored")
	}
}

func TestReflectionCannotSubstituteAuthoredHealthyForMissingControllerCapture(t *testing.T) {
	root := goodTree(t)
	commit(t, root, "base")
	write(t, root, ".sdo/diagnostics/detectors/incidents/cache/detector.go", "package cache\n")
	_, err := selectCompileBaseline(context.Background(), Request{Root: root, Base: "HEAD", Actor: Responder, Phase: ReflectionPhase, HealthyBaseline: t.TempDir()})
	failure, ok := err.(*Failure)
	if !ok || len(failure.Diagnostics) != 1 || failure.Diagnostics[0].Rule != RuleDetectorHealthyBaseline {
		t.Fatalf("missing reference did not fail once/actionably: %v", err)
	}
}

func TestMissingHealthyReferenceDiagnosticIsTransportNeutral(t *testing.T) {
	root := goodTree(t)
	commit(t, root, "base")
	write(t, root, ".sdo/diagnostics/detectors/incidents/cache/detector.go", "package cache\n")
	var message string
	for _, capture := range []string{filepath.Join(t.TempDir(), "controller-session"), filepath.Join(t.TempDir(), "worker-readonly-mount")} {
		_, err := selectCompileBaseline(context.Background(), Request{Root: root, Base: "HEAD", Actor: Responder, Phase: ReflectionPhase, CaptureDirectory: capture})
		failure, ok := err.(*Failure)
		if !ok || len(failure.Diagnostics) != 1 {
			t.Fatalf("unexpected diagnostic: %v", err)
		}
		if message != "" && message != failure.Diagnostics[0].Message {
			t.Fatalf("physical mount paths changed the shared diagnostic: %q vs %q", message, failure.Diagnostics[0].Message)
		}
		message = failure.Diagnostics[0].Message
	}
}

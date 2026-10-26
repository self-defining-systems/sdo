package validate

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestVerifiedCauseAllowsObservedRecoveredSymptomWithoutWrite(t *testing.T) {
	at := time.Unix(100, 0)
	deployment := &contracts.ObjectRef{Kind: "Deployment", Namespace: "app", Name: "api"}
	service := &contracts.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}
	outcome := &contracts.OutcomeRecord{
		Classification:      "success",
		ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Resources: []*contracts.ObjectRef{deployment, service}}},
		Findings:            []*contracts.Finding{{DetectorId: "workload", Status: "active", Fingerprint: "deployment", PrimaryResource: deployment}, {DetectorId: "endpoints", Status: "active", Fingerprint: "service", PrimaryResource: service}},
		RepairActions:       []*contracts.RepairActionReceipt{{Success: true, Resources: []*contracts.ObjectRef{deployment}, StartedAt: timestamppb.New(at), CompletedAt: timestamppb.New(at.Add(time.Second))}},
		DetectorHistory:     []*contracts.DetectorEvaluation{{DetectorId: "workload", Status: "clear", EvaluatedAt: timestamppb.New(at.Add(2 * time.Second))}, {DetectorId: "endpoints", Status: "clear", EvaluatedAt: timestamppb.New(at.Add(2 * time.Second))}},
	}
	if got := VerifiedReflectionCauses(outcome); len(got) != 1 || len(got[0].Resources) != 2 {
		t.Fatalf("observed symptom disqualified hand repair: %v", got)
	}
	related := proto.Clone(outcome).(*contracts.OutcomeRecord)
	related.Findings = related.Findings[:1]
	related.Findings[0].RelatedResources = []*contracts.ObjectRef{service}
	if got := VerifiedReflectionCauses(related); len(got) != 1 {
		t.Fatalf("opening related symptom was not observed: %v", got)
	}
	unobserved := proto.Clone(outcome).(*contracts.OutcomeRecord)
	unobserved.Findings = unobserved.Findings[:1]
	if got := VerifiedReflectionCauses(unobserved); len(got) != 1 {
		t.Fatalf("symptom without an opening finding vetoed the observed repair: %v", got)
	}
	for name, change := range map[string]func(*contracts.OutcomeRecord){
		"missing symptom clearance": func(o *contracts.OutcomeRecord) { o.DetectorHistory = o.DetectorHistory[:1] },
		"stale symptom clearance":   func(o *contracts.OutcomeRecord) { o.DetectorHistory[1].EvaluatedAt = timestamppb.New(at) },
		"still broken symptom": func(o *contracts.OutcomeRecord) {
			o.DetectorHistory[1].Status = "firing"
			o.DetectorHistory[1].Fingerprints = []string{"service"}
		},
		"related clearance cannot hide primary symptom": func(o *contracts.OutcomeRecord) {
			o.Findings[0].RelatedResources = []*contracts.ObjectRef{service}
			o.DetectorHistory[1].Status = "firing"
			o.DetectorHistory[1].Fingerprints = []string{"service"}
		},
		"no repair":        func(o *contracts.OutcomeRecord) { o.RepairActions = nil },
		"unobserved write": func(o *contracts.OutcomeRecord) { o.Findings = o.Findings[1:] },
		"written target only related": func(o *contracts.OutcomeRecord) {
			o.Findings[0].PrimaryResource = &contracts.ObjectRef{Kind: "Deployment", Namespace: "app", Name: "other"}
			o.Findings[0].RelatedResources = []*contracts.ObjectRef{deployment}
		},
	} {
		t.Run(name, func(t *testing.T) {
			o := proto.Clone(outcome).(*contracts.OutcomeRecord)
			change(o)
			if got := VerifiedReflectionCauses(o); len(got) != 0 {
				t.Fatalf("unproven cause authorized: %v", got)
			}
		})
	}
}

// Shape of the h4 repeat outcome: the responder named the repaired Deployment,
// its observed Service and a traffic-only frontend Service no finding named.
func TestVerifiedCauseTreatsFindinglessSymptomAsContext(t *testing.T) {
	at := time.Unix(100, 0)
	deployment := &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "hotel", Name: "search"}
	service := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "hotel", Name: "search"}
	frontend := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "hotel", Name: "frontend"}
	outcome := &contracts.OutcomeRecord{
		Classification:      "success",
		ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Resources: []*contracts.ObjectRef{deployment, service, frontend}}},
		Findings: []*contracts.Finding{
			{DetectorId: "health-objective", Status: "active", Fingerprint: "deployment", PrimaryResource: deployment},
			{DetectorId: "service-endpoints", Status: "active", Fingerprint: "service", PrimaryResource: service},
		},
		RepairActions:   []*contracts.RepairActionReceipt{{Success: true, Resources: []*contracts.ObjectRef{deployment}, StartedAt: timestamppb.New(at), CompletedAt: timestamppb.New(at.Add(time.Second))}},
		DetectorHistory: []*contracts.DetectorEvaluation{{DetectorId: "health-objective", Status: "clear", EvaluatedAt: timestamppb.New(at.Add(2 * time.Second))}, {DetectorId: "service-endpoints", Status: "clear", EvaluatedAt: timestamppb.New(at.Add(2 * time.Second))}},
	}
	if got := VerifiedReflectionCauses(outcome); len(got) != 1 || len(got[0].Resources) != 3 {
		t.Fatalf("finding-less symptom vetoed the verified repair: %v", got)
	}
	for name, change := range map[string]func(*contracts.OutcomeRecord){
		"observed symptom still firing": func(o *contracts.OutcomeRecord) {
			o.DetectorHistory[1].Status = "firing"
			o.DetectorHistory[1].Fingerprints = []string{"service"}
		},
		"frontend observed but never cleared": func(o *contracts.OutcomeRecord) {
			o.Findings = append(o.Findings, &contracts.Finding{DetectorId: "traffic-health", Status: "active", Fingerprint: "frontend", PrimaryResource: frontend})
		},
		"frontend observed as related but never cleared": func(o *contracts.OutcomeRecord) {
			o.Findings = append(o.Findings, &contracts.Finding{DetectorId: "traffic-health", Status: "active", Fingerprint: "traffic", PrimaryResource: &contracts.ObjectRef{Kind: "Service", Namespace: "hotel", Name: "gateway"}, RelatedResources: []*contracts.ObjectRef{frontend}})
		},
		"written target without a finding": func(o *contracts.OutcomeRecord) { o.Findings = o.Findings[1:] },
		"only the finding-less symptom written": func(o *contracts.OutcomeRecord) {
			o.RepairActions[0].Resources = []*contracts.ObjectRef{frontend}
		},
	} {
		t.Run(name, func(t *testing.T) {
			o := proto.Clone(outcome).(*contracts.OutcomeRecord)
			change(o)
			if got := VerifiedReflectionCauses(o); len(got) != 0 {
				t.Fatalf("unproven cause authorized: %v", got)
			}
		})
	}
}

func TestReflectionProgramBindsRepairTargetWithoutSymptomParameter(t *testing.T) {
	root := t.TempDir()
	copyTree(t, "../../../../tests/fixtures/sdo/playbook_program_app/.sdo", filepath.Join(root, ".sdo"))
	if err := os.RemoveAll(filepath.Join(root, ".sdo/playbooks/missing-configmap")); err != nil {
		t.Fatal(err)
	}
	ref := &contracts.ObjectRef{Kind: "Service", Namespace: "demo", Name: "front"}
	symptom := &contracts.ObjectRef{Kind: "Pod", Namespace: "demo", Name: "front-1"}
	outcome := &contracts.OutcomeRecord{Classification: "success", ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Resources: []*contracts.ObjectRef{ref}}}}
	addSyntheticFindingRepairProof(outcome)
	// The second object was observed, recovered and is available in the replay
	// snapshot. It is deliberately not a required program parameter or a write.
	outcome.ConfirmedRootCauses[0].Resources = append(outcome.ConfirmedRootCauses[0].Resources, symptom)
	outcome.Findings = append(outcome.Findings, &contracts.Finding{DetectorId: outcome.Findings[0].DetectorId, Status: "active", PrimaryResource: symptom, Fingerprint: "symptom"})
	decision := &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"service-targetport-mismatch"}}
	ds, err := ReflectionLearningDiagnostics(root, outcome, decision)
	if err != nil || len(ds) != 0 {
		t.Fatalf("symptom demanded as program parameter: %v %v", ds, err)
	}
	ds, err = reflectionExecutableDiagnostics(root, outcome, []string{".sdo/playbooks/service-targetport-mismatch/diagnose.go"})
	if err != nil || len(ds) != 0 {
		t.Fatalf("symptom blocked executable authority: %v %v", ds, err)
	}
}

// One recorded action may repair two independent causes at once. Each cause
// binds to that write through its own target and its own cleared finding.
func TestCombinedWriteBindsToEachIndependentlyClearedCause(t *testing.T) {
	at := time.Unix(100, 0)
	recommendation := &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "hotel", Name: "recommendation"}
	search := &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "hotel", Name: "search"}
	outcome := &contracts.OutcomeRecord{
		Classification:      "success",
		ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Resources: []*contracts.ObjectRef{recommendation}}, {Resources: []*contracts.ObjectRef{search}}},
		Findings: []*contracts.Finding{
			{DetectorId: "workload-health", Status: "active", Fingerprint: "recommendation", PrimaryResource: recommendation},
			{DetectorId: "pod-admission", Status: "active", Fingerprint: "search", PrimaryResource: search},
		},
		RepairActions:   []*contracts.RepairActionReceipt{{Success: true, Resources: []*contracts.ObjectRef{recommendation, search}, StartedAt: timestamppb.New(at), CompletedAt: timestamppb.New(at.Add(time.Second))}},
		DetectorHistory: []*contracts.DetectorEvaluation{{DetectorId: "workload-health", Status: "clear", EvaluatedAt: timestamppb.New(at.Add(2 * time.Second))}, {DetectorId: "pod-admission", Status: "clear", EvaluatedAt: timestamppb.New(at.Add(2 * time.Second))}},
	}
	if got := VerifiedReflectionCauses(outcome); len(got) != 2 {
		t.Fatalf("combined write was not bound to both causes: %v", got)
	}
	outcome.DetectorHistory[1].Status = "firing"
	outcome.DetectorHistory[1].Fingerprints = []string{"search"}
	if got := VerifiedReflectionCauses(outcome); len(got) != 1 || got[0].GetCauseIndex() != 0 {
		t.Fatalf("only the cause whose own finding cleared may bind: %v", got)
	}
}

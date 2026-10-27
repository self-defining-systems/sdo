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
	if got := observedCauses(outcome); len(got) != 1 || len(got[0].Resources) != 2 {
		t.Fatalf("observed symptom disqualified hand repair: %v", got)
	}
	related := proto.Clone(outcome).(*contracts.OutcomeRecord)
	related.Findings = related.Findings[:1]
	related.Findings[0].RelatedResources = []*contracts.ObjectRef{service}
	if got := observedCauses(related); len(got) != 1 {
		t.Fatalf("opening related symptom was not observed: %v", got)
	}
	unobserved := proto.Clone(outcome).(*contracts.OutcomeRecord)
	unobserved.Findings = unobserved.Findings[:1]
	if got := observedCauses(unobserved); len(got) != 1 {
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
		"no repair": func(o *contracts.OutcomeRecord) { o.RepairActions = nil },
		"written target only related": func(o *contracts.OutcomeRecord) {
			o.Findings[0].PrimaryResource = &contracts.ObjectRef{Kind: "Deployment", Namespace: "app", Name: "other"}
			o.Findings[0].RelatedResources = []*contracts.ObjectRef{deployment}
		},
	} {
		t.Run(name, func(t *testing.T) {
			o := proto.Clone(outcome).(*contracts.OutcomeRecord)
			change(o)
			if got := observedCauses(o); len(got) != 0 {
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
	if got := observedCauses(outcome); len(got) != 1 || len(got[0].Resources) != 3 {
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
	} {
		t.Run(name, func(t *testing.T) {
			o := proto.Clone(outcome).(*contracts.OutcomeRecord)
			change(o)
			if got := observedCauses(o); len(got) != 0 {
				t.Fatalf("unproven cause authorized: %v", got)
			}
		})
	}
	// Opening findings omit findings that activate after launch. Without
	// global health closure, an unnamed cause resource may still be broken.
	for _, classification := range []string{"partial", "failed"} {
		t.Run(classification+" outcome with post-launch symptom", func(t *testing.T) {
			o := proto.Clone(outcome).(*contracts.OutcomeRecord)
			o.Classification = classification
			o.ReflectionEligibility = &contracts.ReflectionEligibility{Version: 1, Causes: []*contracts.ReflectionCauseWork{{CauseIndex: 0}}}
			o.FinalHealthDetectorState = []*contracts.DetectorEvaluation{{DetectorId: "traffic-health", Status: "firing", Fingerprints: []string{"frontend"}, EvaluatedAt: timestamppb.New(at.Add(3 * time.Second))}}
			if RepairedCauseObserved(o, o.GetConfirmedRootCauses()[0].GetResources()) {
				t.Fatal("unobserved resource treated as context without global health closure")
			}
			if got := observedCauses(o); len(got) != 0 {
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
	if got := observedCauses(outcome); len(got) != 2 {
		t.Fatalf("combined write was not bound to both causes: %v", got)
	}
	outcome.DetectorHistory[1].Status = "firing"
	outcome.DetectorHistory[1].Fingerprints = []string{"search"}
	if got := observedCauses(outcome); len(got) != 1 || got[0].GetCauseIndex() != 0 {
		t.Fatalf("only the cause whose own finding cleared may bind: %v", got)
	}
}

// A written target no opening finding named is covered by its cause when
// another finding of that cause cleared after the cause's last write (P35).
func TestVerifiedCauseCoversFindinglessWrittenTargetByItsCause(t *testing.T) {
	at := time.Unix(100, 0)
	write := func(offset time.Duration, refs ...*contracts.ObjectRef) *contracts.RepairActionReceipt {
		return &contracts.RepairActionReceipt{Success: true, Resources: refs, StartedAt: timestamppb.New(at.Add(offset)), CompletedAt: timestamppb.New(at.Add(offset + time.Second))}
	}
	clear := func(detector string, offset time.Duration) *contracts.DetectorEvaluation {
		return &contracts.DetectorEvaluation{DetectorId: detector, Status: "clear", EvaluatedAt: timestamppb.New(at.Add(offset))}
	}

	// h4/50/002: cause 0 writes Deployment/media-service, whose only finding is
	// traffic on its Service; cause 1 writes Deployment/user-service, which no
	// finding of its own cause observed.
	media := &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "social", Name: "media-service"}
	mediaService := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "social", Name: "media-service"}
	user := &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "social", Name: "user-service"}
	compose := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "social", Name: "compose-post-service"}
	h4 := &contracts.OutcomeRecord{
		Classification:           "success",
		ConfirmedRootCauses:      []*contracts.ConfirmedRootCause{{Resources: []*contracts.ObjectRef{media, mediaService}}, {Resources: []*contracts.ObjectRef{user}}},
		Findings:                 []*contracts.Finding{{DetectorId: "traffic-links", Status: "active", Fingerprint: "media-link", PrimaryResource: mediaService, RelatedResources: []*contracts.ObjectRef{compose}}},
		RepairActions:            []*contracts.RepairActionReceipt{{Resources: []*contracts.ObjectRef{media}, StartedAt: timestamppb.New(at), CompletedAt: timestamppb.New(at)}, write(3*time.Second, media), write(37*time.Second, user)},
		FinalHealthDetectorState: []*contracts.DetectorEvaluation{clear("traffic-links", 156*time.Second)},
	}
	// Cause 1 stays an evidence gap: the cleared finding belongs to cause 0.
	if got := observedCauses(h4); len(got) != 1 || got[0].GetCauseIndex() != 0 {
		t.Fatalf("h4: want only cause 0 covered by its cleared Service finding: %v", got)
	}
	// Listing cause 0's related-only resource in cause 1 does not make cause
	// 0's finding cover cause 1: only a finding whose primary resource is in
	// the cause covers its finding-less written target.
	related := proto.Clone(h4).(*contracts.OutcomeRecord)
	related.ConfirmedRootCauses[1].Resources = append(related.ConfirmedRootCauses[1].Resources, compose)
	if got := observedCauses(related); len(got) != 1 || got[0].GetCauseIndex() != 0 {
		t.Fatalf("h4: a related-only resource of another cause's finding covered cause 1: %v", got)
	}

	// h8/54/004: one combined act wrote the ConfigMap (observed) plus
	// Deployment/search and the webhook (no finding); the cause's other
	// findings cleared after that write.
	configMap := &contracts.ObjectRef{ApiVersion: "v1", Kind: "ConfigMap", Namespace: "hotel", Name: "mongo-geo-script"}
	geo := &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "hotel", Name: "mongodb-geo"}
	search := &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "hotel", Name: "search"}
	geoService := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "hotel", Name: "mongodb-geo"}
	webhook := &contracts.ObjectRef{ApiVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingWebhookConfiguration", Name: "pod-policy.validation.k8s.io"}
	h8 := &contracts.OutcomeRecord{
		Classification:      "success",
		ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Resources: []*contracts.ObjectRef{configMap, geo, search, geoService, webhook}}},
		Findings: []*contracts.Finding{
			{DetectorId: "health-objective", Status: "active", Fingerprint: "geo-unavailable", PrimaryResource: geo},
			{DetectorId: "health-objective", Status: "active", Fingerprint: "configmap-missing", PrimaryResource: configMap},
			{DetectorId: "service-endpoints", Status: "active", Fingerprint: "geo-endpoints", PrimaryResource: geoService},
			{DetectorId: "pod-admission", Status: "active", Fingerprint: "admission", PrimaryResource: &contracts.ObjectRef{ApiVersion: "v1", Kind: "Namespace", Name: "hotel"}},
		},
		RepairActions: []*contracts.RepairActionReceipt{write(0, configMap, search, webhook)},
		FinalHealthDetectorState: []*contracts.DetectorEvaluation{
			clear("health-objective", 90*time.Second), clear("service-endpoints", 90*time.Second), clear("pod-admission", 90*time.Second),
		},
	}
	if got := observedCauses(h8); len(got) != 1 || len(got[0].GetResources()) != 5 {
		t.Fatalf("h8: finding-less written targets vetoed the covered cause: %v", got)
	}

	for name, tc := range map[string]struct {
		outcome *contracts.OutcomeRecord
		change  func(*contracts.OutcomeRecord)
	}{
		// The Service finding cleared before the last write to media-service.
		"cause finding cleared before the last write": {h4, func(o *contracts.OutcomeRecord) {
			o.RepairActions = append(o.RepairActions, write(200*time.Second, media))
		}},
		// The written target has no finding, and the only observed resource
		// of its cause is related-only on a finding whose primary is elsewhere.
		"written finding-less target with only a related-only observation": {h4, func(o *contracts.OutcomeRecord) {
			o.ConfirmedRootCauses = []*contracts.ConfirmedRootCause{{Resources: []*contracts.ObjectRef{user, compose}}}
			o.RepairActions = []*contracts.RepairActionReceipt{write(37*time.Second, user)}
		}},
		"cause finding still firing": {h8, func(o *contracts.OutcomeRecord) {
			o.FinalHealthDetectorState[1] = &contracts.DetectorEvaluation{DetectorId: "service-endpoints", Status: "firing", Fingerprints: []string{"geo-endpoints"}, EvaluatedAt: timestamppb.New(at.Add(90 * time.Second))}
		}},
		"no finding in the cause at all": {h8, func(o *contracts.OutcomeRecord) {
			o.ConfirmedRootCauses[0].Resources = []*contracts.ObjectRef{search, webhook}
		}},
		"written observed target still needs its own primary finding": {h8, func(o *contracts.OutcomeRecord) {
			o.Findings[1].PrimaryResource = geo
			o.Findings[1].RelatedResources = []*contracts.ObjectRef{configMap}
		}},
		"without global health closure": {h8, func(o *contracts.OutcomeRecord) {
			o.Classification = "partial"
			o.ReflectionEligibility = &contracts.ReflectionEligibility{Version: 1, Causes: []*contracts.ReflectionCauseWork{{CauseIndex: 0}}}
		}},
	} {
		t.Run(name, func(t *testing.T) {
			o := proto.Clone(tc.outcome).(*contracts.OutcomeRecord)
			tc.change(o)
			for _, cause := range o.GetConfirmedRootCauses() {
				if RepairedCauseObserved(o, cause.GetResources()) {
					t.Fatalf("unproven cause observed as repaired: %v", cause.GetResources())
				}
			}
			if got := observedCauses(o); len(got) != 0 {
				t.Fatalf("unproven cause authorized: %v", got)
			}
		})
	}
}

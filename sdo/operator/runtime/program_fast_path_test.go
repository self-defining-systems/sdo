package runtime

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	policyv1 "k8s.io/api/policy/v1"

	"google.golang.org/protobuf/encoding/protojson"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func TestLiveEvidenceTrustAndFastPathCoverage(t *testing.T) {
	var fixture struct {
		PriorOutcomes []json.RawMessage `json:"prior_outcomes"`
		LiveIncident  struct {
			Findings  []sdk.Finding     `json:"findings"`
			MatchRuns []json.RawMessage `json:"match_runs"`
			Snapshot  sdktest.Snapshot  `json:"snapshot"`
		} `json:"live_incident"`
	}
	data, err := os.ReadFile("testdata/fast_path_live_evidence.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	outcomes := make([]*contracts.OutcomeRecord, 0, len(fixture.PriorOutcomes))
	for _, raw := range fixture.PriorOutcomes {
		outcome := new(contracts.OutcomeRecord)
		if err := protojson.Unmarshal(raw, outcome); err != nil {
			t.Fatal(err)
		}
		outcomes = append(outcomes, outcome)
	}
	trust := ProgramTrustStats(outcomes, 3)
	state := trust["health-objective-failure"]
	if !state.Trusted || state.VerifiedSuccesses != 3 || state.Failures != 0 {
		t.Fatalf("live successes did not establish trust: %+v", state)
	}
	runs := make([]ProgramRun, 0, len(fixture.LiveIncident.MatchRuns))
	for _, raw := range fixture.LiveIncident.MatchRuns {
		message := new(contracts.ProgramRun)
		if err := protojson.Unmarshal(raw, message); err != nil {
			t.Fatal(err)
		}
		run := programRunState(message)
		run.Verdict.Result = program.Match
		runs = append(runs, run)
	}
	selected, related, reason := selectTrustedFastPathRuns(fixture.LiveIncident.Findings, runs, trust, 3, fixture.LiveIncident.Snapshot)
	if reason != "" || len(selected) != 1 || len(related) != 1 || related[0].FindingFingerprint != FindingFingerprint(fixture.LiveIncident.Findings[2]) || related[0].ProgramID != "health-objective-failure" || related[0].Bindings["affected"].Name != "profile" {
		t.Fatalf("live incident eligibility: selected=%+v related=%+v reason=%q", selected, related, reason)
	}
	unrelated := append([]sdk.Finding(nil), fixture.LiveIncident.Findings...)
	unrelated[2].PrimaryResource.Name = "other"
	unrelated[2].Fingerprint = "other-service"
	_, _, reason = selectTrustedFastPathRuns(unrelated, runs, trust, 3, fixture.LiveIncident.Snapshot)
	if reason != FastPathSkipUncoveredFinding {
		t.Fatalf("unrelated Service skip = %q", reason)
	}

	// The same trusted diagnosis can be associated with a hint and a finding.
	// Repeated observations of either association must not create ambiguity.
	covered := fixture.LiveIncident.Findings[:2]
	runs = append(runs, runs[0], runs[1])
	selected, related, reason = selectTrustedFastPathRuns(covered, runs, trust, 3, fixture.LiveIncident.Snapshot)
	if reason != "" || len(selected) != 1 || len(related) != 0 || selected[0].ProgramID != "health-objective-failure" {
		t.Fatalf("equivalent matches should select one program: selected=%+v reason=%q", selected, reason)
	}
}

func TestFastPathRelationCoverageUsesSnapshotObjects(t *testing.T) {
	bound := sdk.ObjectRef{Kind: "Deployment", Namespace: "app", Name: "api"}
	run := ProgramRun{ProgramID: "repair", ProgramRevision: "rev", FindingFingerprint: "deployment", Bindings: program.Bindings{"workload": bound}, Verdict: program.Verdict{Result: program.Match}}
	trust := map[string]ProgramTrust{"repair": {Trusted: true, VerifiedSuccesses: 3, LastChange: "rev"}}
	controller := true
	snapshot := sdktest.Snapshot{NamespaceName: "app", DeploymentList: []appsv1.Deployment{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "api"}}}}}}, ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "api"}}}}, ReplicaSetList: []appsv1.ReplicaSet{{ObjectMeta: metav1.ObjectMeta{Name: "api-rs", Namespace: "app", OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", Name: "api", Controller: &controller}}}}}, PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "api-pod", Namespace: "app", OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "api-rs", Controller: &controller}}}}}}
	snapshot.HPAList = []autoscalingv2.HorizontalPodAutoscaler{{ObjectMeta: metav1.ObjectMeta{Name: "api-hpa", Namespace: "app"}, Spec: autoscalingv2.HorizontalPodAutoscalerSpec{ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{Kind: "Deployment", Name: "api"}}}}
	snapshot.PDBList = []policyv1.PodDisruptionBudget{{ObjectMeta: metav1.ObjectMeta{Name: "api-pdb", Namespace: "app"}, Spec: policyv1.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "api"}}}}}
	for _, ref := range []sdk.ObjectRef{{Kind: "Service", Namespace: "app", Name: "api"}, {Kind: "ReplicaSet", Namespace: "app", Name: "api-rs"}, {Kind: "Pod", Namespace: "app", Name: "api-pod"}, {Kind: "HorizontalPodAutoscaler", Namespace: "app", Name: "api-hpa"}, {Kind: "PodDisruptionBudget", Namespace: "app", Name: "api-pdb"}} {
		findings := []sdk.Finding{{Fingerprint: "deployment", PrimaryResource: bound}, {Fingerprint: ref.Kind, PrimaryResource: ref}}
		_, related, reason := selectTrustedFastPathRuns(findings, []ProgramRun{run}, trust, 3, snapshot)
		if reason != "" || len(related) != 1 || related[0].FindingFingerprint != ref.Kind {
			t.Fatalf("%s relation: %+v %q", ref.Kind, related, reason)
		}
	}
	for _, ref := range []sdk.ObjectRef{{Kind: "Service", Namespace: "other", Name: "api"}, {Kind: "Service", Namespace: "app", Name: "missing"}, {Kind: "Pod", Namespace: "app", Name: "unowned"}} {
		_, _, reason := selectTrustedFastPathRuns([]sdk.Finding{{Fingerprint: "deployment", PrimaryResource: bound}, {Fingerprint: ref.Kind + ref.Namespace + ref.Name, PrimaryResource: ref}}, []ProgramRun{run}, trust, 3, snapshot)
		if reason != FastPathSkipUncoveredFinding {
			t.Fatalf("unrelated %s accepted: %q", ref.Kind, reason)
		}
	}
}

func TestFastPathRelationCoverageForStatefulSetAndPolicyNearMisses(t *testing.T) {
	controller := true
	bound := sdk.ObjectRef{Kind: "StatefulSet", Namespace: "app", Name: "database"}
	snapshot := sdktest.Snapshot{NamespaceName: "app", StatefulSetList: []appsv1.StatefulSet{{ObjectMeta: metav1.ObjectMeta{Name: "database", Namespace: "app"}, Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "database"}}}}}}, ReplicaSetList: []appsv1.ReplicaSet{{ObjectMeta: metav1.ObjectMeta{Name: "database-rs", Namespace: "app", OwnerReferences: []metav1.OwnerReference{{Kind: "StatefulSet", Name: "database", Controller: &controller}}}}}, PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "database-0", Namespace: "app", OwnerReferences: []metav1.OwnerReference{{Kind: "StatefulSet", Name: "database", Controller: &controller}}}}}, HPAList: []autoscalingv2.HorizontalPodAutoscaler{{ObjectMeta: metav1.ObjectMeta{Name: "database", Namespace: "app"}, Spec: autoscalingv2.HorizontalPodAutoscalerSpec{ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{Kind: "StatefulSet", Name: "database"}}}}, PDBList: []policyv1.PodDisruptionBudget{{ObjectMeta: metav1.ObjectMeta{Name: "database", Namespace: "app"}, Spec: policyv1.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "database"}}}}}}
	run := ProgramRun{ProgramID: "repair", ProgramRevision: "rev", FindingFingerprint: "workload", Bindings: program.Bindings{"workload": bound}, Verdict: program.Verdict{Result: program.Match}}
	trust := map[string]ProgramTrust{"repair": {Trusted: true, VerifiedSuccesses: 3, LastChange: "rev"}}
	for _, kind := range []string{"Pod", "ReplicaSet", "HorizontalPodAutoscaler", "PodDisruptionBudget"} {
		name := "database"
		if kind == "Pod" {
			name = "database-0"
		} else if kind == "ReplicaSet" {
			name = "database-rs"
		}
		findings := []sdk.Finding{{Fingerprint: "workload", PrimaryResource: bound}, {Fingerprint: kind, PrimaryResource: sdk.ObjectRef{Kind: kind, Namespace: "app", Name: name}}}
		_, related, reason := selectTrustedFastPathRuns(findings, []ProgramRun{run}, trust, 3, snapshot)
		if reason != "" || len(related) != 1 {
			t.Fatalf("%s coverage: %+v %q", kind, related, reason)
		}
	}
	snapshot.PDBList[0].Spec.Selector.MatchLabels["app"] = "other"
	_, _, reason := selectTrustedFastPathRuns([]sdk.Finding{{Fingerprint: "workload", PrimaryResource: bound}, {Fingerprint: "pdb", PrimaryResource: sdk.ObjectRef{Kind: "PodDisruptionBudget", Namespace: "app", Name: "database"}}}, []ProgramRun{run}, trust, 3, snapshot)
	if reason != FastPathSkipUncoveredFinding {
		t.Fatalf("wrong PDB selector covered: %q", reason)
	}
}

func trustedPortLoop(t *testing.T, withOtherFault, trusted, healthyVerifier bool) (*ResponderLoop, *fake.Clientset) {
	t.Helper()
	client := fake.NewSimpleClientset(&corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(9999)}}},
	})
	provider := fastloopObjects{client: client}
	view, err := provider.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	findings, err := (fastloopDetector{}).Detect(context.Background(), view)
	if err != nil {
		t.Fatal(err)
	}
	if withOtherFault {
		findings = append(findings, sdk.Finding{DetectorID: "other", RuleID: "other", Status: sdk.FindingActive,
			PrimaryResource: sdk.ObjectRef{Kind: "ConfigMap", Namespace: "app", Name: "other"}})
	}
	detectors := []sdk.Detector{fastloopDetector{}}
	if withOtherFault {
		detectors = append(detectors, loopDetector{finding: findings[len(findings)-1]})
	}
	registry, err := program.NewRegistry([]program.Diagnoser{fastloopPortProgram{}}, []program.Mitigator{fastloopPortProgram{}})
	if err != nil {
		t.Fatal(err)
	}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "fast", Namespace: "app", SourceCommit: "tree-a", RepairPolicy: "recorded-actions", Findings: findings},
		registry, provider, detectors, PlanExecutor{
			Client: TypedPlanClient{Client: client}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: healthyVerifier},
		}, 5)
	loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}
	}
	loop.DiagnosisSettleDelay = time.Millisecond
	loop.CorpusReplayPassed = true
	loop.InitialDiagnose(context.Background())
	if trusted {
		loop.Trust = map[string]ProgramTrust{"service-targetport-mismatch": {VerifiedSuccesses: 3, LastChange: "tree-a", Trusted: true}}
	}
	return loop, client
}

func TestTrustedProgramFastPathResolvesWithoutResponder(t *testing.T) {
	loop, client := trustedPortLoop(t, false, true, true)
	attempt := loop.TryTrustedFastPath(context.Background(), 3)
	if !attempt.Resolved || !attempt.Attempted || len(attempt.Programs) != 1 || attempt.Programs[0] != "service-targetport-mismatch" {
		t.Fatalf("fast path: %+v", attempt)
	}
	service, err := client.CoreV1().Services("app").Get(context.Background(), "api", metav1.GetOptions{})
	if err != nil || service.Spec.Ports[0].TargetPort.IntVal != 8080 {
		t.Fatalf("plan not applied: %+v %v", service, err)
	}
	if len(loop.PlanApplications()) != 1 || len(loop.ProgramRuns()) < 2 {
		t.Fatalf("missing durable program evidence: applications=%+v runs=%+v", loop.PlanApplications(), loop.ProgramRuns())
	}
}

func TestTrustedProgramFastPathFailureRollsBackForResponder(t *testing.T) {
	loop, client := trustedPortLoop(t, false, true, false)
	attempt := loop.TryTrustedFastPath(context.Background(), 3)
	if !attempt.Attempted || attempt.Resolved || !attempt.RolledBack || !strings.Contains(attempt.Summary, "service-targetport-mismatch") {
		t.Fatalf("failed attempt: %+v", attempt)
	}
	service, err := client.CoreV1().Services("app").Get(context.Background(), "api", metav1.GetOptions{})
	if err != nil || service.Spec.Ports[0].TargetPort.IntVal != 9999 {
		t.Fatalf("failed plan not rolled back: %+v %v", service, err)
	}
}

func TestTrustedProgramFastPathRequiresEveryFindingExplained(t *testing.T) {
	for _, tc := range []struct {
		name, reason   string
		other, trusted bool
	}{
		{"untrusted", "untrusted", false, false},
		{"composite", "unexplained", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loop, client := trustedPortLoop(t, tc.other, tc.trusted, true)
			attempt := loop.TryTrustedFastPath(context.Background(), 3)
			if attempt.Attempted || attempt.Resolved {
				t.Fatalf("ineligible attempt: %+v", attempt)
			}
			wantReason := FastPathSkipUntrustedProgram
			if tc.other {
				wantReason = FastPathSkipUncoveredFinding
			}
			if attempt.SkippedReason != wantReason {
				t.Fatalf("skip reason = %q, want %q", attempt.SkippedReason, wantReason)
			}
			service, err := client.CoreV1().Services("app").Get(context.Background(), "api", metav1.GetOptions{})
			if err != nil || service.Spec.Ports[0].TargetPort.IntVal != 9999 {
				t.Fatalf("ineligible plan changed service: %+v %v", service, err)
			}
		})
	}
}

func TestTrustedProgramFastPathSkipsUnrelatedService(t *testing.T) {
	loop, client := trustedPortLoop(t, false, true, true)
	if _, err := client.CoreV1().Services("app").Create(context.Background(), &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "app"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "other"}, Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(8080)}}}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	finding := sdk.Finding{DetectorID: "other-service", RuleID: "no-endpoints", Status: sdk.FindingActive, PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "other"}}
	loop.Request.Findings = append(loop.Request.Findings, finding)
	loop.Detectors = append(loop.Detectors, loopDetector{finding: finding})
	attempt := loop.TryTrustedFastPath(context.Background(), 3)
	if attempt.Attempted || attempt.SkippedReason != FastPathSkipUncoveredFinding {
		t.Fatalf("unrelated Service eligibility: %+v", attempt)
	}
	service, err := client.CoreV1().Services("app").Get(context.Background(), "api", metav1.GetOptions{})
	if err != nil || service.Spec.Ports[0].TargetPort.IntVal != 9999 {
		t.Fatalf("skipped plan changed Service: %+v %v", service, err)
	}
}

type judgementPortProgram struct{}

func (judgementPortProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "port-judgement", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}
}

func (judgementPortProgram) Diagnose(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {
	return program.Verdict{Result: program.NeedsJudgement, Question: "which port is intended?"}, nil
}

func TestTrustedProgramFastPathRejectsNeedsJudgement(t *testing.T) {
	loop, client := trustedPortLoop(t, false, true, true)
	registry, err := program.NewRegistry([]program.Diagnoser{fastloopPortProgram{}, judgementPortProgram{}}, []program.Mitigator{fastloopPortProgram{}})
	if err != nil {
		t.Fatal(err)
	}
	loop.Programs = registry
	attempt := loop.TryTrustedFastPath(context.Background(), 3)
	if attempt.Attempted || attempt.Resolved {
		t.Fatalf("judgement program bypassed: %+v", attempt)
	}
	service, err := client.CoreV1().Services("app").Get(context.Background(), "api", metav1.GetOptions{})
	if err != nil || service.Spec.Ports[0].TargetPort.IntVal != 9999 {
		t.Fatalf("judgement changed service: %+v %v", service, err)
	}
}

func TestTrustedProgramFastPathResolvesLatentDriftWithoutResponder(t *testing.T) {
	loop, client := trustedPortLoop(t, false, true, true)
	loop.Detectors = []sdk.Detector{fastloopClearDetector{}}
	finding := loop.Request.Findings[0]
	finding.DetectorID = "drift/service-targetport-mismatch"
	finding.Severity = sdk.SeverityLatent
	finding.ParameterBindings = map[string]sdk.ObjectRef{"service": finding.PrimaryResource}
	loop.Request.Findings = []sdk.Finding{finding}
	loop.findings = []sdk.Finding{finding}
	attempt := loop.TryTrustedFastPath(context.Background(), 1)
	if !attempt.Attempted || !attempt.Resolved || len(attempt.Programs) != 1 {
		t.Fatalf("latent drift did not use trusted program: %+v", attempt)
	}
	service, err := client.CoreV1().Services("app").Get(context.Background(), "api", metav1.GetOptions{})
	if err != nil || service.Spec.Ports[0].TargetPort.IntVal != 8080 {
		t.Fatalf("latent drift was not repaired: %+v %v", service, err)
	}
}

func TestTrustedProgramFastPathLeavesInvestigatorFindingToResponder(t *testing.T) {
	loop, _ := trustedPortLoop(t, false, true, true)
	loop.Detectors = []sdk.Detector{fastloopClearDetector{}}
	finding := loop.Request.Findings[0]
	finding.DetectorID = "drift/investigator"
	finding.Severity = sdk.SeverityLatent
	loop.Request.Findings = []sdk.Finding{finding}
	loop.findings = []sdk.Finding{finding}
	if attempt := loop.TryTrustedFastPath(context.Background(), 1); attempt.Attempted || attempt.Resolved {
		t.Fatalf("investigator finding bypassed responder: %+v", attempt)
	}
}

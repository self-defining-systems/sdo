package runtime

import (
	"context"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	policyv1 "k8s.io/api/policy/v1"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func TestHandwrittenFastPathCoverageDeduplicatesMatches(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
	bindings := program.Bindings{Objects: program.ObjectBindings{"workload": ref}}
	snapshot := sdktest.Snapshot{NamespaceName: "app", DeploymentList: []appsv1.Deployment{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "api"}}}}}}, ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "api"}}}}}
	findings := []sdk.Finding{{Fingerprint: "deployment", PrimaryResource: ref}, {Fingerprint: "hint", PrimaryResource: ref}, {Fingerprint: "service", PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}}}
	runs := []ProgramRun{{ProgramID: "repair", ProgramRevision: "revision-a", FindingFingerprint: "deployment", Bindings: bindings, Verdict: program.Verdict{Result: program.Match}}, {ProgramID: "repair", ProgramRevision: "revision-a", FindingFingerprint: "hint", Bindings: bindings, Verdict: program.Verdict{Result: program.Match}}}
	trust := map[string]ProgramTrust{"repair": {Trusted: true, VerifiedSuccesses: 3, LastChange: "revision-a"}}
	selected, related, reason := selectTrustedFastPathRuns(findings, runs, trust, 3, snapshot)
	if reason != "" || len(selected) != 1 || len(related) != 1 || related[0].FindingFingerprint != "service" || related[0].ProgramID != "repair" {
		t.Fatalf("coverage: selected=%+v related=%+v reason=%q", selected, related, reason)
	}
	runs = append(runs, runs[0], runs[1])
	selected, related, reason = selectTrustedFastPathRuns(findings[:2], runs, trust, 3, snapshot)
	if reason != "" || len(selected) != 1 || len(related) != 0 {
		t.Fatalf("duplicate matches: selected=%+v related=%+v reason=%q", selected, related, reason)
	}
}

func TestFastPathRelationCoverageUsesSnapshotObjects(t *testing.T) {
	bound := sdk.ObjectRef{Kind: "Deployment", Namespace: "app", Name: "api"}
	run := ProgramRun{ProgramID: "repair", ProgramRevision: "rev", FindingFingerprint: "deployment", Bindings: program.Bindings{Objects: program.ObjectBindings{"workload": bound}}, Verdict: program.Verdict{Result: program.Match}}
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

}

func TestFastPathRelationCoverageForStatefulSetAndPolicyNearMisses(t *testing.T) {
	controller := true
	bound := sdk.ObjectRef{Kind: "StatefulSet", Namespace: "app", Name: "database"}
	snapshot := sdktest.Snapshot{NamespaceName: "app", StatefulSetList: []appsv1.StatefulSet{{ObjectMeta: metav1.ObjectMeta{Name: "database", Namespace: "app"}, Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "database"}}}}}}, ReplicaSetList: []appsv1.ReplicaSet{{ObjectMeta: metav1.ObjectMeta{Name: "database-rs", Namespace: "app", OwnerReferences: []metav1.OwnerReference{{Kind: "StatefulSet", Name: "database", Controller: &controller}}}}}, PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "database-0", Namespace: "app", OwnerReferences: []metav1.OwnerReference{{Kind: "StatefulSet", Name: "database", Controller: &controller}}}}}, HPAList: []autoscalingv2.HorizontalPodAutoscaler{{ObjectMeta: metav1.ObjectMeta{Name: "database", Namespace: "app"}, Spec: autoscalingv2.HorizontalPodAutoscalerSpec{ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{Kind: "StatefulSet", Name: "database"}}}}, PDBList: []policyv1.PodDisruptionBudget{{ObjectMeta: metav1.ObjectMeta{Name: "database", Namespace: "app"}, Spec: policyv1.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "database"}}}}}}
	run := ProgramRun{ProgramID: "repair", ProgramRevision: "rev", FindingFingerprint: "workload", Bindings: program.Bindings{Objects: program.ObjectBindings{"workload": bound}}, Verdict: program.Verdict{Result: program.Match}}
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
		findings = append(findings, sdk.Finding{DetectorID: "other", RuleID: "other", Status: sdk.FindingActive, Severity: sdk.SeverityCritical, Summary: "unrelated fault", Evidence: "synthetic unrelated fault",
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

func TestFastPathSkipsSettleWithoutTrustedMitigator(t *testing.T) {
	for _, tc := range []struct {
		name         string
		trust        ProgramTrust
		noPrograms   bool
		diagnoseOnly bool
		want         FastPathSkipReason
	}{
		{name: "empty catalog", noPrograms: true, want: FastPathSkipNoSelectedProgram},
		{name: "diagnosis only", diagnoseOnly: true, trust: ProgramTrust{Trusted: true, VerifiedSuccesses: 3, LastChange: "tree-a"}, want: FastPathSkipMitigatorMissing},
		{name: "cold", want: FastPathSkipUntrustedProgram},
		{name: "below threshold", trust: ProgramTrust{Trusted: true, VerifiedSuccesses: 2, LastChange: "tree-a"}, want: FastPathSkipUntrustedProgram},
		{name: "failed", trust: ProgramTrust{Trusted: true, VerifiedSuccesses: 3, Failures: 1, LastChange: "tree-a"}, want: FastPathSkipUntrustedProgram},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				loop, _ := trustedPortLoop(t, false, false, true)
				if tc.noPrograms || tc.diagnoseOnly {
					var diagnosers []program.Diagnoser
					if tc.diagnoseOnly {
						diagnosers = []program.Diagnoser{fastloopPortProgram{}}
					}
					registry, err := program.NewRegistry(diagnosers, nil)
					if err != nil {
						t.Fatal(err)
					}
					loop.Programs = registry
				}
				loop.Trust = map[string]ProgramTrust{"service-targetport-mismatch": tc.trust}
				loop.DiagnosisSettleDelay = DefaultDiagnosisSettleDelay
				started := time.Now()
				attempt := loop.TryTrustedFastPath(context.Background(), 3)
				if elapsed := time.Since(started); elapsed != 0 {
					t.Fatalf("ineligible fast path delayed responder by %s", elapsed)
				}
				if attempt.Attempted || attempt.SkippedReason != tc.want {
					t.Fatalf("ineligible fast path: %+v, want %q", attempt, tc.want)
				}
			})
		})
	}
}

func TestTrustedFastPathStillSettlesAfterInitialNoMatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		loop, client := trustedPortLoop(t, false, true, true)
		service, err := client.CoreV1().Services("app").Get(context.Background(), "api", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		service.Spec.Ports[0].TargetPort = intstr.FromInt32(8080)
		if _, err := client.CoreV1().Services("app").Update(context.Background(), service, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		loop.runs = nil
		runs := loop.InitialDiagnose(context.Background())
		if len(runs) == 0 {
			t.Fatal("missing initial diagnosis")
		}
		for _, run := range runs {
			if run.Verdict.Result != program.NoMatch {
				t.Fatalf("initial verdict: %+v", run)
			}
		}
		service.Spec.Ports[0].TargetPort = intstr.FromInt32(9999)
		if _, err := client.CoreV1().Services("app").Update(context.Background(), service, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		loop.DiagnosisSettleDelay = DefaultDiagnosisSettleDelay
		started := time.Now()
		attempt := loop.TryTrustedFastPath(context.Background(), 3)
		if elapsed := time.Since(started); elapsed != DefaultDiagnosisSettleDelay {
			t.Fatalf("trusted repair settled for %s, want %s", elapsed, DefaultDiagnosisSettleDelay)
		}
		if !attempt.Attempted || !attempt.Resolved {
			t.Fatalf("transient NoMatch prevented trusted repair: %+v", attempt)
		}
	})
}

func TestTrustedFastPathPlansFromTheSettledMatchSnapshot(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "hotel-reservation", Name: "search"}
	finding := sdk.Finding{DetectorID: "deployment-image-pull-backoff", RuleID: "unavailable-image-pull-backoff", Status: sdk.FindingActive, PrimaryResource: ref, ParameterBindings: map[string]sdk.ObjectRef{"deployment": ref}, Playbooks: []string{"health-objective-failure"}}
	broken := sdktest.Snapshot{NamespaceName: ref.Namespace, DeploymentList: []appsv1.Deployment{{ObjectMeta: metav1.ObjectMeta{Name: ref.Name, Namespace: ref.Namespace}}}, PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "search-pod", Namespace: ref.Namespace, Labels: map[string]string{"app": "search"}}, Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}}}}}}}}
	recovered := broken
	recovered.PodList = nil
	provider := &changingProvider{snapshot: broken}
	detections := 0
	detector := imagePullChangingDetector{find: func() []sdk.Finding {
		detections++
		provider.snapshot = recovered
		if detections == 1 {
			return []sdk.Finding{finding}
		}
		return nil
	}}
	registry, err := program.NewRegistry([]program.Diagnoser{transientImageProgram{}}, []program.Mitigator{transientImageProgram{}})
	if err != nil {
		t.Fatal(err)
	}
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: planObject(ref, map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"image": "search:broken"}}}}})}}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "synthetic-settled-match", Namespace: ref.Namespace, SourceCommit: "tree-a", RepairPolicy: "recorded-actions", Findings: []sdk.Finding{finding}}, registry, provider, []sdk.Detector{detector}, PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}, 5)
	loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}
	}
	loop.DiagnosisSettleDelay = time.Millisecond
	loop.CorpusReplayPassed = true
	loop.Trust = map[string]ProgramTrust{"health-objective-failure": {VerifiedSuccesses: 3, LastChange: "tree-a", Trusted: true}}
	attempt := loop.TryTrustedFastPath(context.Background(), 3)
	if !attempt.Resolved || !attempt.Attempted {
		t.Fatalf("settled match lost when pods changed: %+v", attempt)
	}
}

type imagePullChangingDetector struct{ find func() []sdk.Finding }

func (imagePullChangingDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "deployment-image-pull-backoff"}
}

func (d imagePullChangingDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return d.find(), nil
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

type judgementPortProgram struct{}

func (judgementPortProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "port-judgement", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}
}

func (judgementPortProgram) Diagnose(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {
	return program.Verdict{Result: program.NeedsJudgement, Question: "which port is intended?"}, nil
}

func TestTrustedProgramFastPathResolvesLatentDriftWithoutResponder(t *testing.T) {
	loop, client := trustedPortLoop(t, false, true, true)
	loop.Detectors = []sdk.Detector{fastloopClearDetector{}}
	finding := loop.Request.Findings[0]
	finding.DetectorID = "drift.service-targetport-mismatch"
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

func TestSuccessfulFastPathAddsBoundTrustCredit(t *testing.T) {
	loop, _ := trustedPortLoop(t, false, true, true)
	attempt := loop.TryTrustedFastPath(context.Background(), 3)
	if !attempt.Resolved {
		t.Fatalf("fast path failed: %+v", attempt)
	}
	loop.CreditEquivalentRepairs(context.Background(), ProgramRepairReceipts(loop.PlanApplications(), nil))
	result := fastPathResult(loop.Request, loop, attempt, time.Now())
	fillProgramRevisions(context.Background(), result.ProgramRuns, loop.Request.RepositoryWorktree, loop.Request.SourceCommit)
	message, err := incidentResultContract(result)
	if err != nil {
		t.Fatal(err)
	}
	outcome := &contracts.OutcomeRecord{Classification: "success", SourceCommit: "tree-a", ProgramRuns: message.ProgramRuns, PlanApplications: message.PlanApplications, ProgramToolCalls: message.ProgramToolCalls, RepairActions: message.RepairActions, ConfirmedRootCauses: message.ConfirmedRootCauses}
	settleTestOutcome(outcome)
	state := ProgramTrustStats([]*contracts.OutcomeRecord{outcome}, 1)["service-targetport-mismatch"]
	if state.VerifiedSuccesses != 1 || !state.Trusted {
		t.Fatalf("successful fast path lost bound evidence: %+v calls=%+v", state, message.ProgramToolCalls)
	}
	outcome.Classification = "failed"
	if state := ProgramTrustStats([]*contracts.OutcomeRecord{outcome}, 1)["service-targetport-mismatch"]; state.Trusted || state.VerifiedSuccesses != 0 {
		t.Fatalf("unverified fast path earned credit: %+v", state)
	}
}

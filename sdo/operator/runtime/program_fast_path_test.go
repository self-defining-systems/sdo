package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

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
			service, err := client.CoreV1().Services("app").Get(context.Background(), "api", metav1.GetOptions{})
			if err != nil || service.Spec.Ports[0].TargetPort.IntVal != 9999 {
				t.Fatalf("ineligible plan changed service: %+v %v", service, err)
			}
		})
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

package runtime

import (
	"context"
	"testing"

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
	loop.InitialDiagnose(context.Background())
	if trusted {
		loop.ProgramCounts = map[string]ProgramCount{programCountKey("service-targetport-mismatch", "tree-a"): {Successes: 3}}
	}
	return loop, client
}

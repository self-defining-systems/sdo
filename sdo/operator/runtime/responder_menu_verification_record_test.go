package runtime

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type unresolvedMenuDiagnosis struct{ fastloopPortProgram }

func (unresolvedMenuDiagnosis) Diagnose(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {
	return program.Verdict{Result: program.Match}, nil
}

func TestMenuApplicationRecordsFinalScopedDiagnosisVerdict(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset(&corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "first", Namespace: "app"},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(9999)}}},
	})
	provider := fastloopObjects{client: client}
	learned := unresolvedMenuDiagnosis{}
	registry, err := program.NewRegistry([]program.Diagnoser{learned}, []program.Mitigator{learned})
	if err != nil {
		t.Fatal(err)
	}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "diagnosis-persists", Namespace: "app", RepairPolicy: "recorded-actions"}, registry, provider, []sdk.Detector{fastloopDetector{}}, PlanExecutor{
		Client: TypedPlanClient{Client: client}, Rollouts: KubernetesPlanRollouts{Client: client},
		Verifier: PlanHealthVerifier{Detectors: []sdk.Detector{fastloopDetector{}}, Provider: provider},
	}, 3)
	loop.MenuConfig = productionResponderMenu()
	loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}
	}
	menu := loop.Handle(ctx, ToolRequest{Command: "menu", ProgramID: learned.Spec().ID, Parameters: map[string]string{"service": "Service/app/first"}})
	option := ""
	for _, item := range menu.Menu.GetOptions() {
		if item.ProgramId == learned.Spec().ID {
			option = item.OptionId
		}
	}
	if menu.Error != "" || option == "" {
		t.Fatalf("missing learned option: %+v", menu)
	}
	response := loop.Handle(ctx, ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{Version: 1, OptionId: option, RoundLimit: 1}, Parameters: map[string]string{"service": "Service/app/first"}})
	if response.TargetedHealthy || response.Report.GetStatus() != contracts.MitigationReport_STATUS_UNRESOLVED {
		t.Fatalf("remaining program diagnosis must prevent verified repair: %+v", response)
	}
	applications := loop.PlanApplications()
	if len(applications) != 1 || !applications[0].Result.VerificationCompleted || applications[0].Result.VerificationPassed {
		t.Fatalf("durable application disagrees with final scoped diagnosis: %+v", applications)
	}
}

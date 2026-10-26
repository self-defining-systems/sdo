package runtime

import (
	"context"
	"encoding/json"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"strings"
	"testing"
)

type scriptVerifyProgram struct {
	fastloopPortProgram
	remains bool
}

func (p scriptVerifyProgram) Diagnose(ctx context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	if p.remains {
		return program.Verdict{Result: program.Match}, nil
	}
	return p.fastloopPortProgram.Diagnose(ctx, v, b)
}
func (p scriptVerifyProgram) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.RunScript{Script: `kubectl patch service "$SERVICE" --type merge -p "$PATCH"`, Env: map[string]program.Value{"SERVICE": program.FromParam("name"), "PATCH": program.FromParam("patch")}}}}, nil
}
func (p scriptVerifyProgram) Spec() program.ProgramSpec {
	s := p.fastloopPortProgram.Spec()
	s.Params = append(s.Params, program.ParamSpec{Name: "name", ValueKind: program.StringValue, Default: "first"}, program.ParamSpec{Name: "patch", ValueKind: program.StringValue, Default: `{"spec":{"ports":[{"port":80,"targetPort":8080}]}}`})
	return s
}

type verifyingScriptClient struct{ TypedPlanClient }

func (c verifyingScriptClient) RunScript(ctx context.Context, r ScriptExecutionRequest) (ScriptExecutionResult, error) {
	service, err := c.Client.CoreV1().Services("app").Get(ctx, r.Env["SERVICE"], metav1.GetOptions{})
	if err != nil {
		return ScriptExecutionResult{}, err
	}
	service.Spec.Ports[0].TargetPort = intstr.FromInt32(8080)
	_, err = c.Client.CoreV1().Services("app").Update(ctx, service, metav1.UpdateOptions{})
	code := int32(0)
	return ScriptExecutionResult{ApplicationID: r.ApplicationID, JobName: "script-job", JobUID: "script-uid", ScriptDigest: program.ScriptDigest(r.Script), ExitCode: &code}, err
}

func TestRunScriptActVerifyRequiresDiagnosisAndTargetedClearance(t *testing.T) {
	for _, remains := range []bool{true, false} {
		t.Run(map[bool]string{true: "Match gives no credit", false: "NoMatch with peer unhealthy gives scoped credit"}[remains], func(t *testing.T) {
			client := fake.NewSimpleClientset(&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "first", Namespace: "app"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(9999)}}}}, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "peer", Namespace: "app"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(9999)}}}})
			provider := fastloopObjects{client: client}
			learned := scriptVerifyProgram{remains: remains}
			registry, err := program.NewRegistry([]program.Diagnoser{learned}, []program.Mitigator{learned})
			if err != nil {
				t.Fatal(err)
			}
			snapshot, _ := provider.Snapshot(context.Background())
			findings, _ := (fastloopDetector{}).Detect(context.Background(), snapshot)
			loop := NewResponderLoop(IncidentRequest{IncidentID: "script-verify", Namespace: "app", RepairPolicy: "recorded-actions", Findings: findings[:1]}, registry, provider, []sdk.Detector{fastloopDetector{}}, PlanExecutor{Client: verifyingScriptClient{TypedPlanClient{Client: client, ScriptImage: "script-tools:fixture"}}, Rollouts: KubernetesPlanRollouts{Client: client}, Verifier: PlanHealthVerifier{Detectors: []sdk.Detector{fastloopDetector{}}, Provider: provider}}, 3)
			loop.MenuConfig = productionResponderMenu()
			loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView {
				return &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}
			}
			menu := loop.Handle(context.Background(), ToolRequest{Command: "menu", ProgramID: learned.Spec().ID, Parameters: map[string]string{"service": "Service/app/first"}})
			option := ""
			for _, item := range menu.Menu.GetOptions() {
				if item.ProgramId == learned.Spec().ID {
					option = item.OptionId
				}
			}
			if menu.Error != "" || option == "" {
				t.Fatalf("script menu unavailable: %+v", menu)
			}
			response := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", FindingFingerprints: []string{FindingFingerprint(findings[0])}, Selection: &contracts.MitigationSelection{Version: 1, OptionId: option, RoundLimit: 1}, Parameters: map[string]string{"service": "Service/app/first"}})
			if response.TargetedHealthy == remains || response.Healthy {
				t.Fatalf("script credit/global closure: targeted=%v healthy=%v error=%s report=%v", response.TargetedHealthy, response.Healthy, response.Error, response.Report)
			}
			loop.CreditEquivalentRepairs(context.Background(), loop.MenuRepairActions())
			applications := loop.PlanApplications()
			encoded, _ := json.Marshal(applications[0].Result)
			hasObservedBasis := strings.Contains(string(encoded), `"FinalVerificationBasis":"observed-transition"`)
			if !remains && applications[0].Result.FinalEquivalenceVerified != nil {
				t.Fatal("opaque script claimed exact effect equivalence")
			}
			if hasObservedBasis == remains {
				t.Fatalf("script final settlement must use observed-transition without typed equivalence: %s", encoded)
			}
			receipts := ProgramRepairReceipts(applications, nil)
			if len(receipts) == 0 || receipts[0].Reversible || len(receipts[0].Resources) != 1 || receipts[0].Resources[0].Kind != "Service" {
				t.Fatalf("script repair scope/preimages lost: %+v", receipts)
			}
			if len(applications) != 1 || applications[0].Result.VerificationPassed == remains {
				t.Fatalf("durable credit disagrees: %+v", applications)
			}
		})
	}
}

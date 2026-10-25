package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type reviewConfigProvider struct{ client *fake.Clientset }

func (p reviewConfigProvider) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	maps, err := p.client.CoreV1().ConfigMaps("app").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return sdktest.Snapshot{NamespaceName: "app", ConfigMapList: maps.Items}, nil
}

type reviewConfigProgram struct{}

func (reviewConfigProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "config-repair", Params: []program.ParamSpec{{Name: "config", Kind: "ConfigMap", Required: true}, {Name: "desired", ValueKind: program.StringValue, Default: "fixed"}}}
}
func (reviewConfigProgram) Diagnose(_ context.Context, view program.ReadView, bindings program.Bindings) (program.Verdict, error) {
	for _, cm := range view.(interface{ ConfigMaps() []corev1.ConfigMap }).ConfigMaps() {
		if cm.Name == bindings.Objects["config"].Name && cm.Data["value"] != "fixed" {
			return program.Verdict{Result: program.Match}, nil
		}
	}
	return program.Verdict{Result: program.NoMatch}, nil
}
func (reviewConfigProgram) Mitigate(_ context.Context, _ program.ReadView, bindings program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.SetField{Ref: bindings.Objects["config"], Path: "data[value]", Value: program.FromParam("desired")}}}, nil
}

func TestExplicitFindingScopeThroughRepairTransport(t *testing.T) {
	for _, command := range []string{"apply", "mitigate"} {
		for _, scenario := range []string{"deployment-still-active", "peer-on-same-resource", "scope-unknown"} {
			t.Run(command+"/"+scenario, func(t *testing.T) {
				ctx := context.Background()
				client := fake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "settings", Namespace: "app", UID: "synthetic", ResourceVersion: "1"}, Data: map[string]string{"value": "bad"}})
				provider := reviewConfigProvider{client}
				original := sdk.Finding{DetectorID: "availability", RuleID: "target", Fingerprint: "target", Status: sdk.FindingActive, Severity: sdk.SeverityCritical, PrimaryResource: sdk.ObjectRef{Kind: "Deployment", Namespace: "app", Name: "api"}, Summary: "target failing", Evidence: "synthetic observation", Playbooks: []string{"base"}}
				current := original
				if scenario == "peer-on-same-resource" {
					original.PrimaryResource = sdk.ObjectRef{Kind: "ConfigMap", Namespace: "app", Name: "settings"}
					current = original
					current.RuleID = "peer"
					current.Fingerprint = "peer"
				}
				detector := reviewScopeDetector{current}
				registry, err := program.NewRegistry([]program.Diagnoser{reviewConfigProgram{}}, []program.Mitigator{reviewConfigProgram{}})
				if err != nil {
					t.Fatal(err)
				}
				loop := NewResponderLoop(IncidentRequest{IncidentID: "explicit-scope", Namespace: "app", RepairPolicy: "recorded-actions", Findings: []sdk.Finding{original}}, registry, provider, []sdk.Detector{detector}, PlanExecutor{Client: TypedPlanClient{Client: client}, Rollouts: KubernetesPlanRollouts{Client: client}, Verifier: PlanHealthVerifier{Provider: provider, Detectors: []sdk.Detector{detector}}}, 3)
				loop.ReadView = func(view sdk.DetectionContext) program.ReadView {
					return &sdktest.ReplayReadView{Snapshot: view.(sdktest.Snapshot)}
				}
				loop.MenuConfig = productionResponderMenu()
				server := SessionToolServer{Directory: filepath.Join(t.TempDir(), "tools"), Handle: loop.Handle}
				if err = server.Prepare(); err != nil {
					t.Fatal(err)
				}
				defer server.Close()
				targets := `["target"]`
				if scenario == "scope-unknown" {
					targets = `[]`
				}
				var response ToolResponse
				if command == "apply" {
					planned := exchangeSelfCheck(t, &server, fmt.Sprintf(`{"command":"plan","program_id":"config-repair","bindings":{"config":"ConfigMap/app/settings"},"finding_fingerprints":%s}`, targets))
					if planned.Error != "" {
						t.Fatal(planned.Error)
					}
					response = exchangeSelfCheck(t, &server, fmt.Sprintf(`{"command":"apply","plan_ids":[%q]}`, planned.PlanID))
				} else {
					menu := loop.Handle(ctx, ToolRequest{Command: "menu", ProgramID: "config-repair", Parameters: map[string]string{"config": "ConfigMap/app/settings"}})
					option := ""
					for _, item := range menu.Menu.GetOptions() {
						if item.ProgramId == "config-repair" {
							option = item.OptionId
						}
					}
					response = exchangeSelfCheck(t, &server, fmt.Sprintf(`{"command":"mitigate","selection":{"version":1,"option_id":%q,"round_limit":1},"parameters":{"config":"ConfigMap/app/settings"},"finding_fingerprints":%s}`, option, targets))
				}
				want := scenario == "peer-on-same-resource"
				if response.Error != "" || response.TargetedHealthy != want || response.MitigationHealth == nil || (scenario != "scope-unknown" && len(response.MitigationHealth.TargetedFindingFingerprints) != 1) {
					t.Fatalf("target scope: %+v want=%v", response, want)
				}
				if response.Healthy {
					t.Fatal("scope must preserve global peer/target health failure")
				}
				applications := loop.PlanApplications()
				if len(applications) != 1 || applications[0].Result.VerificationPassed != want {
					t.Fatalf("durable scoped verdict: %+v", applications)
				}
			})
		}
	}
}

func TestUnknownTargetFindingIsRefusedBeforeRepair(t *testing.T) {
	loop, client := trustedPortLoop(t, false, false, true)
	loop.MenuConfig = productionResponderMenu()
	ctx := context.Background()
	planned := loop.Handle(ctx, ToolRequest{Command: "plan", ProgramID: "service-targetport-mismatch", FindingFingerprints: []string{"not-an-active-finding"}, Bindings: program.Bindings{Objects: program.ObjectBindings{"service": {Kind: "Service", APIVersion: "v1", Namespace: "app", Name: "api"}}}})
	if planned.Error == "" || planned.PlanID != "" {
		t.Fatalf("unknown finding accepted: %+v", planned)
	}
	menu := loop.Handle(ctx, ToolRequest{Command: "menu", ProgramID: "service-targetport-mismatch", Parameters: map[string]string{"service": "Service/app/api"}})
	if menu.Error != "" {
		t.Fatal(menu.Error)
	}
	var raw []byte
	for _, option := range menu.Menu.GetOptions() {
		if option.ProgramId == "service-targetport-mismatch" {
			raw = []byte(fmt.Sprintf(`{"command":"mitigate","selection":{"version":1,"option_id":%q,"round_limit":1},"finding_fingerprints":["not-an-active-finding"]}`, option.OptionId))
			break
		}
	}
	var request ToolRequest
	if err := json.Unmarshal(raw, &request); err != nil {
		t.Fatal(err)
	}
	response := loop.Handle(ctx, request)
	if response.Error == "" || response.ApplicationID != "" {
		t.Fatalf("unknown mitigation target accepted: %+v", response)
	}
	service, err := client.CoreV1().Services("app").Get(ctx, "api", metav1.GetOptions{})
	if err != nil || service.Spec.Ports[0].TargetPort.IntVal != 9999 || loop.LoopsUsed() != 0 {
		t.Fatalf("invalid scope changed state: %v %v", service, err)
	}
}

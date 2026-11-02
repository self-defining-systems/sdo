package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"os"
	"os/exec"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"strings"
	"testing"
	"time"
)

func multiCall(t *testing.T, loop *ResponderLoop, options string) ToolResponse {
	t.Helper()
	var request ToolRequest
	if err := json.Unmarshal([]byte(`{"command":"mitigate","selection_set":{"version":1,"options":`+options+`}}`), &request); err != nil {
		t.Fatal(err)
	}
	return loop.Handle(context.Background(), request)
}

func TestMultiMitigationIndependentOneRound(t *testing.T) {
	loop, cluster := menuB3Loop(t)
	loop.Request.RepairPolicy = "recorded-actions"
	loop.Executor.Verifier = planFakeVerifier{true}
	loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	result := multiCall(t, loop, `[{"option_id":"SetField","parameters":{"ref":"Service/app/backend","path":"spec.ports[0].targetPort","value":"8080","reason":"restore route"}},{"option_id":"ScaleWorkload","parameters":{"ref":"Deployment/app/frontend","replicas":"1"}}]`)
	if result.Error != "" || result.Report == nil || len(result.MenuExecution) != 2 || loop.LoopsUsed() != 1 {
		t.Fatalf("batch failed: %+v", result)
	}
	if len(loop.MenuRepairActions()) != 2 {
		t.Fatal("per-option receipts missing")
	}
	if value, _ := testField(cluster.objects[menuService], "spec.ports.0.targetPort"); value != float64(8080) {
		t.Fatal(value)
	}
}

func TestMultiMitigationConflictBeforeWrites(t *testing.T) {
	loop, cluster := menuB3Loop(t)
	loop.Request.RepairPolicy = "recorded-actions"
	loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	before := len(cluster.patches)
	result := multiCall(t, loop, `[{"option_id":"ScaleWorkload","parameters":{"ref":"Deployment/app/frontend","replicas":"1"}},{"option_id":"RestartRollout","parameters":{"ref":"Deployment/app/frontend","reason":"restart"}}]`)
	if result.Report == nil || result.Report.ReasonCode != "write_set_conflict" || len(cluster.patches) != before || loop.LoopsUsed() != 0 {
		t.Fatalf("overlap not refused atomically: %+v writes=%d", result, len(cluster.patches)-before)
	}
}

func TestMultiMitigationPendingReadsAndOverlap(t *testing.T) {
	loop, _ := menuB3Loop(t)
	loop.Request.RepairPolicy = "recorded-actions"
	loop.Executor.Verifier = planFakeVerifier{true}
	loop.Executor.Rollouts = peerBlockedRollout{entered: make(chan struct{}, 1)}
	loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	started := time.Now()
	result := multiCall(t, loop, `[{"option_id":"ScaleWorkload","parameters":{"ref":"Deployment/app/frontend","replicas":"1"}}]`)
	if time.Since(started) > time.Second || result.RolloutPending == nil || result.ApplicationID == "" {
		t.Fatalf("pending blocked: %+v", result)
	}
	if err := wire.Validate(result.Report); err != nil {
		t.Fatalf("invalid typed pending report: %v", err)
	}
	for _, command := range []string{"status", "detect"} {
		if response := loop.Handle(context.Background(), ToolRequest{Command: command}); response.Error != "" {
			t.Fatalf("%s unavailable: %s", command, response.Error)
		}
	}
	overlap := multiCall(t, loop, `[{"option_id":"ScaleWorkload","parameters":{"ref":"Deployment/app/frontend","replicas":"2"}}]`)
	if overlap.Report == nil || overlap.Report.ReasonCode != "pending_write_conflict" || loop.LoopsUsed() != 1 {
		t.Fatalf("pending conflict not refused: %+v", overlap)
	}
	disjoint := multiCall(t, loop, `[{"option_id":"SetField","parameters":{"ref":"Service/app/backend","path":"spec.ports[0].targetPort","value":"8080","reason":"restore route"}}]`)
	// No verification separates the two batches: one act→verify loop.
	if disjoint.Error != "" || loop.LoopsUsed() != 1 {
		t.Fatalf("disjoint repair refused: %+v", disjoint)
	}
}

func TestMultiMitigationPendingReservationSurvivesRestart(t *testing.T) {
	loop, _ := menuB3Loop(t)
	loop.Request.RepairPolicy = "recorded-actions"
	loop.Executor.Verifier = planFakeVerifier{true}
	loop.Executor.Rollouts = peerBlockedRollout{entered: make(chan struct{}, 1)}
	loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	var saved []byte
	loop.saveToolState = func(_ context.Context, payload []byte) error { saved = append([]byte(nil), payload...); return nil }
	first := multiCall(t, loop, `[{"option_id":"ScaleWorkload","parameters":{"ref":"Deployment/app/frontend","replicas":"1"}}]`)
	restored := NewResponderLoop(loop.Request, loop.Programs, loop.Provider, loop.Detectors, loop.Executor, 5)
	restored.MenuConfig = loop.MenuConfig
	if err := restored.restoreToolSnapshot(saved); err != nil {
		t.Fatal(err)
	}
	refused := multiCall(t, restored, `[{"option_id":"ScaleWorkload","parameters":{"ref":"Deployment/app/frontend","replicas":"2"}}]`)
	if refused.Report == nil || refused.Report.ReasonCode != "pending_write_conflict" || restored.LoopsUsed() != 1 {
		t.Fatalf("restart lost reservation: %+v", refused)
	}
	if len(restored.applications) != 0 {
		t.Fatal("restart restored write authority")
	}
	if first.ApplicationID == "" {
		t.Fatal("pending omitted identity")
	}
}

func TestMultiMitigationPendingReportSettlesWithoutAnotherWrite(t *testing.T) {
	loop, cluster := menuB3Loop(t)
	loop.Request.RepairPolicy = "recorded-actions"
	loop.Executor.Verifier = planFakeVerifier{true}
	loop.Executor.Rollouts = peerBlockedRollout{entered: make(chan struct{}, 1)}
	loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	first := multiCall(t, loop, `[{"option_id":"ScaleWorkload","parameters":{"ref":"Deployment/app/frontend","replicas":"1"}}]`)
	before := len(cluster.patches)
	loop.Executor.Rollouts = &planFakeRollout{}
	settled := loop.Handle(context.Background(), ToolRequest{Command: "report", ReportQuery: &contracts.MitigationReportQuery{AttemptId: first.Report.AttemptId}})
	if settled.Report == nil || settled.Report.Status != contracts.MitigationReport_STATUS_SUCCESS || len(settled.MenuExecution) != 1 || settled.MenuExecution[0].Pending || len(cluster.patches) != before {
		t.Fatalf("settled report or write count incorrect: %+v", settled)
	}
}

func TestMultiMitigationTransportServesReadsWhilePending(t *testing.T) {
	loop, cluster := menuB3Loop(t)
	loop.Request.RepairPolicy = "recorded-actions"
	loop.Executor.Verifier = planFakeVerifier{true}
	loop.Executor.Source = planFakeSource{source: cluster.objects[menuService]}
	loop.Executor.Rollouts = peerBlockedRollout{entered: make(chan struct{}, 1)}
	directory := t.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	server := SessionToolServer{Directory: directory, Handle: loop.Handle, PollInterval: time.Millisecond}
	if err := server.Prepare(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	t.Cleanup(func() { cancel(); <-done; _ = server.Close() })
	transport := func(payload string) map[string]any {
		t.Helper()
		name := fmt.Sprintf("scripted-%d.json", time.Now().UnixNano())
		if err := os.WriteFile(filepath.Join(directory, "requests", name), []byte(payload), 0600); err != nil {
			t.Fatal(err)
		}
		timeout := time.After(time.Second)
		for {
			data, err := os.ReadFile(filepath.Join(directory, "responses", name))
			if err == nil {
				var response map[string]any
				if err = json.Unmarshal(data, &response); err != nil {
					t.Fatal(err)
				}
				return response
			}
			if !os.IsNotExist(err) {
				t.Fatal(err)
			}
			select {
			case <-timeout:
				t.Fatal("tool server blocked during pending rollout")
			case <-time.After(time.Millisecond):
			}
		}
	}
	transport(`{"command":"menu"}`)
	started := time.Now()
	pending := transport(`{"command":"mitigate","selection_set":{"version":1,"options":[{"option_id":"ScaleWorkload","parameters":{"ref":"Deployment/app/frontend","replicas":"1"}}]}}`)
	if pending["rollout_pending"] == nil || pending["application_id"] == nil || time.Since(started) > 500*time.Millisecond {
		t.Fatalf("rollout did not return immediately: %+v", pending)
	}
	for _, command := range []string{"status", "detect", "verify", "menu"} {
		response := transport(`{"command":"` + command + `"}`)
		if response["error"] != nil {
			t.Fatalf("read-only %s failed while pending: %+v", command, response)
		}
	}
	response := transport(`{"command":"source","ref":"Service/app/backend"}`)
	if response["error"] != nil {
		t.Fatalf("source unavailable while pending: %+v", response)
	}
	report := pending["report"].(map[string]any)
	query, _ := json.Marshal(map[string]any{"command": "report", "report_query": map[string]any{"attempt_id": report["attempt_id"]}})
	if response := transport(string(query)); response["error"] != nil {
		t.Fatal(response)
	}
	overlap := transport(`{"command":"mitigate","selection_set":{"version":1,"options":[{"option_id":"ScaleWorkload","parameters":{"ref":"Deployment/app/frontend","replicas":"2"}}]}}`)
	if overlap["report"].(map[string]any)["reason_code"] != "pending_write_conflict" {
		t.Fatalf("overlap wrote while pending: %+v", overlap)
	}
}

func multiCLIRefusal(t *testing.T, directory, option, reason string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "uv", "run", "python", "-m", "sdo.agents.responder.tools_cli", "--json", "mitigate", "--option", option)
	command.Dir = filepath.Join("..", "..", "..")
	command.Env = append(os.Environ(), "SDO_TOOLS_DIR="+directory)
	output, err := command.Output()
	if err == nil {
		t.Fatalf("overlapping CLI mutation was not refused: %s", output)
	}
	var response struct {
		Report struct {
			ReasonCode string `json:"reason_code"`
		} `json:"report"`
	}
	if err := json.Unmarshal(fullToolAction(t, []byte(strings.SplitN(string(output), "\n", 2)[0])), &response); err != nil {
		t.Fatalf("invalid refusal: %s %v", output, err)
	}
	if response.Report.ReasonCode != reason {
		t.Fatalf("wrong typed refusal: %s", output)
	}
}

func TestMultiMitigationSameOptionHasIndependentInputsAndReceipts(t *testing.T) {
	loop, cluster := menuB3Loop(t)
	loop.Request.RepairPolicy = "recorded-actions"
	loop.Executor.Verifier = planFakeVerifier{true}
	other := menuDeployment
	other.Name = "second"
	cluster.objects[other] = cluster.objects[menuDeployment].DeepCopy()
	cluster.objects[other].SetName(other.Name)
	cluster.objects[other].SetUID("second-uid")
	loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	response := multiCall(t, loop, `[{"option_id":"ScaleWorkload","parameters":{"ref":"Deployment/app/frontend","replicas":"2"}},{"option_id":"ScaleWorkload","parameters":{"ref":"Deployment/app/second","replicas":"3"}}]`)
	if response.Error != "" || len(response.OptionReports) != 2 || loop.LoopsUsed() != 1 {
		t.Fatalf("repeated option lost inputs: %+v", response)
	}
	for index, ref := range []sdk.ObjectRef{menuDeployment, other} {
		if value, _ := testField(cluster.objects[ref], "spec.replicas"); value != float64(index+2) {
			t.Fatalf("wrong per-option replicas: %v", value)
		}
		report := response.OptionReports[index]
		if report.OptionId != "ScaleWorkload" || report.ApplicationId == "" || report.Status != contracts.MitigationReport_STATUS_SUCCESS {
			t.Fatalf("invalid per-option receipt: %v", report)
		}
		if err := wire.Validate(report); err != nil {
			t.Fatal(err)
		}
	}
	if response.OptionReports[0].ApplicationId == response.OptionReports[1].ApplicationId {
		t.Fatal("options share application identity")
	}
}

func TestMultiMitigationCanonicalAPIWriteConflictBeforeWrites(t *testing.T) {
	for _, mismatch := range []bool{false, true} {
		t.Run(fmt.Sprint(mismatch), func(t *testing.T) {
			loop, _ := menuB3Loop(t)
			client := fake.NewSimpleClientset()
			client.Resources = []*metav1.APIResourceList{{GroupVersion: "apps/v1", APIResources: []metav1.APIResource{{Name: "deployments", Kind: "Deployment", Namespaced: true}}}}
			loop.Executor.Client = TypedPlanClient{Client: client}
			raw := program.KubernetesRequest{Method: "PATCH", Path: "/apis/apps/v1/namespaces/app/deployments/frontend/scale", Body: []byte(`{"spec":{"replicas":2}}`), Reason: "restore scale"}
			want := "write_set_conflict"
			if mismatch {
				raw.Ref = menuDeployment
				raw.Ref.Name = "wrong"
				want = "write_set_unknown"
			}
			loop.MenuConfig.Plans = []ResponderMenuPlan{{ID: "raw", ProgramID: "raw", Plan: program.Plan{Actions: []program.Action{raw}}}, {ID: "scale", ProgramID: "scale", Plan: program.Plan{Actions: []program.Action{program.ScaleWorkload{Ref: menuDeployment, Replicas: testPlanValue(3)}}}}}
			loop.Handle(context.Background(), ToolRequest{Command: "menu"})
			response := multiCall(t, loop, `[{"option_id":"raw"},{"option_id":"scale"}]`)
			if response.Report == nil || response.Report.ReasonCode != want || loop.LoopsUsed() != 0 {
				t.Fatalf("canonical API write escaped batch guard: %+v", response)
			}
			for _, action := range client.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" {
					t.Fatalf("conflicting option crossed write boundary: %+v", action)
				}
			}
		})
	}
}

func TestMultiMitigationProductionRolloutProbeReturnsImmediately(t *testing.T) {
	replicas := int32(1)
	client := fake.NewSimpleClientset(&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: menuDeployment.Name, Namespace: "app", UID: "pending-deployment", Generation: 2}, Spec: appsv1.DeploymentSpec{Replicas: &replicas}, Status: appsv1.DeploymentStatus{ObservedGeneration: 1}})
	executor := PlanExecutor{PollRollouts: true, Rollouts: KubernetesPlanRollouts{Client: client}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	started := time.Now()
	pending, err := executor.waitForRollouts(ctx, IncidentRequest{ResponseDeadline: time.Now().Add(time.Minute)}, []sdk.ObjectRef{menuDeployment})
	if pending == nil || err == nil || time.Since(started) > 500*time.Millisecond {
		t.Fatalf("production rollout waited instead of observing once: %+v %v", pending, err)
	}
	gets := 0
	for _, action := range client.Actions() {
		if action.GetVerb() == "get" && action.GetResource().Resource == "deployments" {
			gets++
		}
	}
	if gets != 1 {
		t.Fatalf("expected one rollout sample, got %d", gets)
	}
}

func TestMultiMitigationPendingRolloutBlocksEarlyClosure(t *testing.T) {
	loop, _ := menuB3Loop(t)
	loop.Request.RepairPolicy = "recorded-actions"
	loop.Executor.Verifier = planFakeVerifier{true}
	loop.Executor.Rollouts = peerBlockedRollout{entered: make(chan struct{}, 1)}
	loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	response := multiCall(t, loop, `[{"option_id":"ScaleWorkload","parameters":{"ref":"Deployment/app/frontend","replicas":"1"}}]`)
	if response.RolloutPending == nil {
		t.Fatal("missing pending setup")
	}
	_, release, ok := acquireClosureGuard(loop)
	release()
	if ok {
		t.Fatal("early closure accepted while rollout remained pending")
	}
	loop.Executor.Rollouts = &planFakeRollout{}
	_, release, ok = acquireClosureGuard(loop)
	release()
	if !ok {
		t.Fatal("settled rollout prevented independent closure check")
	}
}

//go:build integration

package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"os"
	"os/exec"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"strings"
	"testing"
	"time"
)

// Full command reports retain the original controller response under actions[].
// Read that complete typed output for assertions; summaries are presentation only.
func fullToolAction(t *testing.T, encoded []byte) []byte {
	t.Helper()
	var envelope struct {
		DetailsFile string            `json:"details_file"`
		Actions     []json.RawMessage `json:"actions"`
	}
	if err := json.Unmarshal(encoded, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.DetailsFile != "" {
		data, err := os.ReadFile(envelope.DetailsFile)
		if err != nil {
			t.Fatal(err)
		}
		encoded = data
		if err = json.Unmarshal(data, &envelope); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Remove(envelope.DetailsFile) })
	}
	if envelope.Actions != nil {
		if len(envelope.Actions) != 1 {
			t.Fatalf("one invocation must retain one complete raw controller response, got %d", len(envelope.Actions))
		}
		return envelope.Actions[0]
	}
	return encoded
}

func fastloopTool(t *testing.T, directory string, args ...string) ToolResponse {
	t.Helper()
	// Include Python startup and cold JIT compilation under the race-enabled CI
	// shard. Eight seconds can expire even after the CLI emits its response.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	prefix := []string{"run", "python", "-m", "sdo.agents.responder.tools_cli"}
	compact := os.Getenv("SDO_SCRIPTED_COMPACT_OUTPUT") == "1"
	if !compact {
		prefix = append(prefix, "--json")
	}
	command := exec.CommandContext(ctx, "uv", append(prefix, args...)...)
	command.Dir = filepath.Join("..", "..", "..")
	command.Env = append(os.Environ(), "SDO_TOOLS_DIR="+directory)
	command.WaitDelay = time.Second
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		t.Fatalf("sdo %v: %v (context: %v): stdout: %s; stderr: %s", args, err, ctx.Err(), output, stderr.String())
	}
	var response ToolResponse
	encoded := []byte(strings.SplitN(string(output), "\n", 2)[0])
	if compact {
		parts := strings.SplitN(string(output), "Full JSON: ", 2)
		if len(parts) != 2 {
			t.Fatalf("compact sdo %v must name complete JSON: %s", args, output)
		}
		detailsFile := strings.TrimSpace(strings.SplitN(parts[1], "\n", 2)[0])
		if len([]rune(string(output))) > 3001 {
			t.Fatalf("compact sdo %s: %d characters exceeds 3001", args[0], len([]rune(string(output))))
		}
		t.Logf("compact sdo %s stdout=%d characters", args[0], len([]rune(string(output))))
		if destination := os.Getenv("SDO_SCRIPTED_TOOL_RESPONSES"); destination != "" {
			if err := os.WriteFile(filepath.Join(destination, args[0]+".summary.txt"), output, 0600); err != nil {
				t.Fatal(err)
			}
		}
		encoded, err = os.ReadFile(detailsFile)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Remove(detailsFile) })
		encoded = fullToolAction(t, encoded)
		var full ToolResponse
		if err := json.Unmarshal(encoded, &full); err != nil {
			t.Fatal(err)
		}
		mutation := args[0] == "mitigate" || args[0] == "apply" || args[0] == "act"
		if mutation {
			if len(output) > 2048 {
				t.Fatalf("mutation summary exceeds 2KiB: %d bytes", len(output))
			}
			if !strings.Contains(string(output), "Incident:") || !strings.Contains(string(output), "incident_verified:") {
				t.Fatal("mutation summary omitted closure status")
			}
			if full.IncidentVerified == nil && !strings.Contains(string(output), "Incident: NOT verified") {
				t.Fatal("action health was promoted to incident closure")
			}
		} else if full.Healthy && !strings.Contains(string(output), "healthy=true") {
			t.Fatal("compact output omitted verified health")
		}
	}
	encoded = fullToolAction(t, encoded)
	if err := json.Unmarshal(encoded, &response); err != nil {
		t.Fatalf("decode sdo %v: %v: stdout: %s; stderr: %s", args, err, output, stderr.String())
	}
	if destination := os.Getenv("SDO_SCRIPTED_TOOL_RESPONSES"); destination != "" {
		name := args[0]
		if name == "plan" {
			name += "." + args[1]
		}
		if err := os.WriteFile(filepath.Join(destination, name+".json"), encoded, 0600); err != nil {
			t.Fatal(err)
		}
	}
	return response
}

func TestHermeticResponderProgramReuseThroughRealToolTransport(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := fastloopClient()
	provider := fastloopObjects{client: client}
	root := t.TempDir()
	port := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	image := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
	for incident := 1; incident <= 2; incident++ {
		id := "incident-1"
		if incident == 2 {
			id = "incident-2"
		}
		if incident == 2 {
			// A new object pair has the same fault classes as the first pair.
			service, _ := client.CoreV1().Services("app").Get(ctx, "api", metav1.GetOptions{})
			service.Name = "peer"
			service.Spec.Ports[0].TargetPort = intstr.FromInt32(9999)
			_, _ = client.CoreV1().Services("app").Create(ctx, service, metav1.CreateOptions{})
			deployment, _ := client.AppsV1().Deployments("app").Get(ctx, "api", metav1.GetOptions{})
			deployment.Name = "peer"
			deployment.Spec.Template.Spec.Containers[0].Image = "missing:v1"
			deployment.Status = appsv1.DeploymentStatus{}
			_, _ = client.AppsV1().Deployments("app").Create(ctx, deployment, metav1.CreateOptions{})
			pod, _ := client.CoreV1().Pods("app").Get(ctx, "api-pod", metav1.GetOptions{})
			pod.Name = "peer-pod"
			pod.Labels = map[string]string{"app": "peer"}
			pod.Spec.Containers[0].Image = "missing:v1"
			pod.Status = corev1.PodStatus{Phase: corev1.PodPending, ContainerStatuses: []corev1.ContainerStatus{{Name: "api", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}}}}}
			_, _ = client.CoreV1().Pods("app").Create(ctx, pod, metav1.CreateOptions{})
			port.Name, image.Name = "peer", "peer"
		}
		view, err := provider.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		findings, err := (fastloopDetector{}).Detect(ctx, view)
		if err != nil || len(findings) < 2 {
			t.Fatalf("incident %d findings: %+v %v", incident, findings, err)
		}
		var registry program.ProgramRegistry
		if incident == 1 {
			registry, err = program.NewRegistry([]program.Diagnoser{fastloopImageProgram{}}, []program.Mitigator{fastloopImageProgram{}})
			if err != nil {
				t.Fatal(err)
			}
		} else {
			registry, err = program.NewRegistry([]program.Diagnoser{fastloopPortProgram{}, fastloopImageProgram{}}, []program.Mitigator{fastloopPortProgram{}, fastloopImageProgram{}})
			if err != nil {
				t.Fatal(err)
			}
		}
		loop := NewResponderLoop(IncidentRequest{IncidentID: id, Namespace: "app", RepairPolicy: "recorded-actions", Findings: findings}, registry, provider, []sdk.Detector{fastloopDetector{}}, PlanExecutor{Client: TypedPlanClient{Client: client}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}, 5)
		now := time.Now().UTC()
		controller, err := NewController(testControllerConfig(), []sdk.Detector{fastloopDetector{}}, provider, &verificationRetryDispatcher{}, now)
		if err != nil {
			t.Fatal(err)
		}
		controller.currentIncidentRequest = &loop.Request
		controller.incidentOpen = true
		bindRecoveryFixture(controller, loop, now)
		if err := controller.Step(ctx, now, nil); err != nil {
			t.Fatal(err)
		}
		loop.CorpusSessionRoot = root
		loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView {
			return &sdktest.ReplayReadView{Snapshot: snapshot.(sdktest.Snapshot)}
		}
		initial := loop.InitialDiagnose(ctx)
		if incident == 1 {
			if len(initial) != 1 || initial[0].Verdict.Result != program.Unknown {
				t.Fatalf("container-creating diagnosis: %+v", initial)
			}
			fastloopPodWaiting(t, ctx, client, "api-pod", "ErrImagePull")
			fastloopPodWaiting(t, ctx, client, "api-pod", "ImagePullBackOff")
		}
		tools := filepath.Join(t.TempDir(), "tools")
		server := SessionToolServer{Directory: tools, Handle: loop.Handle, PollInterval: time.Millisecond}
		if err := server.Prepare(); err != nil {
			t.Fatal(err)
		}
		serverCtx, stop := context.WithCancel(ctx)
		serverErr := make(chan error, 1)
		served := make(chan struct{})
		// A failing assertion must not leave the server writing under tools.
		t.Cleanup(func() { stop(); <-served })
		go func() {
			defer close(served)
			err := server.Serve(serverCtx)
			if err != nil {
				t.Logf("tool server exited: %v", err)
			}
			serverErr <- err
		}()
		if incident == 2 {
			fastloopTool(t, tools, "status")
		}
		fastloopTool(t, tools, "diagnose")
		if incident == 1 {
			if got := loop.ProgramRuns(); got[len(got)-1].Verdict.Result != program.Match {
				t.Fatalf("diagnosis after the pull failure settled: %+v", got)
			}
			if response := loop.Handle(ctx, ToolRequest{Command: "pre-apply"}); response.Error != "" {
				t.Fatal(response.Error)
			}
			for _, ref := range []sdk.ObjectRef{port, image} {
				patch := []byte(`{"spec":{"ports":[{"port":80,"targetPort":8080}]}}`)
				if ref.Kind == "Deployment" {
					patch = []byte(`{"spec":{"template":{"spec":{"containers":[{"name":"api","image":"good:v1","ports":[{"containerPort":8080}]}]}}}}`)
				}
				if err := (TypedPlanClient{Client: client}).Patch(ctx, ref, types.MergePatchType, patch); err != nil {
					t.Fatal(err)
				}
			}
		} else {
			before := loop.ProgramRuns()
			if len(before) == 0 || before[0].Verdict.Result != program.Match {
				t.Fatalf("reused program did not match: %+v", before)
			}
			first := fastloopTool(t, tools, "plan", "service-targetport-mismatch", "--bind", "service=Service/app/peer")
			second := fastloopTool(t, tools, "plan", "image-pull-failure", "--bind", "deployment=Deployment/app/peer")
			applied := fastloopTool(t, tools, "apply", first.PlanID, second.PlanID)
			if applied.ApplicationID == "" || len(loop.PlanApplications()) != 1 {
				t.Fatalf("plans were not applied: %+v", applied)
			}
			state := &contracts.ControllerState{Version: RuntimeStateVersion, IncidentResult: mustProject(t, incidentResultContract, IncidentResult{SchemaVersion: ProtocolSchemaVersion, IncidentID: id, Status: IncidentCompleted, Timing: TimingMetrics{StartedAt: time.Unix(1, 0), CompletedAt: time.Unix(2, 0)}, PlanApplications: loop.PlanApplications()})}
			store := lifecycleio.NewConfigMapStateStore(client, "app", "controller-state")
			if _, err := store.Save(ctx, state, ""); err != nil {
				t.Fatalf("persist plan outcome: %v", err)
			}
			reloaded, _, err := lifecycleio.NewConfigMapStateStore(client, "app", "controller-state").Load(ctx)
			if err != nil || len(incidentResultState(reloaded.IncidentResult).PlanApplications[0].Result.ActionsApplied) != 2 || len(incidentResultState(reloaded.IncidentResult).PlanApplications[0].Result.PreImages) != 0 {
				t.Fatalf("reload plan outcome: %+v %v", reloaded.IncidentResult, err)
			}
		}
		pending := fastloopTool(t, tools, "verify")
		if pending.Healthy || pending.IncidentVerified != nil {
			t.Fatalf("clear health bypassed the controller proof: %+v", pending)
		}
		if destination := os.Getenv("SDO_SCRIPTED_TOOL_RESPONSES"); destination != "" {
			encoded, err := json.Marshal(pending)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(destination, "verify-pending.json"), encoded, 0600); err != nil {
				t.Fatal(err)
			}
		}
		// Model the independent workload controller's observed completion in
		// the fake Kubernetes API; repairing a template alone is not recovery.
		deployment, err := client.AppsV1().Deployments("app").Get(ctx, image.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		one := int32(1)
		deployment.Spec.Replicas = &one
		deployment.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"app": image.Name}}
		deployment.Spec.Template.Labels = map[string]string{"app": image.Name}
		deployment.Status = appsv1.DeploymentStatus{ObservedGeneration: deployment.Generation, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}
		if _, err := client.AppsV1().Deployments("app").Update(ctx, deployment, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		pod, err := client.CoreV1().Pods("app").Get(ctx, image.Name+"-pod", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		pod.Spec = *deployment.Spec.Template.Spec.DeepCopy()
		pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
		if _, err := client.CoreV1().Pods("app").Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		assertRecoveryFixtureWindow(t, controller, ctx, time.Now().UTC().Add(time.Second))
		verified := fastloopTool(t, tools, "verify")
		if !verified.Healthy || len(verified.Findings) != 0 {
			t.Fatalf("incident %d did not verify: %+v", incident, verified)
		}
		stop()
		if err := <-serverErr; err != nil {
			t.Fatalf("tool server: %v", err)
		}
		for _, path := range []string{tools, filepath.Join(tools, "requests"), filepath.Join(tools, "responses")} {
			if err := os.Chmod(path, 02770); err != nil {
				t.Fatal(err)
			}
		}
		if err := server.Prepare(); err != nil {
			t.Fatalf("tool session failed to reprepare after worker restart: %v", err)
		}
		for _, call := range loop.ToolCalls() {
			if strings.Contains(call.ResponseJSON, ".sdo/") || len(call.ResponseJSON) > maxRecordedToolResponse {
				t.Fatalf("unbounded or physical tool evidence: %+v", call)
			}
		}
		fixed, _ := provider.Snapshot(ctx)
		if err := memory.CaptureIncidentSnapshot(root, id, "fixed", fixed, "app"); err != nil {
			t.Fatal(err)
		}
		captureDir, err := memory.IncidentCorpusDir(root, id)
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"open@detect.json", "open@pre-apply.json", "fixed.json"} {
			if _, err := os.Stat(filepath.Join(captureDir, name)); err != nil {
				t.Fatalf("missing %s: %v", name, err)
			}
		}
		for _, name := range []string{"open@settle.json"} {
			if _, err := os.Stat(filepath.Join(captureDir, name)); !os.IsNotExist(err) {
				t.Fatalf("%s must no longer be captured: %v", name, err)
			}
		}
		if incident == 1 {
			wire, err := os.ReadFile(filepath.Join(captureDir, "open@detect.json"))
			if err != nil || !strings.Contains(string(wire), "ContainerCreating") {
				t.Fatalf("open@detect was captured at the wrong phase: %v", err)
			}
		}
	}
}

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

func fastloopTool(t *testing.T, directory string, args ...string) ToolResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "uv", append([]string{"run", "python", "-m", "sdo.agents.responder.tools_cli"}, args...)...)
	command.Dir = filepath.Join("..", "..", "..")
	command.Env = append(os.Environ(), "SDO_TOOLS_DIR="+directory)
	command.WaitDelay = time.Second
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		t.Fatalf("sdo %v: %v: stdout: %s; stderr: %s", args, err, output, stderr.String())
	}
	var response ToolResponse
	encoded := []byte(strings.SplitN(string(output), "\n", 2)[0])
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
			_, _ = client.AppsV1().Deployments("app").Create(ctx, deployment, metav1.CreateOptions{})
			pod, _ := client.CoreV1().Pods("app").Get(ctx, "api-pod", metav1.GetOptions{})
			pod.Name = "peer-pod"
			pod.Labels = map[string]string{"app": "peer"}
			pod.Status.ContainerStatuses[0].State.Waiting.Reason = "ImagePullBackOff"
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
			loop.DiagnosisSettleDelay = time.Millisecond
			loop.RetryInitialDiagnose(ctx)
			if got := loop.ProgramRuns(); len(got) != 2 || got[1].Verdict.Result != program.Match {
				t.Fatalf("settled diagnosis: %+v", got)
			}
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
		names := []string{"open@detect.json", "open@pre-apply.json", "fixed.json"}
		if incident == 1 {
			names = append(names, "open@settle.json")
		}
		for _, name := range names {
			if _, err := os.Stat(filepath.Join(captureDir, name)); err != nil {
				t.Fatalf("missing %s: %v", name, err)
			}
		}
		if incident == 1 {
			for name, want := range map[string]string{"open@detect.json": "ContainerCreating", "open@settle.json": "ImagePullBackOff", "open@pre-apply.json": "ImagePullBackOff"} {
				wire, err := os.ReadFile(filepath.Join(captureDir, name))
				if err != nil || !strings.Contains(string(wire), want) {
					t.Fatalf("%s was captured at the wrong phase: %v", name, err)
				}
			}
		}
	}
}

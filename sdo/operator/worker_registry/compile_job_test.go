package worker_registry_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/builder"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/toolchain"
	"sdo.dev/operator/worker_registry"
)

// Resolve the actual production Job's compiler mounts and run the real learned
// program test/replay builder with those files. No cluster, image or model.
func TestIncidentJobCompilerMountsRunLearnedProgramTestsAndReplay(t *testing.T) {
	testutil.OfflineGo(t, "..")
	operator, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	repository := t.TempDir()
	app := filepath.Join(repository, "proposal")
	fixture := filepath.Join(operator, "..", "..", "tests/fixtures/sdo/playbook_program_app")
	if output, err := exec.Command("cp", "-a", fixture, app).CombinedOutput(); err != nil {
		t.Fatalf("fixture: %v %s", err, output)
	}

	payload, _ := anypb.New(&contracts.IncidentRequest{IncidentId: "compile"})
	task := &contracts.AgentTask{SchemaVersion: worker_registry.SchemaVersion, TaskId: "compile", AgentId: "compile-agent", TaskType: "incident", Request: payload, Workspace: &contracts.ProposalWorkspace{ProposalId: "compile", CheckoutPath: app, BaseCommit: "accepted"}}
	registry, err := worker_registry.New(&contracts.AgentRegistration{SchemaVersion: worker_registry.SchemaVersion, AgentId: "compile-agent", Capabilities: []*contracts.AgentCapability{{TaskType: "incident", RequestTypeUrl: worker_registry.TypeURL(&contracts.IncidentRequest{}), ResultTypeUrl: worker_registry.TypeURL(&contracts.IncidentResult{})}}, Launch: &contracts.AgentRegistration_KubernetesJob{KubernetesJob: &contracts.KubernetesJobLaunch{Image: "worker:slim", Command: []string{"go", "test", "./..."}}}})
	if err != nil {
		t.Fatal(err)
	}
	client := fake.NewSimpleClientset()
	e := worker_registry.KubernetesExecutor{Client: client, Namespace: "control", Registry: registry, RepositoryPVC: "repo", RepositoryMountPath: repository, ToolchainImage: "controller@sha256:test", PollInterval: time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := e.Execute(ctx, task); done <- err }()
	t.Cleanup(func() { cancel(); <-done })
	deadline := time.Now().Add(5 * time.Second)
	compiler, cache := "", ""
	for time.Now().Before(deadline) {
		job, err := client.BatchV1().Jobs("control").Get(ctx, worker_registry.JobName(task.TaskId), metav1.GetOptions{})
		if err == nil {
			for _, m := range job.Spec.Template.Spec.Containers[0].VolumeMounts {
				if m.MountPath == "/usr/local/go" || m.MountPath == "/go" {
					if m.Name != "repository" || !m.ReadOnly {
						t.Fatalf("unsafe compiler mount: %+v", m)
					}
					if m.MountPath == "/usr/local/go" {
						compiler = filepath.Join(repository, m.SubPath)
					} else {
						cache = filepath.Join(repository, m.SubPath)
					}
				}
			}
			break
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if compiler == "" || cache == "" {
		t.Fatal("real Job did not provide offline compiler mounts")
	}
	if !strings.Contains(compiler, toolchain.SharedSubPath(e.ToolchainImage)) {
		t.Fatal("compiler was not image-bound")
	}
	if err := os.MkdirAll(filepath.Dir(compiler), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(runtime.GOROOT(), compiler); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(cache, "pkg"), 0700); err != nil {
		t.Fatal(err)
	}
	modcache, err := exec.Command("go", "env", "GOMODCACHE").Output()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(strings.TrimSpace(string(modcache)), filepath.Join(cache, "pkg/mod")); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", filepath.Join(compiler, "bin")+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("GOROOT", compiler)
	t.Setenv("GOMODCACHE", filepath.Join(cache, "pkg/mod"))
	t.Setenv("SDO_GO_CACHE_SEED", "")
	workspace, err := builder.BuildWorkspace(context.Background(), builder.Config{ApplicationRoot: app, DiagnosticsRoot: filepath.Join(app, ".sdo/diagnostics"), OperatorRoot: operator})
	if err != nil {
		t.Fatal(err)
	}
	defer workspace.Close()
	if _, err := workspace.ValidateAndBuild(context.Background()); err != nil {
		t.Fatal("learned compile/test", err)
	}
	replay, binary, err := builder.BuildReplay(context.Background(), builder.Config{ApplicationRoot: app, DiagnosticsRoot: filepath.Join(app, ".sdo/diagnostics"), OperatorRoot: operator})
	if err != nil {
		t.Fatal(err)
	}
	defer replay.Close()
	output, err := exec.Command(binary, "-fixtures", filepath.Join(app, ".sdo/corpus"), "-source", filepath.Join(operator, "builder/testdata/replay-accepted.yaml"), "-repeat", "2").CombinedOutput()
	if err != nil || !strings.Contains(string(output), `"status":"replayed"`) {
		t.Fatalf("learned replay: %v %s", err, output)
	}
}

package operatorcli

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/deployment/buildtools"
	"sdo.dev/operator/worker_registry"
)

func yieldPoll(ctx context.Context, _ time.Duration) error {
	runtime.Gosched()
	return ctx.Err()
}

func deployerTask(t *testing.T, request *contracts.SourceDeploymentRequest) *contracts.AgentTask {
	t.Helper()
	packed, err := anypb.New(request)
	if err != nil {
		t.Fatal(err)
	}
	return &contracts.AgentTask{TaskId: "op-source-deployer-1", Request: packed, Workspace: &contracts.ProposalWorkspace{CheckoutPath: "/repo/worktrees/op-source"}}
}

// While a deployer task runs, its pod gets a session mount with the build
// tool directory, and a request there builds that task's checkout.
func TestDeployerTaskIsServedGosBuildOfItsCheckout(t *testing.T) {
	mount := t.TempDir()
	var built []string
	var checkouts []string
	workers := deployerBuildWorkers{
		KubernetesExecutor: worker_registry.KubernetesExecutor{Environment: map[string]string{"KEEP": "1"}},
		RepositoryMount:    mount,
		Build: func(_ context.Context, checkout string, images []string) (string, bool, error) {
			checkouts = append(checkouts, checkout)
			built = append(built, images...)
			return "  failed  registry.test/app/currency:1\nCMake Error\n", true, nil
		},
	}
	var response buildtools.Response
	workers.execute = func(ctx context.Context, executor worker_registry.KubernetesExecutor, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		// The executor mounts the task's evidence session; the tools sit in it.
		if executor.SessionPVCSubPath != "" || executor.Environment[buildtools.EnvDirectory] != buildtools.PodDirectory || executor.Environment["KEEP"] != "1" {
			t.Errorf("executor session %q env %v", executor.SessionPVCSubPath, executor.Environment)
		}
		session := filepath.Join(mount, worker_registry.TaskEvidenceRoot, worker_registry.TaskEvidenceKey(task.GetTaskId()))
		var err error
		response, err = buildtools.Call(ctx, filepath.Join(session, "tools"), buildtools.Request{Images: []string{"currency"}}, func() string { return "1" }, yieldPoll)
		return &contracts.AgentResult{}, err
	}
	if _, err := workers.Execute(context.Background(), deployerTask(t, &contracts.SourceDeploymentRequest{})); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(built, []string{"currency"}) || !slices.Equal(checkouts, []string{"/repo/worktrees/op-source"}) || !response.Failed || !strings.Contains(response.Text, "CMake Error") {
		t.Fatalf("built %v in %v, response %+v", built, checkouts, response)
	}
	if _, ok := workers.Environment[buildtools.EnvDirectory]; ok {
		t.Fatal("the shared executor environment must not change")
	}
}

// Other lifecycle agents run without the build service.
func TestOtherLifecycleTasksGetNoBuildService(t *testing.T) {
	called := false
	workers := deployerBuildWorkers{RepositoryMount: t.TempDir(), Build: func(context.Context, string, []string) (string, bool, error) { return "", false, nil }}
	workers.execute = func(context.Context, worker_registry.KubernetesExecutor, *contracts.AgentTask) (*contracts.AgentResult, error) {
		called = true
		return nil, nil
	}
	packed, _ := anypb.New(&contracts.HealthJudgeRequest{})
	// A non-deployer task goes straight to the embedded executor, which has
	// no client here, so it fails instead of reaching execute.
	if _, err := workers.Execute(context.Background(), &contracts.AgentTask{TaskId: "judge", Request: packed}); err == nil || called {
		t.Fatalf("err = %v called = %v; a judge task must not be given the build service", err, called)
	}
}

func TestDeploymentBuildPrintsTheReportAndExitsByOutcome(t *testing.T) {
	for name, tc := range map[string]struct {
		build    func(context.Context, []string) (string, bool, error)
		wantCode int
		wantOut  string
		wantErr  string
	}{
		"built":  {func(context.Context, []string) (string, bool, error) { return "  ok      a\n", false, nil }, exitClean, "ok      a", ""},
		"failed": {func(context.Context, []string) (string, bool, error) { return "  failed  a\nstep\n", true, nil }, exitDiagnostics, "failed  a", ""},
		"refused": {func(context.Context, []string) (string, bool, error) {
			return "", false, errors.New(`"x" is not an image this commit builds`)
		}, exitInfra, "", "not an image"},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			server := buildtools.Server{Directory: dir, Build: tc.build, Wait: yieldPoll}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if err := server.Prepare(); err != nil {
				t.Fatal(err)
			}
			go func() { _ = server.Serve(ctx) }()
			var stdout, stderr bytes.Buffer
			getenv := func(key string) string {
				if key == buildtools.EnvDirectory {
					return dir
				}
				return ""
			}
			code := RunDeployment(ctx, []string{"build", "a"}, getenv, &stdout, &stderr)
			if code != tc.wantCode || !strings.Contains(stdout.String(), tc.wantOut) || !strings.Contains(stderr.String(), tc.wantErr) {
				t.Fatalf("code %d stdout %q stderr %q", code, stdout.String(), stderr.String())
			}
		})
	}
}

func TestDeploymentBuildOutsideADeployerTaskSaysSo(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := RunDeployment(context.Background(), []string{"build"}, func(string) string { return "" }, &stdout, &stderr)
	if code != exitInfra || !strings.Contains(stderr.String(), "deployer task") {
		t.Fatalf("code %d stderr %q", code, stderr.String())
	}
}

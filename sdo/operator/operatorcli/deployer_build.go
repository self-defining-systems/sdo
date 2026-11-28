package operatorcli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"path"
	"path/filepath"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/deployment"
	"sdo.dev/operator/deployment/buildtools"
	"sdo.dev/operator/operatorcli/internal/processio"
	"sdo.dev/operator/planfile"
	"sdo.dev/operator/safefs"
	"sdo.dev/operator/worker_registry"
)

// deployerBuildWorkers runs lifecycle agents like the embedded executor and,
// while a deployer task runs, serves its `sdo-operator deployment build`
// requests: Go's own build path (deployment.Deployer.BuildImages, the operate
// Job's BuildKit identity) on that task's checkout. The deployer then gets a
// build's result inside its turn instead of a whole Go deploy round later. The
// tool directory sits in the task's evidence session, which the executor
// mounts in the worker pod.
type deployerBuildWorkers struct {
	worker_registry.KubernetesExecutor
	RepositoryMount string
	Build           func(ctx context.Context, checkout string, images []string, progress func(string)) (string, bool, error)
	// execute runs the prepared executor; nil is its Execute (tests inject it).
	execute func(context.Context, worker_registry.KubernetesExecutor, *contracts.AgentTask) (*contracts.AgentResult, error)
}

func (w deployerBuildWorkers) Execute(ctx context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
	if w.Build == nil || !task.GetRequest().MessageIs(&contracts.SourceDeploymentRequest{}) {
		return w.KubernetesExecutor.Execute(ctx, task)
	}
	checkout := task.GetWorkspace().GetCheckoutPath()
	session := path.Join(worker_registry.TaskEvidenceRoot, worker_registry.TaskEvidenceKey(task.GetTaskId()))
	if err := safefs.MkdirAll(w.RepositoryMount, session, 0o700); err != nil {
		return nil, fmt.Errorf("prepare deployer build tools: %w", err)
	}
	server := buildtools.Server{
		Directory: filepath.Join(w.RepositoryMount, session, "tools"),
		Build: func(ctx context.Context, images []string, progress func(string)) (string, bool, error) {
			return w.Build(ctx, checkout, images, progress)
		},
	}
	if err := server.Prepare(); err != nil {
		return nil, fmt.Errorf("prepare deployer build tools: %w", err)
	}
	executor := w.KubernetesExecutor
	executor.Environment = cloneMap(w.Environment)
	executor.Environment[buildtools.EnvDirectory] = buildtools.PodDirectory
	serving, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- server.Serve(serving) }()
	defer func() {
		stop()
		if err := <-done; err != nil {
			slog.Warn("deployer_build_tools_stopped", "task_id", task.GetTaskId(), "error", err)
		}
	}()
	if w.execute != nil {
		return w.execute(ctx, executor, task)
	}
	return executor.Execute(ctx, task)
}

func cloneMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in)+1)
	for k, v := range in {
		out[k] = v
	}
	return out
}

// imageBuildRecord is where the operate process remembers each image's last
// good turn-time build, below the repository mount; it outlives one deployer
// task, so a correction task's build rebuilds only what changed since.
const imageBuildRecord = ".sdo-runtime/image-builds.json"

// deployerBuild is the build a deployer task's request runs: the checkout's
// override (if any) and Go's deployer, selecting by record when no image is
// named.
func deployerBuild(deployer deployment.Deployer, record deployment.BuildRecord) func(ctx context.Context, checkout string, images []string, progress func(string)) (string, bool, error) {
	return func(ctx context.Context, checkout string, images []string, progress func(string)) (string, bool, error) {
		plan, err := planfile.Read(checkout, planfile.Catalogs{Renderers: deployer.Renderers, Images: deployer.Images})
		switch {
		case errors.Is(err, planfile.ErrAbsent):
			plan = nil
		case err != nil:
			return "", false, fmt.Errorf("the deployment override .sdo/deployment/plan.yaml does not parse: %w", err)
		}
		report, err := deployer.BuildImages(ctx, checkout, plan, images, deployment.BuildOptions{Record: record, Progress: progress})
		if err != nil {
			return "", false, err
		}
		return report.Text(), report.Err != nil, nil
	}
}

// deploymentBuild is `sdo-operator deployment build [IMAGE...]`: one request
// to the operate process serving this deployer task.
func deploymentBuild(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) (int, error) {
	for _, arg := range args {
		if arg == "-h" || arg == "--help" {
			return exitClean, flag.ErrHelp
		}
		if strings.HasPrefix(arg, "-") {
			return exitInfra, fmt.Errorf("unknown flag %q\n%s", arg, DeploymentUsage)
		}
	}
	directory := getenv(buildtools.EnvDirectory)
	if directory == "" {
		return exitInfra, errors.New("no build service: `deployment build` runs inside a deployer task, where SDO serves Go's own image builds")
	}
	response, err := buildtools.Call(ctx, directory, buildtools.Request{Images: args}, processio.RequestID, nil, stdout)
	if err != nil {
		return exitInfra, err
	}
	fmt.Fprint(stdout, "\n"+response.Text)
	if response.Error != "" {
		return exitInfra, errors.New(response.Error)
	}
	if response.Failed {
		return exitDiagnostics, nil
	}
	return exitClean, nil
}

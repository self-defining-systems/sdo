// Package operatorcli translates installation arguments into the controller's
// typed launch contract. The compiled child receives only ControllerConfig.
package operatorcli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"sdo.dev/operator/builder"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/toolchain"
)

type repeatable []string

func (r *repeatable) String() string         { return strings.Join(*r, ",") }
func (r *repeatable) Set(value string) error { *r = append(*r, value); return nil }

// ParseControllerLaunch rejects native/legacy split-brain before any build or
// Kubernetes effect. It is pure so installer Job arguments can be contract
// tested against the same parser shipped in the controller image.
func ParseControllerLaunch(args []string) (builder.RunConfig, error) {
	if len(args) == 0 || args[0] != "controller" {
		return builder.RunConfig{}, errors.New("expected controller subcommand")
	}
	flags := flag.NewFlagSet("sdo-operator controller", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var responseStageOverrides []string
	flags.Func("response-stages", "ordered response stages; overrides the selected code path", func(value string) error { responseStageOverrides = stageOverride(value); return nil })
	codePath := flags.String("code-path", "", "programming-model plugin set: go or shell")
	codePathOverrides := flags.String("code-path-overrides", "", "CodePathOverrides JSON")
	app := flags.String("app", "", "accepted application checkout")
	operatorRoot := flags.String("operator-root", "/opt/sdo/sdo/operator", "installed Go operator modules")
	namespace := flags.String("namespace", "", "application namespace")
	controlNamespace := flags.String("control-namespace", "", "controller namespace")
	application := flags.String("application", "", "application identity")
	responderImage := flags.String("responder-image", "", "registered responder image")
	toolchainImage := flags.String("toolchain-image", "", "trusted controller image supplying Go to workers (required)")
	scriptImage := flags.String("script-image", "", "fixed tool image for app-local RunScript Jobs")
	repositoryPVC := flags.String("repository-pvc", "", "repository PVC")
	repositoryMount := flags.String("repository-mount-path", "", "repository PVC mount")
	repositorySubPath := flags.String("repository-pvc-subpath", "", "repository PVC subpath")
	credentialsSecret := flags.String("credentials-secret", "", "responder credentials secret")
	worktreeRoot := flags.String("worktree-root", "", "broker proposal and ledger root")
	model := flags.String("model", "", "responder model")
	backend := flags.String("responder-backend", "codex", "responder backend")
	replay := flags.String("replay", "", "replay and corpus capture: on (default) or off")
	effort := flags.String("agent-effort", "", "reasoning effort of every agent worker (empty: the shared default)")
	brokerMode := flags.String("broker-mode", "", "broker mode")
	workerMountMode := flags.String("worker-mount-mode", "", "worker mount mode")
	registrations := flags.String("agent-registration-directory", "", "agent registration directory")
	responderID := flags.String("responder-agent-id", "", "responder agent identity; defaults to the agent registered for incident requests")
	sourceCommit := flags.String("source-commit", "", "source commit")
	deployedCommit := flags.String("deployed-commit", "", "deployed commit")
	responseTimeout := flags.Duration("response-timeout", 30*time.Minute, "responder timeout")
	verificationTimeout := flags.Duration("verification-timeout", 5*time.Minute, "verification timeout")
	repairPolicy := flags.String("repair-policy", "recorded-actions", "repair evidence policy")
	reflectionSession := flags.String("reflection-session", "resume", "reflection session mode: fresh or resume")
	reflectionGuidance := flags.String("reflection-guidance", "baseline", "reflection guidance mode")
	forceReflection := flags.Bool("force-reflection", false, "always dispatch reflection after verified closure")
	healthyBaseline := flags.Bool("healthy-baseline", false, "validate incident detectors against recorded healthy snapshots")
	metricsPublishURL := flags.String("metrics-publish-url", "", "SDO Prometheus publishing base URL")
	metricsURL := flags.String("metrics-url", "", "Prometheus-compatible API URL that metric and alert detectors query once per evaluation cycle")
	maxResponseLoops := flags.Int("max-response-loops", 10, "maximum mitigation rounds the responder agent may use per incident (the automatic stage's one apply does not count)")
	diagnosisDeadline := flags.Duration("diagnosis-deadline", 10*time.Second, "shared candidate diagnosis deadline")
	duration := flags.Duration("duration", 0, "bounded controller duration")
	exitAfterClosure := flags.Bool("exit-after-closure", false, "exit after the first acknowledged closure")
	standby := flags.Bool("responder-standby", true, "pre-start a responder; false forces cold Jobs")
	upstreamURL := flags.String("memory-upstream-url", "", "shared remote Git URL the accepted repository replicates")
	upstreamBranch := flags.String("memory-upstream-branch", "", "remote branch for this run's accepted memory")
	upstreamSSH := flags.String("memory-upstream-ssh-dir", "", "read-only directory with the ssh identity and known_hosts for the memory remote")
	var responderEnv repeatable
	flags.Var(&responderEnv, "responder-env", "responder environment NAME=VALUE")
	var deployedManifestPaths repeatable
	flags.Var(&deployedManifestPaths, "deployed-manifest-path", "manifest path selected by the accepted deployment plan")
	if err := flags.Parse(args[1:]); err != nil {
		return builder.RunConfig{}, err
	}

	if flags.NArg() != 0 {
		return builder.RunConfig{}, fmt.Errorf("unexpected controller arguments: %v", flags.Args())
	}
	if err := toolchain.ValidateImage(*toolchainImage); err != nil {
		return builder.RunConfig{}, err
	}
	if *brokerMode != "native" || *workerMountMode != "proposal-only" {
		return builder.RunConfig{}, errors.New("native broker requires proposal-only worker mounts")
	}
	if *duration < 0 {
		return builder.RunConfig{}, errors.New("duration must be nonnegative")
	}
	if *app == "" || !filepath.IsAbs(*app) || *operatorRoot == "" || *namespace == "" || *responderImage == "" || *repositoryPVC == "" || *repositoryMount == "" ||
		*credentialsSecret == "" || *worktreeRoot == "" || *model == "" || *backend == "" || *responseTimeout <= 0 || *verificationTimeout <= 0 {
		return builder.RunConfig{}, errors.New("native launch requires application, namespace, images, repository, credentials, and responder identity")
	}
	if !filepath.IsAbs(*repositoryMount) || !filepath.IsAbs(*worktreeRoot) {
		return builder.RunConfig{}, errors.New("repository mount and proposal root must be absolute")
	}
	if *application == "" {
		*application = filepath.Base(*app)
	}
	selectedCodePath, err := parseCodePathSelection(*codePath, *codePathOverrides)
	if err != nil {
		return builder.RunConfig{}, err
	}
	addResponseStageOverride(&selectedCodePath, responseStageOverrides)
	launch := &contracts.ControllerConfig{
		CodePath:         selectedCodePath,
		ResponderStandby: standby,
		Namespace:        *namespace, ControlNamespace: *controlNamespace, AppRoot: *app, Application: *application,
		SourceCommit: *sourceCommit, DeployedCommit: *deployedCommit,
		DeployedManifestPaths: deployedManifestPaths,
		Dispatcher:            "python3", DispatcherMode: "job", DispatcherArgs: []string{"-m", "sdo.agents.responder.worker"},
		ResponderImage: *responderImage, ResponderEnv: responderEnv,
		RepositoryPvc: *repositoryPVC, RepositoryMountPath: *repositoryMount, RepositoryPvcSubpath: *repositorySubPath,
		ResponderCredentialsSecret: *credentialsSecret, BrokerWorktreeRoot: *worktreeRoot,
		ResponseTimeout: durationpb.New(*responseTimeout), VerificationTimeout: durationpb.New(*verificationTimeout),
		RepairPolicy: *repairPolicy, MaxResponseLoops: int32(*maxResponseLoops), DiagnosisDeadline: durationpb.New(*diagnosisDeadline), LeaseName: "sdo-controller",
		ExitAfterClosure: *exitAfterClosure,
		ScriptImage:      *scriptImage, ToolchainImage: *toolchainImage, AgentRegistrationDirectory: *registrations, ResponderAgentId: *responderID,
		BrokerMode: *brokerMode, WorkerMountMode: *workerMountMode,
		ResponderBackend: *backend, ResponderModel: *model, AgentEffort: *effort, Replay: *replay, ReflectionSession: *reflectionSession, ReflectionGuidance: *reflectionGuidance, ForceReflection: *forceReflection, HealthyBaseline: *healthyBaseline, MetricsUrl: *metricsURL, MetricsPublishUrl: *metricsPublishURL,
	}
	if *duration > 0 {
		launch.Duration = durationpb.New(*duration)
	}
	if *upstreamURL != "" || *upstreamBranch != "" || *upstreamSSH != "" {
		launch.MemoryUpstream = &contracts.MemoryUpstream{Url: *upstreamURL, Branch: *upstreamBranch, SshDir: *upstreamSSH}
	}
	if err := wire.Validate(launch); err != nil {
		return builder.RunConfig{}, err
	}
	return builder.RunConfig{
		Build: builder.Config{Replay: *replay, ApplicationRoot: *app, OperatorRoot: *operatorRoot}, Controller: launch,
		PrepareShell: func(ctx context.Context) error {
			repository, err := openControllerBroker(launch)
			if err != nil {
				return err
			}
			return refreshControllerHealth(ctx, launch, repository)
		},
		ResolveDiagnostics: func(ctx context.Context, root string) (builder.DiagnosticsProjection, error) {
			projected, err := memory.AcceptedDiagnostics(ctx, root)
			if err != nil {
				return builder.DiagnosticsProjection{}, err
			}
			repository, err := openControllerBroker(launch)
			if err != nil {
				return builder.DiagnosticsProjection{}, err
			}
			if err := refreshControllerHealth(ctx, launch, repository); err != nil {
				return builder.DiagnosticsProjection{}, err
			}
			return builder.DiagnosticsProjection{Directory: projected.Directory, Fingerprint: projected.Fingerprint, AcceptedRevision: projected.AcceptedRevision}, nil
		},
	}, nil
}

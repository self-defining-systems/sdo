// Package operatorcli translates installation arguments into the controller's
// typed launch contract. The compiled child receives only ControllerConfig.
package operatorcli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"sdo.dev/operator/builder"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/deploymentpolicy"
	"sdo.dev/operator/operatorcli/deploywire"
	"sdo.dev/operator/planfile"
	"sdo.dev/operator/renderenv"
	"sdo.dev/operator/toolchain"
)

type repeatable []string

func (r *repeatable) String() string         { return strings.Join(*r, ",") }
func (r *repeatable) Set(value string) error { *r = append(*r, value); return nil }

// ParseControllerLaunch rejects native/legacy split-brain before any build or
// Kubernetes effect. It is pure so installer Job arguments can be contract
// tested against the same parser shipped in the controller image.
func ParseControllerLaunch(args []string) (builder.RunConfig, error) {
	run, _, _, err := parseControllerLaunch(args)
	return run, err
}

// RunController is the controller pod's parent process: the supervised
// controller child (builder.Run, which rebuilds in-process on adoption) and,
// beside it, the upstream follower, which advances the accepted source to new
// upstream revisions of an accepted .sdo/goal/sources.yaml. The follower
// never ends the controller; it stops when the child loop ends.
func RunController(ctx context.Context, args []string) error {
	run, identity, policy, err := parseControllerLaunch(args)
	if err != nil {
		return err
	}
	// The accepted tree is the only home of the deployment source; a lane
	// without .sdo/deployment/plan.yaml has nothing to read desired state from.
	if _, err := planfile.Read(run.Controller.GetAppRoot(), planCatalogs()); err != nil {
		return fmt.Errorf("controller cannot start: %w", err)
	}
	// The child and every wiring read below see the values the parent started with.
	if err := publishResolvedSettings(policy, run.Controller.GetRepositoryMountPath()); err != nil {
		return fmt.Errorf("controller cannot record its configuration: %w", err)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// A deployment that violates the policy in force is published as a typed
	// hold; detection keeps running either way.
	controllerPolicyHold(ctx, run.Controller, identity, slog.Default())
	err = builder.Run(ctx, run, builder.RestartPolicy{})
	cancel()
	return err
}

func parseControllerLaunch(args []string) (builder.RunConfig, deployIdentity, settings.Config, error) {
	fail := func(err error) (builder.RunConfig, deployIdentity, settings.Config, error) {
		return builder.RunConfig{}, deployIdentity{}, settings.Config{}, err
	}
	if len(args) == 0 || args[0] != "controller" {
		return fail(errors.New("expected controller subcommand"))
	}
	flags := flag.NewFlagSet("sdo-operator controller", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
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
	brokerMode := flags.String("broker-mode", "", "broker mode")
	workerMountMode := flags.String("worker-mount-mode", "", "worker mount mode")
	registrations := flags.String("agent-registration-directory", "", "agent registration directory")
	responderID := flags.String("responder-agent-id", "", "responder agent identity; defaults to the agent registered for incident requests")
	sourceCommit := flags.String("source-commit", "", "source commit")
	deployedCommit := flags.String("deployed-commit", "", "deployed commit")
	telemetryEnvironment := flags.String("telemetry-environment", "", "telemetry environment document: the sources the operator provides, their credential directory and the pinning mode")
	duration := flags.Duration("duration", 0, "bounded controller duration")
	exitAfterClosure := flags.Bool("exit-after-closure", false, "exit after the first acknowledged closure")
	upstreamURL := flags.String("memory-upstream-url", "", "shared remote Git URL the accepted repository replicates")
	upstreamBranch := flags.String("memory-upstream-branch", "", "remote branch for this run's accepted memory")
	upstreamSSH := flags.String("memory-upstream-ssh-dir", "", "read-only directory with the ssh identity and known_hosts for the memory remote")
	deploymentPolicyFlag := flags.String("deployment-policy", "", "install-time deployment policy (DeploymentPolicy as ProtoJSON); empty is no policy")
	renderEnvFlag := flags.String("render-env", "", "install-time render environment (RenderEnvironment as ProtoJSON); empty is unknown")
	// The deploy identity: the parent deploys upstream advances with it, the
	// child (through the launch config) a responder's source repair.
	var identity deployIdentity
	flags.StringVar(&identity.BuildKitImage, "buildkit-image", "", "digest-pinned image builder for upstream advances")
	flags.StringVar(&identity.RegistryPushSecret, "registry-push-secret", "", "image registry push Secret for upstream advances")
	flags.StringVar(&identity.RegistryHost, "registry-host", "", "trusted source image registry host[:port]")
	flags.BoolVar(&identity.RegistryInsecure, "registry-insecure", false, "allow the explicitly configured HTTP image registry")
	if err := flags.Parse(args[1:]); err != nil {
		return fail(err)
	}
	if flags.NArg() != 0 {
		return fail(fmt.Errorf("unexpected controller arguments: %v", flags.Args()))
	}
	// Every timing and policy value is the mounted sdo-config, read once here.
	policy, err := loadMountedSettings(mountedSettings)
	if err != nil {
		return fail(err)
	}
	identity.BuildTimeout, identity.BuildBudget = time.Duration(policy.Deployment.BuildTimeout), time.Duration(policy.Deployment.BuildBudget)
	identity.RolloutTimeout = time.Duration(policy.Deployment.RolloutTimeout)
	if err := identity.validate(); err != nil {
		return fail(err)
	}
	renderEnvironment, err := renderenv.Parse(*renderEnvFlag)
	if err != nil {
		return fail(err)
	}
	deploymentPolicy, err := deploymentpolicy.Parse(*deploymentPolicyFlag)
	if err != nil {
		return fail(err)
	}
	if err := toolchain.ValidateImage(*toolchainImage); err != nil {
		return fail(err)
	}
	if *brokerMode != "native" || *workerMountMode != "proposal-only" {
		return fail(errors.New("native broker requires proposal-only worker mounts"))
	}
	if *duration < 0 {
		return fail(errors.New("duration must be nonnegative"))
	}
	if *app == "" || !filepath.IsAbs(*app) || *operatorRoot == "" || *namespace == "" || *responderImage == "" || *repositoryPVC == "" || *repositoryMount == "" ||
		*credentialsSecret == "" || *worktreeRoot == "" {
		return fail(errors.New("native launch requires application, namespace, images, repository, credentials, and responder identity"))
	}
	if !filepath.IsAbs(*repositoryMount) || !filepath.IsAbs(*worktreeRoot) {
		return fail(errors.New("repository mount and proposal root must be absolute"))
	}
	if *application == "" {
		*application = filepath.Base(*app)
	}
	codePath, _ := policy.Agent.CodePathSelection() // validated with the configuration
	launch := &contracts.ControllerConfig{
		CodePath:  codePath,
		Namespace: *namespace, ControlNamespace: *controlNamespace, AppRoot: *app, Application: *application,
		SourceCommit: *sourceCommit, DeployedCommit: *deployedCommit,
		RenderEnvironment: renderEnvironment, DeploymentPolicy: deploymentPolicy,
		ResponderImage: *responderImage,
		RepositoryPvc:  *repositoryPVC, RepositoryMountPath: *repositoryMount, RepositoryPvcSubpath: *repositorySubPath,
		ResponderCredentialsSecret: *credentialsSecret, BrokerWorktreeRoot: *worktreeRoot,
		ExitAfterClosure: *exitAfterClosure,
		ScriptImage:      *scriptImage, ToolchainImage: *toolchainImage, AgentRegistrationDirectory: *registrations, ResponderAgentId: *responderID,
		BrokerMode: *brokerMode, WorkerMountMode: *workerMountMode,
		TelemetryEnvironmentFile: *telemetryEnvironment,
		SettingsPath:             resolvedSettingsPath(*repositoryMount),
	}
	// The child deploys a responder's source repair with the identity the
	// parent deploys upstream advances with.
	launch.DeployIdentity = deploywire.Identity(identity).Proto()
	if *duration > 0 {
		launch.Duration = durationpb.New(*duration)
	}
	if *upstreamURL != "" || *upstreamBranch != "" || *upstreamSSH != "" {
		launch.MemoryUpstream = &contracts.MemoryUpstream{Url: *upstreamURL, Branch: *upstreamBranch, SshDir: *upstreamSSH}
	}
	if err := wire.Validate(launch); err != nil {
		return fail(err)
	}
	return builder.RunConfig{
		Build: builder.Config{Replay: policy.Learning.Replay, ApplicationRoot: *app, OperatorRoot: *operatorRoot}, Controller: launch,
		PrepareShell: func(ctx context.Context) error {
			repository, err := openControllerBroker(launch)
			if err != nil {
				return err
			}
			return holdRefreshOnPolicy(refreshControllerHealth(ctx, "", launch, *operatorRoot, repository), slog.Default())
		},
		ResolveDiagnostics: func(ctx context.Context, root string) (builder.DiagnosticsProjection, error) {
			repository, err := openMigrationBroker(launch)
			if err != nil {
				return builder.DiagnosticsProjection{}, err
			}
			projected, err := resolveAcceptedDiagnostics(ctx, root, repository)
			if err != nil {
				return builder.DiagnosticsProjection{}, err
			}
			repository, err = openControllerBroker(launch)
			if err != nil {
				return builder.DiagnosticsProjection{}, err
			}
			if err := holdRefreshOnPolicy(refreshControllerHealth(ctx, "", launch, *operatorRoot, repository), slog.Default()); err != nil {
				return builder.DiagnosticsProjection{}, err
			}
			return builder.DiagnosticsProjection{Directory: projected.Directory, Fingerprint: projected.Fingerprint, AcceptedRevision: projected.AcceptedRevision}, nil
		},
	}, identity, policy, nil
}

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

const sdkSchemaIdentity = "sdo.dev/v1alpha1+sdk.5d8413fd20fbdc03"

type repeatable []string

func (r *repeatable) String() string         { return strings.Join(*r, ",") }
func (r *repeatable) Set(value string) error { *r = append(*r, value); return nil }

// ParseControllerLaunch rejects native/legacy split-brain before any build or
// Kubernetes effect. It is pure so installer Job arguments can be contract
// tested against the same parser shipped in the controller image.
func ParseControllerLaunch(args []string) (builder.RunConfig, bool, error) {
	if len(args) == 0 || args[0] != "controller" {
		return builder.RunConfig{}, false, errors.New("expected controller subcommand")
	}
	flags := flag.NewFlagSet("sdo-operator controller", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	app := flags.String("app", "", "accepted application checkout")
	operatorRoot := flags.String("operator-root", "/opt/sdo/sdo/operator", "installed Go operator modules")
	namespace := flags.String("namespace", "", "application namespace")
	controlNamespace := flags.String("control-namespace", "", "controller namespace")
	application := flags.String("application", "", "application identity")
	responderImage := flags.String("responder-image", "", "registered responder image")
	toolchainImage := flags.String("toolchain-image", "", "trusted controller image supplying Go to workers and validators (required)")
	proberImage := flags.String("prober-image", "", "traffic prober image")
	repositoryPVC := flags.String("repository-pvc", "", "repository PVC")
	repositoryMount := flags.String("repository-mount-path", "", "repository PVC mount")
	repositorySubPath := flags.String("repository-pvc-subpath", "", "repository PVC subpath")
	credentialsSecret := flags.String("credentials-secret", "", "responder credentials secret")
	worktreeRoot := flags.String("worktree-root", "", "broker proposal and ledger root")
	validatorImage := flags.String("validator-image", "", "isolated validator image")
	model := flags.String("model", "", "responder model")
	backend := flags.String("responder-backend", "codex", "responder backend")
	schemaIdentity := flags.String("schema-identity", sdkSchemaIdentity, "accepted SDK schema identity")
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
	metricsURL := flags.String("metrics-url", "", "Prometheus-compatible API URL that metric and alert detectors query once per evaluation cycle")
	maxResponseLoops := flags.Int("max-response-loops", 10, "maximum sdo apply batches per incident")
	programFastPath := flags.Bool("program-fast-path", true, "allow trusted programs to resolve incidents")
	programTrustThreshold := flags.Int("program-trust-threshold", 1, "verified successes required to trust a program")
	diagnosisDeadline := flags.Duration("diagnosis-deadline", 10*time.Second, "shared candidate diagnosis deadline")
	programMinDuration := flags.Duration("program-min-duration", 10*time.Second, "minimum sustained learned-program firing and clearing duration")
	diagnosisSettleDelay := flags.Duration("diagnosis-settle-delay", 15*time.Second, "settle delay before a trusted-program fast-path attempt")
	duration := flags.Duration("duration", 0, "bounded controller duration")
	exitAfterClosure := flags.Bool("exit-after-closure", false, "exit after the first acknowledged closure")
	supervise := flags.Bool("supervise", false, "rebuild and relaunch after closure")
	var responderEnv repeatable
	flags.Var(&responderEnv, "responder-env", "responder environment NAME=VALUE")
	var deployedManifestPaths repeatable
	flags.Var(&deployedManifestPaths, "deployed-manifest-path", "manifest path selected by the accepted deployment plan")
	if err := flags.Parse(args[1:]); err != nil {
		return builder.RunConfig{}, false, err
	}

	if flags.NArg() != 0 {
		return builder.RunConfig{}, false, fmt.Errorf("unexpected controller arguments: %v", flags.Args())
	}
	if err := toolchain.ValidateImage(*toolchainImage); err != nil {
		return builder.RunConfig{}, false, err
	}
	if *brokerMode != "native" || *workerMountMode != "proposal-only" {
		return builder.RunConfig{}, false, errors.New("native broker requires proposal-only worker mounts")
	}
	if *duration < 0 || (*supervise && (*duration > 0 || *exitAfterClosure)) {
		return builder.RunConfig{}, false, errors.New("supervision cannot be combined with bounded duration or exit-after-closure")
	}
	if *programTrustThreshold < 1 {
		return builder.RunConfig{}, false, errors.New("program trust threshold must be positive")
	}
	if *programMinDuration < 0 {
		return builder.RunConfig{}, false, errors.New("program minimum duration must be nonnegative")
	}

	if *app == "" || !filepath.IsAbs(*app) || *operatorRoot == "" || *namespace == "" || *responderImage == "" || *repositoryPVC == "" || *repositoryMount == "" ||
		*credentialsSecret == "" || *worktreeRoot == "" || *validatorImage == "" || *model == "" || *backend == "" || *schemaIdentity == "" || *responseTimeout <= 0 || *verificationTimeout <= 0 {
		return builder.RunConfig{}, false, errors.New("native launch requires application, namespace, images, repository, credentials, validator, schema, and responder identity")
	}
	if !filepath.IsAbs(*repositoryMount) || !filepath.IsAbs(*worktreeRoot) {
		return builder.RunConfig{}, false, errors.New("repository mount and proposal root must be absolute")
	}
	if *application == "" {
		*application = filepath.Base(*app)
	}
	launch := &contracts.ControllerConfig{
		Namespace: *namespace, ControlNamespace: *controlNamespace, AppRoot: *app, Application: *application,
		SourceCommit: *sourceCommit, DeployedCommit: *deployedCommit,
		DeployedManifestPaths: deployedManifestPaths,
		Dispatcher:            "python3", DispatcherMode: "job", DispatcherArgs: []string{"-m", "sdo.agents.responder.worker"},
		ResponderImage: *responderImage, ResponderEnv: responderEnv,
		RepositoryPvc: *repositoryPVC, RepositoryMountPath: *repositoryMount, RepositoryPvcSubpath: *repositorySubPath,
		ResponderCredentialsSecret: *credentialsSecret, BrokerWorktreeRoot: *worktreeRoot,
		ResponseTimeout: durationpb.New(*responseTimeout), VerificationTimeout: durationpb.New(*verificationTimeout),
		RepairPolicy: *repairPolicy, MaxResponseLoops: int32(*maxResponseLoops), DiagnosisDeadline: durationpb.New(*diagnosisDeadline), DiagnosisSettleDelay: durationpb.New(*diagnosisSettleDelay), LeaseName: "sdo-controller", RestartAfterClosure: *supervise,
		ExitAfterClosure: *exitAfterClosure,
		ToolchainImage:   *toolchainImage, ProberImage: *proberImage, AgentRegistrationDirectory: *registrations, ResponderAgentId: *responderID,
		BrokerMode: *brokerMode, WorkerMountMode: *workerMountMode, ValidatorImage: *validatorImage,
		SchemaIdentity: *schemaIdentity, ResponderBackend: *backend, ResponderModel: *model, ReflectionSession: *reflectionSession, ReflectionGuidance: *reflectionGuidance, ForceReflection: *forceReflection, HealthyBaseline: *healthyBaseline, MetricsUrl: *metricsURL,
		ProgramFastPath: *programFastPath, ProgramTrustThreshold: int32(*programTrustThreshold),
		ProgramMinDuration: durationpb.New(*programMinDuration),
	}
	if *duration > 0 {
		launch.Duration = durationpb.New(*duration)
	}
	if err := wire.Validate(launch); err != nil {
		return builder.RunConfig{}, false, err
	}
	return builder.RunConfig{
		Build: builder.Config{ApplicationRoot: *app, OperatorRoot: *operatorRoot}, Controller: launch,
		ResolveDiagnostics: func(ctx context.Context, root string) (builder.DiagnosticsProjection, error) {
			repository, err := openMigrationBroker(launch)
			if err != nil {
				return builder.DiagnosticsProjection{}, err
			}
			if _, err := resolveAcceptedDiagnostics(ctx, root, repository); err != nil {
				return builder.DiagnosticsProjection{}, err
			}
			if err := refreshControllerHealth(ctx, launch, repository); err != nil {
				return builder.DiagnosticsProjection{}, err
			}
			projected, err := memory.AcceptedDiagnostics(ctx, root)
			if err != nil {
				return builder.DiagnosticsProjection{}, err
			}
			return builder.DiagnosticsProjection{Directory: projected.Directory, Fingerprint: projected.Fingerprint, AcceptedRevision: projected.AcceptedRevision}, nil
		},
		PublishProber: memory.PublishProber,
	}, *supervise, nil
}

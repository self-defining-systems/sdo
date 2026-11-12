package operatorcli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	"sdo.dev/operator/builder"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/deploymentpolicy"
	"sdo.dev/operator/install"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/operatorcli/internal/processio"
	"sdo.dev/operator/planfile"
	"sdo.dev/operator/renderenv"
)

// InstallExistingConfig installs an already accepted source tree without
// rerunning deployment agents or replacing the accepted Git history.
type InstallExistingConfig struct {
	Repository          string
	AdoptExistingMemory bool
	OperatorRoot        string
	Kubeconfig          string
	// DeploymentSource, when set, is recorded as the repository's plan.yaml
	// before install; RunInstallExisting then replaces it with the source the
	// accepted tree declares, which scopes adoption evidence.
	DeploymentSource *contracts.DeploymentSource
	// RenderEnvironment is discovered from the cluster at install and passed
	// to the controller (--render-env); RunInstallExisting sets it.
	RenderEnvironment *contracts.RenderEnvironment
	Duration          time.Duration
	Install           install.Config
}

type InstallExistingResult struct {
	Installed      bool   `json:"installed"`
	Reused         bool   `json:"reused"`
	Fingerprint    string `json:"fingerprint"`
	AcceptedCommit string `json:"accepted_commit"`
	ControllerLogs string `json:"controller_logs,omitempty"`
}

func ParseInstallExistingLaunch(args []string) (InstallExistingConfig, error) {
	return ParseInstallExistingLaunchEnv(args, processio.Environ())
}

// ParseInstallExistingLaunchEnv parses an install-existing launch against an explicit environment (KEY=VALUE lines).
func ParseInstallExistingLaunchEnv(args []string, environ []string) (InstallExistingConfig, error) {
	if len(args) == 0 || args[0] != "install-existing" {
		return InstallExistingConfig{}, errors.New("expected install-existing subcommand")
	}
	flags := flag.NewFlagSet("sdo-operator install-existing", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.String("code-path", "", "programming-model plugin set: go or shell")
	flags.String("code-path-overrides", "", "CodePathOverrides JSON")
	adopt := flags.Bool("adopt-existing-memory", false, "explicitly adopt supplied memory unvalidated as the accepted base")
	operatorRoot := flags.String("operator-root", "/opt/sdo/sdo/operator", "operator sources used to compile adopted memory")
	repository := flags.String("repository", "", "clean accepted Git repository")
	namespace := flags.String("namespace", "", "application namespace")
	controlNamespace := flags.String("control-namespace", "", "operator namespace")
	application := flags.String("application", "", "application identity")
	controllerImage := flags.String("controller-image", "", "Go controller image")
	responderImage := flags.String("responder-image", "", "agent worker image")
	scriptImage := flags.String("script-image", "", "fixed slim script-tools image; defaults to the controller image")
	repositoryPVC := flags.String("repository-pvc", "", "shared accepted repository PVC")
	credentialsSecret := flags.String("credentials-secret", "", "agent credentials Secret")
	flags.String("model", "", "agent model")
	flags.String("agent-provider", "codex", "agent provider")
	flags.String("replay", "", "replay and corpus capture: on (default) or off")
	flags.String("agent-effort", "", "reasoning effort of every agent worker (empty: the shared default)")
	flags.Duration("lifecycle-timeout", 0, "limit of one lifecycle (detection designer) session (0: the shared default)")
	flags.String("repair-policy", "recorded-actions", "repair evidence policy")
	flags.String("reflection-guidance", "baseline", "reflection guidance mode")
	flags.Bool("frozen-memory", false, "keep closure evidence private and disable learning")
	flags.Bool("external-incidents", false, "accept external alerts only; close on responder completion (requires frozen memory)")
	flags.Bool("watch-only", false, "detect, open, verify and close incidents but never respond: no automatic stage, responder or reflection (requires frozen memory)")
	flags.Bool("force-reflection", false, "always dispatch reflection after verified closure")
	flags.Int("max-response-loops", 10, "maximum mitigation rounds the responder agent may use per incident (the automatic stage's one apply does not count)")
	flags.Duration("response-timeout", 30*time.Minute, "responder timeout")
	flags.Duration("verification-timeout", 5*time.Minute, "verification timeout")
	duration := flags.Duration("duration", 0, "bounded controller duration")
	selfHealHold := flags.Duration("self-heal-hold", 0, "controller self_heal_hold: how long a Kubernetes failure condition without a give-up signal must hold before it opens an incident (0: the controller default)")
	exitAfterClosure := flags.Bool("exit-after-closure", false, "stop after the first closure")
	flags.String("late-findings", "off", "late finding pull mode: off or pull")
	flags.Bool("permissive-responder", false, "broad lane repair permissions excluding Secret and credential APIs")
	flags.Var(new(repeatable), "responder-namespace", "additional lane namespace for permissive responder")
	flags.Bool("healthy-baseline", false, "validate incident detectors against recorded healthy snapshots")
	registerTelemetryFlags(flags)
	registrationConfigMap := flags.String("agent-registration-configmap", "", "registration ConfigMap")
	kubeconfig := flags.String("kubeconfig", "", "Kubernetes client and kubectl config")
	flags.Bool("responder-standby", true, "pre-start a responder; false forces cold Jobs")
	flags.Var(new(repeatable), "responder-env", "responder environment NAME=VALUE")
	registerEvidenceSinkFlags(flags)
	var configFiles repeatable
	flags.Var(&configFiles, "config", "operator configuration YAML (repeatable; later files override earlier ones; see docs/config.md)")
	var policyFlags deploymentpolicy.Flags
	flags.StringVar(&policyFlags.AllowedRegistries, "allow-registry", "", "comma-separated registry hosts deployments may pull from (enforced; empty: no allowlist)")
	flags.BoolVar(&policyFlags.ScopeBlocking, "scope-blocking", false, "refuse foreign-namespace and cluster-scoped objects unless granted")
	flags.StringVar(&policyFlags.GrantedNamespaces, "grant-namespace", "", "comma-separated foreign namespaces deployments may write to (needs --scope-blocking)")
	flags.StringVar(&policyFlags.GrantedClusterKinds, "grant-cluster-kind", "", "comma-separated cluster-scoped Kinds deployments may contain (needs --scope-blocking)")
	deploymentSourceFlag := flags.String("deployment-source", "", "DeploymentSource as ProtoJSON, recorded as the repository's .sdo/deployment/plan.yaml in a human commit; optional when the repository already carries a plan")
	if err := flags.Parse(args[1:]); err != nil {
		return InstallExistingConfig{}, err
	}

	if flags.NArg() != 0 {
		return InstallExistingConfig{}, fmt.Errorf("unexpected install-existing arguments: %v", flags.Args())
	}
	resolved, err := resolveSettings(configFiles, environ, flags)
	if err != nil {
		return InstallExistingConfig{}, err
	}
	for name, value := range map[string]string{
		"repository": *repository, "namespace": *namespace, "application": *application,
		"controller image": *controllerImage, "responder image": *responderImage,
		"repository PVC": *repositoryPVC, "credentials Secret": *credentialsSecret,
	} {
		if strings.TrimSpace(value) == "" {
			return InstallExistingConfig{}, fmt.Errorf("install-existing requires %s", name)
		}
	}
	if !filepath.IsAbs(*repository) || (*kubeconfig != "" && !filepath.IsAbs(*kubeconfig)) {
		return InstallExistingConfig{}, errors.New("repository and kubeconfig paths must be absolute")
	}
	if *duration < 0 {
		return InstallExistingConfig{}, errors.New("controller duration must be positive")
	}
	controlForTelemetry := *controlNamespace
	if controlForTelemetry == "" {
		controlForTelemetry = *namespace
	}
	config := install.Config{
		Namespace: *namespace, ControlNamespace: *controlNamespace, Application: *application,
		ScriptImage: *scriptImage, ControllerImage: *controllerImage, ResponderImage: *responderImage,
		RepositoryPVC: *repositoryPVC, CredentialsSecret: *credentialsSecret,
		NativeCommand: []string{"/usr/local/bin/sdo-operator"},
		ReuseExisting: *duration == 0 && !*exitAfterClosure, WaitForCompletion: *duration > 0 || *exitAfterClosure,
		RegistrationConfigMap: *registrationConfigMap,
	}
	if err := applySettings(&config, resolved, controlForTelemetry); err != nil {
		return InstallExistingConfig{}, err
	}
	if *registrationConfigMap != "" {
		config.RegistrationDirectory = "/sdo/registrations"
	}
	deploymentSource, err := parseDeploymentSourceFlag(*deploymentSourceFlag)
	if err != nil {
		return InstallExistingConfig{}, err
	}
	if *duration > 0 {
		config.ExtraControllerArgs = append(config.ExtraControllerArgs, "--duration", duration.String())
	}
	if *selfHealHold > 0 {
		config.ExtraControllerArgs = append(config.ExtraControllerArgs, "--self-heal-hold", selfHealHold.String())
	}
	if *exitAfterClosure {
		config.ExtraControllerArgs = append(config.ExtraControllerArgs, "--exit-after-closure")
	}
	return InstallExistingConfig{AdoptExistingMemory: *adopt, OperatorRoot: *operatorRoot, Repository: *repository, Kubeconfig: *kubeconfig, DeploymentSource: deploymentSource, Duration: *duration, Install: config}, nil
}

func RunInstallExisting(ctx context.Context, launch InstallExistingConfig) (InstallExistingResult, error) {
	source, err := recordInstallDeploymentSource(ctx, launch)
	if err != nil {
		return InstallExistingResult{}, err
	}
	launch.DeploymentSource = source
	commit, err := prepareInstallExistingRepository(ctx, launch)
	if err != nil {
		return InstallExistingResult{}, err
	}
	client, err := hostKubernetesClient(launch.Kubeconfig)
	if err != nil {
		return InstallExistingResult{}, err
	}
	environment, err := renderenv.Discover(client.Discovery())
	if err != nil {
		return InstallExistingResult{}, err
	}
	encodedEnvironment, err := renderenv.Format(environment)
	if err != nil {
		return InstallExistingResult{}, err
	}
	launch.RenderEnvironment = environment
	config := launch.Install
	config.ExtraControllerArgs = append(slices.Clone(config.ExtraControllerArgs), "--render-env="+encodedEnvironment)
	config.AcceptedCommit = commit
	installer := install.Installer{
		Client:         client,
		SeedRepository: install.KubectlTarSeederWithKubeconfig(launch.Repository, commit, launch.Kubeconfig),
		SyncRepository: install.KubectlTarSynchronizerWithKubeconfig(launch.Repository, launch.Kubeconfig),
	}
	installer.BeforeControllerStart = existingNamespaceOnboardingHook(client, launch, commit)
	result, err := installer.Install(ctx, config)
	if err != nil {
		return InstallExistingResult{}, err
	}
	return InstallExistingResult{Installed: true, Reused: result.Reused, Fingerprint: result.Fingerprint, AcceptedCommit: commit, ControllerLogs: result.ControllerLogs}, nil
}

// recordInstallDeploymentSource settles the deployment source before any
// cluster call: the flag's source becomes plan.yaml in a human commit, and a
// repository left without a plan is refused (the controller would not start).
func recordInstallDeploymentSource(ctx context.Context, launch InstallExistingConfig) (*contracts.DeploymentSource, error) {
	if launch.DeploymentSource != nil {
		plan, err := memory.RecordDeploymentSource(ctx, launch.Repository, launch.DeploymentSource)
		if err != nil {
			return nil, err
		}
		return plan.GetSource(), nil
	}
	plan, err := planfile.Read(launch.Repository, planCatalogs())
	if err != nil {
		return nil, fmt.Errorf("install-existing: %w", err)
	}
	return plan.GetSource(), nil
}

// parseDeploymentSourceFlag decodes the strict ProtoJSON DeploymentSource of
// --deployment-source; empty is no source.
func parseDeploymentSourceFlag(encoded string) (*contracts.DeploymentSource, error) {
	if encoded == "" {
		return nil, nil
	}
	source := &contracts.DeploymentSource{}
	if err := protojson.Unmarshal([]byte(encoded), source); err != nil {
		return nil, fmt.Errorf("invalid deployment source: %w", err)
	}
	return source, nil
}

// prepareInstallExistingRepository keeps the opt-in adoption edge separate from
// normal accepted installs and every subsequent broker mutation.
func prepareInstallExistingRepository(ctx context.Context, launch InstallExistingConfig) (string, error) {
	if !launch.AdoptExistingMemory {
		return acceptedHead(ctx, launch.Repository)
	}
	if memoryCodePath(launch.Install.CodePath) == "shell" {
		if err := validate.ShellHygieneValidator(ctx, launch.Repository, nil); err != nil {
			return "", err
		}
		return memory.MarkAdoptedExisting(ctx, launch.Repository)
	}
	diagnostics, err := memory.CandidateDiagnostics(launch.Repository)
	if err != nil {
		return "", err
	}
	w, err := builder.BuildWorkspace(ctx, builder.Config{Replay: launch.Install.Replay, ApplicationRoot: launch.Repository, DiagnosticsRoot: diagnostics, OperatorRoot: launch.OperatorRoot, CompileOnly: true})
	if err != nil {
		return "", err
	}
	defer w.Close()
	if _, err = w.ValidateAndBuild(ctx); err != nil {
		return "", err
	}
	return memory.MarkAdoptedExisting(ctx, launch.Repository)
}

func memoryAdoptExisting(ctx context.Context, args []string, getenv func(string) string, out io.Writer) error {
	flags := flag.NewFlagSet("memory adopt-existing", flag.ContinueOnError)
	codePathName := flags.String("code-path", getenv("SDO_CODE_PATH"), "programming-model plugin set: go or shell")
	root := flags.String("root", ".", "seed repository with existing memory")
	operator := flags.String("operator-root", operatorRootDefault(getenv), "operator sources")
	positional, err := parseFlags(flags, args)
	if err != nil {
		return err
	}
	if len(positional) != 0 {
		return errors.New("adopt-existing takes no positional arguments")
	}
	absolute, err := absRoot(*root)
	if err != nil {
		return err
	}
	selectedCodePath, err := parseCodePathSelection(*codePathName, "")
	if err != nil {
		return err
	}
	commit, err := prepareInstallExistingRepository(ctx, InstallExistingConfig{Repository: absolute, OperatorRoot: *operator, AdoptExistingMemory: true, Install: install.Config{CodePath: selectedCodePath}})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "{\"adopted\":true,\"accepted_commit\":%q,\"origin\":\"adopted-existing\",\"validated\":false}\n", commit)
	return err
}

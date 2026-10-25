package operatorcli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"sdo.dev/operator/builder"
	"sdo.dev/operator/install"
	"sdo.dev/operator/memory"
)

// InstallExistingConfig installs an already accepted source tree without
// rerunning deployment agents or replacing the accepted Git history.
type InstallExistingConfig struct {
	Repository          string
	AdoptExistingMemory bool
	OperatorRoot        string
	Kubeconfig          string
	SeedBrokerRoot      string
	Duration            time.Duration
	Install             install.Config
}

type InstallExistingResult struct {
	Installed      bool   `json:"installed"`
	Reused         bool   `json:"reused"`
	Fingerprint    string `json:"fingerprint"`
	AcceptedCommit string `json:"accepted_commit"`
	ControllerLogs string `json:"controller_logs,omitempty"`
}

func ParseInstallExistingLaunch(args []string) (InstallExistingConfig, error) {
	if len(args) == 0 || args[0] != "install-existing" {
		return InstallExistingConfig{}, errors.New("expected install-existing subcommand")
	}
	flags := flag.NewFlagSet("sdo-operator install-existing", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	adopt := flags.Bool("adopt-existing-memory", false, "explicitly adopt supplied memory unvalidated as the accepted base")
	operatorRoot := flags.String("operator-root", "/opt/sdo/sdo/operator", "operator sources used to compile adopted memory")
	repository := flags.String("repository", "", "clean accepted Git repository")
	namespace := flags.String("namespace", "", "application namespace")
	controlNamespace := flags.String("control-namespace", "", "operator namespace")
	application := flags.String("application", "", "application identity")
	controllerImage := flags.String("controller-image", "", "Go controller image")
	responderImage := flags.String("responder-image", "", "agent worker image")
	validatorImage := flags.String("validator-image", "", "isolated validator image")
	repositoryPVC := flags.String("repository-pvc", "", "shared accepted repository PVC")
	credentialsSecret := flags.String("credentials-secret", "", "agent credentials Secret")
	model := flags.String("model", "", "agent model")
	provider := flags.String("agent-provider", "codex", "agent provider")
	repairPolicy := flags.String("repair-policy", "recorded-actions", "repair evidence policy")
	reflectionSession := flags.String("reflection-session", "resume", "reflection session mode: fresh or resume")
	reflectionGuidance := flags.String("reflection-guidance", "baseline", "reflection guidance mode")
	forceReflection := flags.Bool("force-reflection", false, "always dispatch reflection after verified closure")
	maxResponseLoops := flags.Int("max-response-loops", 10, "maximum sdo apply batches per incident")
	responseTimeout := flags.Duration("response-timeout", 30*time.Minute, "responder timeout")
	verificationTimeout := flags.Duration("verification-timeout", 5*time.Minute, "verification timeout")
	duration := flags.Duration("duration", 0, "bounded controller duration")
	exitAfterClosure := flags.Bool("exit-after-closure", false, "stop after the first closure")
	lateFindings := flags.String("late-findings", "off", "late finding pull mode: off or pull")
	permissiveResponder := flags.Bool("permissive-responder", false, "broad lane repair permissions excluding Secret and credential APIs")
	var responderNamespaces repeatable
	flags.Var(&responderNamespaces, "responder-namespace", "additional lane namespace for permissive responder")
	healthyBaseline := flags.Bool("healthy-baseline", false, "validate incident detectors against recorded healthy snapshots")
	metricsURL := flags.String("metrics-url", "", "Prometheus-compatible API URL; installs the metrics alerts detector and lets metric detectors query it")
	registrationConfigMap := flags.String("agent-registration-configmap", "", "registration ConfigMap")
	kubeconfig := flags.String("kubeconfig", "", "Kubernetes client and kubectl config")
	seedBrokerRoot := flags.String("seed-broker-root", "", "operator-controlled trusted seed broker directory, outside the accepted repository")
	var responderEnv repeatable
	flags.Var(&responderEnv, "responder-env", "responder environment NAME=VALUE")
	var deployedManifestPaths repeatable
	flags.Var(&deployedManifestPaths, "deployed-manifest-path", "manifest path selected by the accepted deployment")
	if err := flags.Parse(args[1:]); err != nil {
		return InstallExistingConfig{}, err
	}

	if flags.NArg() != 0 {
		return InstallExistingConfig{}, fmt.Errorf("unexpected install-existing arguments: %v", flags.Args())
	}
	for name, value := range map[string]string{
		"repository": *repository, "namespace": *namespace, "application": *application,
		"controller image": *controllerImage, "responder image": *responderImage, "validator image": *validatorImage,
		"repository PVC": *repositoryPVC, "credentials Secret": *credentialsSecret, "model": *model,
	} {
		if strings.TrimSpace(value) == "" {
			return InstallExistingConfig{}, fmt.Errorf("install-existing requires %s", name)
		}
	}
	if !filepath.IsAbs(*repository) || (*kubeconfig != "" && !filepath.IsAbs(*kubeconfig)) {
		return InstallExistingConfig{}, errors.New("repository and kubeconfig paths must be absolute")
	}
	if *seedBrokerRoot != "" && !filepath.IsAbs(*seedBrokerRoot) {
		return InstallExistingConfig{}, errors.New("trusted seed broker root must be absolute")
	}
	if *provider != "codex" && *provider != "claude" {
		return InstallExistingConfig{}, fmt.Errorf("unsupported agent provider %q", *provider)
	}
	if *repairPolicy != "commit" && *repairPolicy != "recorded-actions" {
		return InstallExistingConfig{}, fmt.Errorf("unsupported repair policy %q", *repairPolicy)
	}
	if *reflectionSession != "fresh" && *reflectionSession != "resume" {
		return InstallExistingConfig{}, fmt.Errorf("unsupported reflection session mode %q", *reflectionSession)
	}
	if *reflectionGuidance != "baseline" && *reflectionGuidance != "generalize" && *reflectionGuidance != "generalize-spec" {
		return InstallExistingConfig{}, fmt.Errorf("unsupported reflection guidance %q", *reflectionGuidance)
	}
	if *lateFindings != "off" && *lateFindings != "pull" {
		return InstallExistingConfig{}, fmt.Errorf("invalid late findings mode %q", *lateFindings)
	}
	if *duration < 0 || *responseTimeout <= 0 || *verificationTimeout <= 0 {
		return InstallExistingConfig{}, errors.New("controller timeouts must be positive")
	}
	menu := os.Getenv("SDO_RESPONDER_MENU")
	if menu != "" && menu != "0" && menu != "1" {
		return InstallExistingConfig{}, errors.New("SDO_RESPONDER_MENU must be 0 or 1")
	}
	config := install.Config{
		Namespace: *namespace, ControlNamespace: *controlNamespace, Application: *application,
		ControllerImage: *controllerImage, ResponderImage: *responderImage, ValidatorImage: *validatorImage,
		RepositoryPVC: *repositoryPVC, CredentialsSecret: *credentialsSecret, Model: *model,
		RepairPolicy: *repairPolicy, ReflectionSession: *reflectionSession, ReflectionGuidance: *reflectionGuidance, ForceReflection: *forceReflection, AgentProvider: *provider,
		MaxResponseLoops: *maxResponseLoops,
		ResponderMenu:    menu != "0",
		LateFindings:     *lateFindings,
		HealthyBaseline:  *healthyBaseline, PermissiveResponder: *permissiveResponder, ResponderNamespaces: responderNamespaces,
		TimeoutSeconds: int(responseTimeout.Seconds()), VerificationTimeoutSeconds: int(verificationTimeout.Seconds()),
		MetricsURL:    *metricsURL,
		NativeCommand: []string{"/usr/local/bin/sdo-operator"},
		ReuseExisting: *duration == 0 && !*exitAfterClosure, WaitForCompletion: *duration > 0 || *exitAfterClosure,
		RegistrationConfigMap: *registrationConfigMap,
	}
	if *registrationConfigMap != "" {
		config.RegistrationDirectory = "/sdo/registrations"
	}
	for _, value := range responderEnv {
		name, _, found := strings.Cut(value, "=")
		if !found || name == "" || strings.ContainsAny(name, "=\r\n") {
			return InstallExistingConfig{}, fmt.Errorf("invalid responder environment %q", value)
		}
		config.ExtraControllerArgs = append(config.ExtraControllerArgs, "--responder-env="+value)
	}
	for _, path := range deployedManifestPaths {
		config.ExtraControllerArgs = append(config.ExtraControllerArgs, "--deployed-manifest-path="+path)
	}
	if *duration > 0 {
		config.ExtraControllerArgs = append(config.ExtraControllerArgs, "--duration", duration.String())
	}
	if *exitAfterClosure {
		config.ExtraControllerArgs = append(config.ExtraControllerArgs, "--exit-after-closure")
	}
	return InstallExistingConfig{AdoptExistingMemory: *adopt, OperatorRoot: *operatorRoot, Repository: *repository, Kubeconfig: *kubeconfig, SeedBrokerRoot: *seedBrokerRoot, Duration: *duration, Install: config}, nil
}

func RunInstallExisting(ctx context.Context, launch InstallExistingConfig) (InstallExistingResult, error) {
	commit, err := prepareInstallExistingRepository(ctx, launch)
	if err != nil {
		return InstallExistingResult{}, err
	}
	client, err := hostKubernetesClient(launch.Kubeconfig)
	if err != nil {
		return InstallExistingResult{}, err
	}
	config := launch.Install
	if launch.AdoptExistingMemory {
	}
	config.AcceptedCommit = commit
	installer := install.Installer{
		Client:         client,
		SeedRepository: install.KubectlTarSeederWithBrokerEvidence(launch.Repository, commit, launch.Kubeconfig, launch.SeedBrokerRoot),
		SyncRepository: install.KubectlTarSynchronizerWithKubeconfig(launch.Repository, launch.Kubeconfig),
	}
	result, err := installer.Install(ctx, config)
	if err != nil {
		return InstallExistingResult{}, err
	}
	return InstallExistingResult{Installed: true, Reused: result.Reused, Fingerprint: result.Fingerprint, AcceptedCommit: commit, ControllerLogs: result.ControllerLogs}, nil
}

// prepareInstallExistingRepository keeps the opt-in adoption edge separate from
// normal accepted installs and every subsequent broker mutation.
func prepareInstallExistingRepository(ctx context.Context, launch InstallExistingConfig) (string, error) {
	if !launch.AdoptExistingMemory {
		return acceptedHead(ctx, launch.Repository)
	}
	diagnostics, err := memory.CandidateDiagnostics(launch.Repository)
	if err != nil {
		return "", err
	}
	w, err := builder.BuildWorkspace(ctx, builder.Config{ApplicationRoot: launch.Repository, DiagnosticsRoot: diagnostics, OperatorRoot: launch.OperatorRoot, CompileOnly: true})
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
	commit, err := prepareInstallExistingRepository(ctx, InstallExistingConfig{Repository: absolute, OperatorRoot: *operator, AdoptExistingMemory: true})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "{\"adopted\":true,\"accepted_commit\":%q,\"origin\":\"adopted-existing\",\"validated\":false}\n", commit)
	return err
}

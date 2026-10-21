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

	"sdo.dev/operator/install"
)

// InstallExistingConfig installs an already accepted source tree without
// rerunning deployment agents or replacing the accepted Git history.
type InstallExistingConfig struct {
	Repository string
	Kubeconfig string
	Duration   time.Duration
	Install    install.Config
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
	repairPolicy := flags.String("repair-policy", "commit", "repair evidence policy")
	reflectionSession := flags.String("reflection-session", "resume", "reflection session mode: fresh or resume")
	reflectionGuidance := flags.String("reflection-guidance", "baseline", "reflection guidance mode")
	forceReflection := flags.Bool("force-reflection", false, "always dispatch reflection after verified closure")
	maxFollowUps := flags.Int("max-follow-ups", 0, "maximum follow-up responders")
	maxResponseLoops := flags.Int("max-response-loops", 5, "maximum sdo apply batches per incident")
	programFastPath := flags.Bool("program-fast-path", false, "allow trusted programs to resolve incidents")
	programTrustThreshold := flags.Int("program-trust-threshold", 3, "verified successes required to trust a program")
	followUpCooldown := flags.Duration("follow-up-cooldown", 30*time.Second, "follow-up responder cooldown")
	responseTimeout := flags.Duration("response-timeout", 30*time.Minute, "responder timeout")
	verificationTimeout := flags.Duration("verification-timeout", 2*time.Minute, "verification timeout")
	duration := flags.Duration("duration", 0, "bounded controller duration")
	exitAfterClosure := flags.Bool("exit-after-closure", false, "stop after the first closure")
	lateFindings := flags.String("late-findings", "off", "late finding pull mode: off or pull")
	healthyBaseline := flags.Bool("healthy-baseline", false, "validate incident detectors against recorded healthy snapshots")
	registrationConfigMap := flags.String("agent-registration-configmap", "", "registration ConfigMap")
	kubeconfig := flags.String("kubeconfig", "", "Kubernetes client and kubectl config")
	var responderEnv repeatable
	flags.Var(&responderEnv, "responder-env", "responder environment NAME=VALUE")
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
	if *maxFollowUps < 0 || *maxFollowUps > 1000 || *followUpCooldown < time.Second || *followUpCooldown%time.Second != 0 {
		return InstallExistingConfig{}, errors.New("follow-up limit must be at most 1000 and cooldown positive whole seconds")
	}
	if *lateFindings != "off" && *lateFindings != "pull" {
		return InstallExistingConfig{}, fmt.Errorf("invalid late findings mode %q", *lateFindings)
	}
	if *duration < 0 || *responseTimeout <= 0 || *verificationTimeout <= 0 {
		return InstallExistingConfig{}, errors.New("controller timeouts must be positive")
	}
	if *programTrustThreshold < 1 {
		return InstallExistingConfig{}, errors.New("program trust threshold must be positive")
	}
	config := install.Config{
		Namespace: *namespace, ControlNamespace: *controlNamespace, Application: *application,
		ControllerImage: *controllerImage, ResponderImage: *responderImage, ValidatorImage: *validatorImage,
		RepositoryPVC: *repositoryPVC, CredentialsSecret: *credentialsSecret, Model: *model,
		RepairPolicy: *repairPolicy, ReflectionSession: *reflectionSession, ReflectionGuidance: *reflectionGuidance, ForceReflection: *forceReflection, AgentProvider: *provider,
		MaxFollowUps: *maxFollowUps, FollowUpCooldownSeconds: int(followUpCooldown.Seconds()), MaxResponseLoops: *maxResponseLoops,
		ProgramFastPath: *programFastPath, ProgramTrustThreshold: *programTrustThreshold,
		LateFindings:    *lateFindings,
		HealthyBaseline: *healthyBaseline,
		TimeoutSeconds:  int(responseTimeout.Seconds()), VerificationTimeoutSeconds: int(verificationTimeout.Seconds()),
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
	if *duration > 0 {
		config.ExtraControllerArgs = append(config.ExtraControllerArgs, "--duration", duration.String())
	}
	if *exitAfterClosure {
		config.ExtraControllerArgs = append(config.ExtraControllerArgs, "--exit-after-closure")
	}
	return InstallExistingConfig{Repository: *repository, Kubeconfig: *kubeconfig, Duration: *duration, Install: config}, nil
}

func RunInstallExisting(ctx context.Context, launch InstallExistingConfig) (InstallExistingResult, error) {
	commit, err := acceptedHead(ctx, launch.Repository)
	if err != nil {
		return InstallExistingResult{}, err
	}
	client, err := hostKubernetesClient(launch.Kubeconfig)
	if err != nil {
		return InstallExistingResult{}, err
	}
	config := launch.Install
	config.AcceptedCommit = commit
	installer := install.Installer{
		Client:         client,
		SeedRepository: install.KubectlTarSeederWithKubeconfig(launch.Repository, commit, launch.Kubeconfig),
		SyncRepository: install.KubectlTarSynchronizerWithKubeconfig(launch.Repository, launch.Kubeconfig),
	}
	result, err := installer.Install(ctx, config)
	if err != nil {
		return InstallExistingResult{}, err
	}
	return InstallExistingResult{Installed: true, Reused: result.Reused, Fingerprint: result.Fingerprint, AcceptedCommit: commit, ControllerLogs: result.ControllerLogs}, nil
}

package operatorcli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"encoding/json"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sdo.dev/operator/deployment"
	"sdo.dev/operator/install"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/worker_registry"
)

// HostConfig is the local side of native bootstrap. The host only supplies a
// clean accepted Git checkout and Kubernetes deployment configuration.
type HostConfig struct {
	Repository, Objective, OperationID string
	MaxAttempts                        int
	Install                            install.Config
	Kubeconfig                         string
	RegistrationSource                 string
}

func ParseHostLaunch(args []string) (HostConfig, error) {
	if len(args) == 0 || args[0] != "host" {
		return HostConfig{}, errors.New("expected host subcommand")
	}
	flags := flag.NewFlagSet("sdo-operator host", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	repository := flags.String("repository", "", "clean accepted Git repository")
	ceilingJSON := flags.String("observation-ceiling-json", "", "installer-owned observation ceiling JSON")
	namespace := flags.String("namespace", "", "application namespace")
	control := flags.String("control-namespace", "", "operator namespace")
	application := flags.String("application", "", "application identity")
	objective := flags.String("health-objective", "", "human health objective")
	flags.StringVar(objective, "goal", "", "human health objective")
	goalFile := flags.String("goal-file", "", "file containing human health objective")
	operationID := flags.String("operation-id", "", "durable operation identity")
	maxAttempts := flags.Int("max-attempts", 3, "maximum deployment attempts")
	controllerImage := flags.String("controller-image", "", "compiled operator image")
	responderImage := flags.String("responder-image", "", "agent worker image")
	validatorImage := flags.String("validator-image", "", "isolated validator image")
	buildkitImage := flags.String("buildkit-image", "", "digest-pinned image builder")
	repositoryPVC := flags.String("repository-pvc", "", "shared accepted repository PVC")
	credentialsSecret := flags.String("credentials-secret", "", "agent credentials Secret")
	registrySecret := flags.String("registry-push-secret", "", "separate image registry credentials Secret")
	registryHost := flags.String("registry-host", "", "trusted source image registry host[:port]")
	model := flags.String("model", "", "agent model")
	provider := flags.String("agent-provider", "codex", "agent provider")
	registrationSource := flags.String("agent-registrations", "", "local registration JSON directory")
	registrationConfigMap := flags.String("agent-registration-configmap", "", "registration ConfigMap name")
	controllerDuration := flags.Duration("controller-duration", 0, "bounded controller run duration")
	exitAfterClosure := flags.Bool("exit-after-closure", false, "stop after first closure")
	lateFindings := flags.String("late-findings", "off", "late finding pull mode: off or pull")
	repairPolicy := flags.String("repair-policy", "recorded-actions", "repair evidence policy")
	reflectionSession := flags.String("reflection-session", "resume", "reflection session mode: fresh or resume")
	reflectionGuidance := flags.String("reflection-guidance", "baseline", "reflection guidance mode")
	forceReflection := flags.Bool("force-reflection", false, "always dispatch reflection after verified closure")
	maxResponseLoops := flags.Int("max-response-loops", 10, "maximum sdo apply batches per incident")
	programFastPath := flags.Bool("program-fast-path", true, "allow trusted programs to resolve incidents")
	programTrustThreshold := flags.Int("program-trust-threshold", 1, "verified successes required to trust a program")
	responseTimeout := flags.Duration("response-timeout", 30*time.Minute, "responder timeout")
	verificationTimeout := flags.Duration("verification-timeout", 5*time.Minute, "independent verification timeout")
	registryInsecure := flags.Bool("registry-insecure", false, "allow an explicitly configured HTTP image registry")
	permissiveResponder := flags.Bool("permissive-responder", false, "broad lane repair permissions excluding Secret and credential APIs")
	var responderNamespaces repeatable
	flags.Var(&responderNamespaces, "responder-namespace", "additional lane namespace for permissive responder")
	healthyBaseline := flags.Bool("healthy-baseline", false, "validate incident detectors against recorded healthy snapshots")
	metricsURL := flags.String("metrics-url", "", "Prometheus-compatible API URL; installs the metrics alerts detector and lets metric detectors query it")
	var responderEnv repeatable
	flags.Var(&responderEnv, "responder-env", "responder environment NAME=VALUE")
	kubeconfig := flags.String("kubeconfig", "", "Kubernetes client and kubectl config")
	if err := flags.Parse(args[1:]); err != nil {
		return HostConfig{}, err
	}

	menuEnabled, menuErr := responderMenuEnabled()
	if menuErr != nil {
		return HostConfig{}, menuErr
	}
	if flags.NArg() != 0 {
		return HostConfig{}, fmt.Errorf("unexpected host arguments: %v", flags.Args())
	}
	if (*goalFile != "") == (*objective != "") {
		return HostConfig{}, errors.New("host requires exactly one of --health-objective/--goal or --goal-file")
	}
	if *goalFile != "" {
		if !filepath.IsAbs(*goalFile) {
			return HostConfig{}, errors.New("goal file path must be absolute")
		}
		data, err := os.ReadFile(*goalFile)
		if err != nil {
			return HostConfig{}, fmt.Errorf("read goal file: %w", err)
		}
		if len(data) > 1<<20 {
			return HostConfig{}, errors.New("goal file exceeds 1 MiB")
		}
		*objective = strings.TrimSpace(string(data))
	}
	for name, value := range map[string]string{
		"repository": *repository, "namespace": *namespace, "application": *application,
		"health objective": *objective, "operation ID": *operationID,
		"controller image": *controllerImage, "responder image": *responderImage,
		"validator image": *validatorImage, "BuildKit image": *buildkitImage,
		"repository PVC": *repositoryPVC, "agent credentials Secret": *credentialsSecret,
		"registry push Secret": *registrySecret, "model": *model,
	} {
		if strings.TrimSpace(value) == "" {
			return HostConfig{}, fmt.Errorf("host launch requires %s", name)
		}
	}
	if !filepath.IsAbs(*repository) || *maxAttempts < 1 || (*provider != "codex" && *provider != "claude") {
		return HostConfig{}, errors.New("host launch requires an absolute repository, positive max attempts, and supported agent provider")
	}
	if *controllerDuration < 0 || (*controllerDuration > 0 && controllerDuration.Seconds() < 1) {
		return HostConfig{}, errors.New("bounded controller duration must be positive whole seconds")
	}
	if *lateFindings != "off" && *lateFindings != "pull" {
		return HostConfig{}, fmt.Errorf("invalid late findings mode %q", *lateFindings)
	}
	if (*repairPolicy != "commit" && *repairPolicy != "recorded-actions") || *responseTimeout < time.Second || *verificationTimeout < time.Second {
		return HostConfig{}, errors.New("host requires supported repair policy and positive controller timeouts")
	}
	if *reflectionSession != "fresh" && *reflectionSession != "resume" {
		return HostConfig{}, fmt.Errorf("unsupported reflection session mode %q", *reflectionSession)
	}
	if *reflectionGuidance != "baseline" && *reflectionGuidance != "generalize" && *reflectionGuidance != "generalize-spec" {
		return HostConfig{}, fmt.Errorf("unsupported reflection guidance %q", *reflectionGuidance)
	}
	if !strings.Contains(*buildkitImage, "@sha256:") || *credentialsSecret == *registrySecret {
		return HostConfig{}, errors.New("host launch requires digest-pinned BuildKit image and separate registry push Secret")
	}
	if *registryHost != "" {
		if err := deployment.ValidateRegistryHost(*registryHost); err != nil {
			return HostConfig{}, fmt.Errorf("invalid trusted registry host: %w", err)
		}
	}
	if *registrationSource != "" && *registrationConfigMap == "" {
		return HostConfig{}, errors.New("local agent registrations require a ConfigMap name")
	}
	if *kubeconfig != "" && !filepath.IsAbs(*kubeconfig) {
		return HostConfig{}, errors.New("kubeconfig path must be absolute")
	}
	var ceiling sdk.ObservationScope
	if *ceilingJSON != "" {
		decoder := json.NewDecoder(strings.NewReader(*ceilingJSON))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&ceiling); err != nil {
			return HostConfig{}, err
		}
		if err := ceiling.Validate(); err != nil {
			return HostConfig{}, err
		}
	}
	config := install.Config{
		ResponderMenu:      menuEnabled,
		ObservationCeiling: ceiling,
		Namespace:          *namespace, ControlNamespace: *control, Application: *application,
		ControllerImage: *controllerImage, ResponderImage: *responderImage,
		ValidatorImage: *validatorImage, BuildKitImage: *buildkitImage,
		RepositoryPVC: *repositoryPVC, CredentialsSecret: *credentialsSecret,
		RegistryPushSecret: *registrySecret, RegistryHost: *registryHost, Model: *model, AgentProvider: *provider,
		RegistryInsecure: *registryInsecure, ResponderEnv: responderEnv, HealthyBaseline: *healthyBaseline, MetricsURL: *metricsURL, PermissiveResponder: *permissiveResponder, ResponderNamespaces: responderNamespaces,
		ControllerDurationSeconds: int(controllerDuration.Seconds()), ExitAfterClosure: *exitAfterClosure, WaitForCompletion: *controllerDuration > 0 || *exitAfterClosure,
		LateFindings: *lateFindings, ReflectionSession: *reflectionSession, ReflectionGuidance: *reflectionGuidance, ForceReflection: *forceReflection,
		MaxResponseLoops: *maxResponseLoops,
		ProgramFastPath:  *programFastPath, ProgramTrustThreshold: *programTrustThreshold,
		TimeoutSeconds: int(responseTimeout.Seconds()), VerificationTimeoutSeconds: int(verificationTimeout.Seconds()), RepairPolicy: *repairPolicy,
		NativeCommand:         []string{"/usr/local/bin/sdo-operator"},
		RegistrationConfigMap: *registrationConfigMap,
	}
	if *registrationConfigMap != "" {
		config.RegistrationDirectory = "/sdo/registrations"
	}
	return HostConfig{Repository: *repository, Objective: *objective, OperationID: *operationID, MaxAttempts: *maxAttempts, Install: config, Kubeconfig: *kubeconfig, RegistrationSource: *registrationSource}, nil
}

func acceptedHead(ctx context.Context, repository string) (string, error) {
	root, err := exec.CommandContext(ctx, "git", "-C", repository, "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return "", fmt.Errorf("inspect accepted repository root: %w", err)
	}
	resolvedRoot, err := filepath.EvalSymlinks(strings.TrimSpace(string(root)))
	if err != nil {
		return "", err
	}
	resolvedRepository, err := filepath.EvalSymlinks(repository)
	if err != nil {
		return "", err
	}
	if resolvedRoot != resolvedRepository {
		return "", errors.New("accepted repository path must be its Git root")
	}
	status, err := exec.CommandContext(ctx, "git", "-C", repository, "status", "--porcelain").Output()
	if err != nil {
		return "", fmt.Errorf("inspect accepted repository status: %w", err)
	}
	if len(status) != 0 {
		return "", errors.New("accepted repository has uncommitted changes")
	}
	commit, err := exec.CommandContext(ctx, "git", "-C", repository, "rev-parse", "HEAD").Output()
	if err != nil {
		return "", fmt.Errorf("inspect accepted commit: %w", err)
	}
	return strings.TrimSpace(string(commit)), nil
}

func hostKubernetesClient(path string) (kubernetes.Interface, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if path != "" {
		rules.ExplicitPath = path
	}
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil && path == "" {
		config, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, fmt.Errorf("load Kubernetes credentials: %w", err)
	}
	config.QPS, config.Burst = 50, 100
	return kubernetes.NewForConfig(config)
}

// RunHost prepares the initial accepted repository on the shared PVC, then
// waits for the in-cluster lifecycle and its attributable installation receipt.
func RunHost(ctx context.Context, launch HostConfig) (install.OperateResult, error) {
	commit, err := acceptedHead(ctx, launch.Repository)
	if err != nil {
		return install.OperateResult{}, err
	}
	client, err := hostKubernetesClient(launch.Kubeconfig)
	if err != nil {
		return install.OperateResult{}, err
	}
	config := launch.Install
	config.AcceptedCommit = commit
	if launch.RegistrationSource != "" {
		if _, err := worker_registry.LoadDirectory(launch.RegistrationSource); err != nil {
			return install.OperateResult{}, fmt.Errorf("validate agent registrations: %w", err)
		}
		entries, err := os.ReadDir(launch.RegistrationSource)
		if err != nil {
			return install.OperateResult{}, err
		}
		config.RegistrationData = map[string]string{}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
				continue
			}
			data, err := os.ReadFile(filepath.Join(launch.RegistrationSource, entry.Name()))
			if err != nil {
				return install.OperateResult{}, err
			}
			config.RegistrationData[entry.Name()] = string(data)
		}
		if len(config.RegistrationData) == 0 {
			return install.OperateResult{}, errors.New("registration directory has no JSON files")
		}
	}
	installer := install.Installer{Client: client}
	seed := install.KubectlTarSeederWithKubeconfig(launch.Repository, commit, launch.Kubeconfig)
	if err := installer.Prepare(ctx, config, seed); err != nil {
		return install.OperateResult{}, err
	}
	return installer.LaunchOperate(ctx, config, launch.Objective, launch.OperationID, launch.MaxAttempts)
}

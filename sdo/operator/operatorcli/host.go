package operatorcli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sdo.dev/operator/operatorcli/internal/processio"
	"strings"

	"encoding/json"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sdo.dev/operator/imagesrc/buildkit"
	"sdo.dev/operator/install"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/worker_registry"
)

// HostConfig is the local side of native bootstrap. The host only supplies a
// clean accepted Git checkout and Kubernetes deployment configuration.
type HostConfig struct {
	Repository, Objective, OperationID string
	Install                            install.Config
	Kubeconfig                         string
	RegistrationSource                 string
}

func ParseHostLaunch(args []string) (HostConfig, error) {
	return ParseHostLaunchEnv(args, processio.Environ())
}

// ParseHostLaunchEnv parses a host launch against an explicit environment (KEY=VALUE lines).
func ParseHostLaunchEnv(args []string, environ []string) (HostConfig, error) {
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
	sourceKind := flags.String("source-kind", "", "upstream watcher kind to follow (published as .sdo/goal/sources.yaml)")
	sourceConfig := flags.String("source-config", "", "JSON config of the followed upstream")
	deploymentPlan := flags.String("deployment-plan", "", "provided DeploymentPlan as ProtoJSON (source, image sources, expected workload IDs): the deployer agent is skipped and the plan is deployed as given")
	var configFiles repeatable
	flags.Var(&configFiles, "config", "operator configuration YAML (repeatable; later files override earlier ones; see docs/config.md)")
	operationID := flags.String("operation-id", "", "durable operation identity")
	controllerImage := flags.String("controller-image", "", "compiled operator image")
	responderImage := flags.String("responder-image", "", "agent worker image")
	buildkitImage := flags.String("buildkit-image", "", "digest-pinned image builder")
	repositoryPVC := flags.String("repository-pvc", "", "shared accepted repository PVC")
	credentialsSecret := flags.String("credentials-secret", "", "agent credentials Secret")
	registrySecret := flags.String("registry-push-secret", "", "separate image registry credentials Secret")
	registryHost := flags.String("registry-host", "", "trusted source image registry host[:port]")
	registrationSource := flags.String("agent-registrations", "", "local registration JSON directory")
	registrationConfigMap := flags.String("agent-registration-configmap", "", "registration ConfigMap name")
	controllerDuration := flags.Duration("controller-duration", 0, "bounded controller run duration")
	exitAfterClosure := flags.Bool("exit-after-closure", false, "stop after first closure")
	registryInsecure := flags.Bool("registry-insecure", false, "allow an explicitly configured HTTP image registry")
	kubeconfig := flags.String("kubeconfig", "", "Kubernetes client and kubectl config")
	if err := flags.Parse(args[1:]); err != nil {
		return HostConfig{}, err
	}

	if flags.NArg() != 0 {
		return HostConfig{}, fmt.Errorf("unexpected host arguments: %v", flags.Args())
	}
	resolved, err := resolveSettings(configFiles, environ)
	if err != nil {
		return HostConfig{}, err
	}
	if *goalFile != "" && *objective != "" {
		return HostConfig{}, errors.New("host accepts at most one of --health-objective/--goal or --goal-file")
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
		"operation ID":     *operationID,
		"controller image": *controllerImage, "responder image": *responderImage,
		"BuildKit image": *buildkitImage,
		"repository PVC": *repositoryPVC, "agent credentials Secret": *credentialsSecret,
		"registry push Secret": *registrySecret,
	} {
		if strings.TrimSpace(value) == "" {
			return HostConfig{}, fmt.Errorf("host launch requires %s", name)
		}
	}
	if !filepath.IsAbs(*repository) {
		return HostConfig{}, errors.New("host launch requires an absolute repository")
	}
	if *controllerDuration < 0 || (*controllerDuration > 0 && controllerDuration.Seconds() < 1) {
		return HostConfig{}, errors.New("bounded controller duration must be positive whole seconds")
	}
	if !strings.Contains(*buildkitImage, "@sha256:") || *credentialsSecret == *registrySecret {
		return HostConfig{}, errors.New("host launch requires digest-pinned BuildKit image and separate registry push Secret")
	}
	if *registryHost != "" {
		if err := buildkit.ValidateRegistryHost(*registryHost); err != nil {
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
	sources, err := parseSourceFlags(*sourceKind, *sourceConfig)
	if err != nil {
		return HostConfig{}, err
	}
	if len(sources) > 0 && strings.TrimSpace(*objective) == "" {
		return HostConfig{}, errors.New("a followed source is published with the human goal and requires a health objective")
	}
	providedPlan, err := rendered.ParseDeploymentPlan(*deploymentPlan)
	if err != nil {
		return HostConfig{}, err
	}
	compactPlan, err := rendered.FormatDeploymentPlan(providedPlan)
	if err != nil {
		return HostConfig{}, err
	}
	controlForTelemetry := *control
	if controlForTelemetry == "" {
		controlForTelemetry = *namespace
	}
	config := install.Config{
		SourceKind: *sourceKind, SourceConfig: *sourceConfig, DeploymentPlan: compactPlan,
		ObservationCeiling: ceiling,
		Namespace:          *namespace, ControlNamespace: *control, Application: *application,
		ControllerImage: *controllerImage, ResponderImage: *responderImage,
		BuildKitImage: *buildkitImage,
		RepositoryPVC: *repositoryPVC, CredentialsSecret: *credentialsSecret,
		RegistryPushSecret: *registrySecret, RegistryHost: *registryHost,
		RegistryInsecure:          *registryInsecure,
		ControllerDurationSeconds: int(controllerDuration.Seconds()), ExitAfterClosure: *exitAfterClosure, WaitForCompletion: *controllerDuration > 0 || *exitAfterClosure,
		NativeCommand:         []string{"/usr/local/bin/sdo-operator"},
		RegistrationConfigMap: *registrationConfigMap,
	}
	if err := applySettings(&config, resolved, controlForTelemetry); err != nil {
		return HostConfig{}, err
	}
	if *registrationConfigMap != "" {
		config.RegistrationDirectory = "/sdo/registrations"
	}
	return HostConfig{Repository: *repository, Objective: *objective, OperationID: *operationID, Install: config, Kubeconfig: *kubeconfig, RegistrationSource: *registrationSource}, nil
}

func hostKubernetesConfig(path string) (*rest.Config, error) {
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
	return config, nil
}

func hostKubernetesClient(path string) (kubernetes.Interface, error) {
	config, err := hostKubernetesConfig(path)
	if err != nil {
		return nil, err
	}
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
	return installer.LaunchOperate(ctx, config, launch.Objective, launch.OperationID)
}

func acceptedHead(ctx context.Context, repository string) (string, error) {
	return processio.AcceptedHead(ctx, repository)
}

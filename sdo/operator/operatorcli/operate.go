package operatorcli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"sdo.dev/operator/operatorcli/catalog/telemetrysources"
	"strings"

	"sdo.dev/operator/deploymentpolicy"
	"sdo.dev/operator/imagesrc/buildkit"
	"sdo.dev/operator/install"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory/observation"
	"sdo.dev/operator/rendered"
)

// OperateConfig is the typed, in-cluster bootstrap launch contract. The
// accepted checkout and broker workspaces must share the repository PVC.
type OperateConfig struct {
	Lifecycle             lifecycle.Config
	Install               install.Config
	ApplicationRoot       string
	RepositoryMount       string
	WorktreeRoot          string
	OperatorRoot          string
	RegistrationDirectory string
	BuildKitImage         string
	RegistryPushSecret    string
}

func ParseOperateLaunch(args []string) (OperateConfig, error) {
	if len(args) == 0 || args[0] != "operate" {
		return OperateConfig{}, errors.New("expected operate subcommand")
	}
	flags := flag.NewFlagSet("sdo-operator operate", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	app := flags.String("app", "", "accepted application checkout")
	operatorRoot := flags.String("operator-root", "/opt/sdo/sdo/operator", "installed operator modules")
	namespace := flags.String("namespace", "", "application namespace")
	controlNamespace := flags.String("control-namespace", "", "controller namespace")
	application := flags.String("application", "", "application identity")
	objective := flags.String("health-objective", "", "human health objective")
	sourceKind := flags.String("source-kind", "", "upstream watcher kind to follow (published as .sdo/goal/sources.yaml)")
	sourceConfig := flags.String("source-config", "", "JSON config of the followed upstream")
	deploymentPlan := flags.String("deployment-plan", "", "provided DeploymentPlan as ProtoJSON; the deployer agent is skipped and the plan is deployed as given")
	deploymentPolicyFlag := flags.String("deployment-policy", "", "the installer's resolved DeploymentPolicy as ProtoJSON; empty is no policy")
	evidenceSink := flags.String("evidence-sink", "", "the installer's resolved evidence sink as compact JSON (run_id, volume); empty keeps evidence on the repository volume")
	telemetryProfile := flags.String("telemetry-profile", telemetrysources.DefaultProfileName, "the installer's resolved telemetry profile: a built-in name or an inline JSON document")
	operationID := flags.String("operation-id", "", "durable operation identity")
	controllerImage := flags.String("controller-image", "", "controller image")
	responderImage := flags.String("responder-image", "", "agent worker image")
	scriptImage := flags.String("script-image", "", "fixed slim RunScript tool image")
	buildkitImage := flags.String("buildkit-image", "", "isolated source image builder")
	registryPushSecret := flags.String("registry-push-secret", "", "image registry credentials Secret")
	registryHost := flags.String("registry-host", "", "trusted source image registry host[:port]")
	repositoryPVC := flags.String("repository-pvc", "", "shared repository PVC")
	repositoryMount := flags.String("repository-mount-path", "", "shared repository PVC mount")
	credentialsSecret := flags.String("credentials-secret", "", "agent credentials Secret")
	worktreeRoot := flags.String("worktree-root", "", "broker proposal and ledger root")
	registrations := flags.String("agent-registration-directory", "", "agent registration directory")
	registrationConfigMap := flags.String("agent-registration-configmap", "", "read-only agent registration ConfigMap")
	deployerID := flags.String("deployer-agent-id", "", "deployer registration ID; defaults to the agent registered for source deployment")
	judgeID := flags.String("judge-agent-id", "", "health judge registration ID; defaults to the agent registered for health judgment")
	controllerDuration := flags.Duration("controller-duration", 0, "bounded controller run duration")
	exitAfterClosure := flags.Bool("exit-after-closure", false, "stop after first closure")
	registryInsecure := flags.Bool("registry-insecure", false, "allow an explicitly configured HTTP image registry")
	if err := flags.Parse(args[1:]); err != nil {
		return OperateConfig{}, err
	}
	// Every timing and policy value is the sdo-config the Job mounts; the controller this Job installs carries the
	// same document.
	policy, err := loadMountedSettings(mountedSettings)
	if err != nil {
		return OperateConfig{}, err
	}
	if flags.NArg() != 0 {
		return OperateConfig{}, fmt.Errorf("unexpected operate arguments: %v", flags.Args())
	}
	for key, value := range map[string]string{
		"application checkout": *app, "operator root": *operatorRoot, "namespace": *namespace,
		"application": *application, "operation ID": *operationID,
		"controller image": *controllerImage, "responder image": *responderImage,
		"repository PVC": *repositoryPVC,
		"BuildKit image": *buildkitImage, "registry push Secret": *registryPushSecret,
		"repository mount": *repositoryMount, "credentials Secret": *credentialsSecret,
		"worktree root": *worktreeRoot,
	} {
		if strings.TrimSpace(value) == "" {
			return OperateConfig{}, fmt.Errorf("operate requires %s", key)
		}
	}
	if *controllerDuration < 0 || (*controllerDuration > 0 && controllerDuration.Seconds() < 1) {
		return OperateConfig{}, errors.New("bounded controller duration must be positive whole seconds")
	}
	if !strings.Contains(*buildkitImage, "@sha256:") || *registryPushSecret == *credentialsSecret {
		return OperateConfig{}, errors.New("operate requires a digest-pinned BuildKit image and separate registry push Secret")
	}
	if *registryHost != "" {
		if err := buildkit.ValidateRegistryHost(*registryHost); err != nil {
			return OperateConfig{}, fmt.Errorf("invalid trusted registry host: %w", err)
		}
	}
	if !filepath.IsAbs(*app) || !filepath.IsAbs(*repositoryMount) || !filepath.IsAbs(*worktreeRoot) || !filepath.IsAbs(*operatorRoot) {
		return OperateConfig{}, errors.New("operate paths must be absolute")
	}
	inside := func(root, child string) bool {
		rel, err := filepath.Rel(root, child)
		return err == nil && rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
	}
	if !inside(*repositoryMount, *app) || !inside(*repositoryMount, *worktreeRoot) {
		return OperateConfig{}, errors.New("application and proposal root must be inside the shared repository mount")
	}
	if inside(*app, *worktreeRoot) || inside(*worktreeRoot, *app) || filepath.Clean(*app) == filepath.Clean(*worktreeRoot) {
		return OperateConfig{}, errors.New("accepted application and broker proposals must be separate")
	}
	if filepath.Clean(*app) != filepath.Join(filepath.Clean(*repositoryMount), "application") {
		return OperateConfig{}, errors.New("accepted checkout must be the application directory on the shared repository mount")
	}
	if (*registrations == "") != (*registrationConfigMap == "") {
		return OperateConfig{}, errors.New("agent registration directory and ConfigMap must be provided together")
	}
	control := *controlNamespace
	if control == "" {
		control = *namespace
	}
	ceiling, err := observation.CeilingFromEnvironment()
	if err != nil {
		return OperateConfig{}, err
	}
	profile, err := loadTelemetryProfile(*telemetryProfile, control)
	if err != nil {
		return OperateConfig{}, err
	}
	sources, err := parseSourceFlags(*sourceKind, *sourceConfig)
	if err != nil {
		return OperateConfig{}, err
	}
	if len(sources) > 0 && strings.TrimSpace(*objective) == "" {
		return OperateConfig{}, errors.New("a followed source is published with the human goal and requires --health-objective")
	}
	if _, err := deploymentpolicy.Parse(*deploymentPolicyFlag); err != nil {
		return OperateConfig{}, err
	}
	if _, err := install.ParseEvidenceSink(*evidenceSink); err != nil {
		return OperateConfig{}, err
	}
	providedPlan, err := rendered.ParseDeploymentPlan(*deploymentPlan)
	if err != nil {
		return OperateConfig{}, err
	}
	config := OperateConfig{
		Lifecycle: lifecycle.Config{
			ProvidedPlan: providedPlan,
			OperationID:  *operationID, Application: *application, Namespace: *namespace,
			HealthObjective: *objective, Sources: sources, DeployerAgentID: *deployerID, JudgeAgentID: *judgeID,
			RegistryHost: *registryHost,
			MaxAttempts:  policy.Deployment.MaxAttempts,
		},
		Install: install.Config{
			Settings:       &policy,
			BackendRecipes: telemetrysources.Recipes(), TelemetryProfile: profile,
			ObservationCeiling: ceiling,
			Namespace:          *namespace, ControlNamespace: control, Application: *application,
			SourceKind: *sourceKind, SourceConfig: *sourceConfig, DeploymentPlan: *deploymentPlan, DeploymentPolicy: *deploymentPolicyFlag, EvidenceSink: *evidenceSink,
			ControllerImage: *controllerImage, ResponderImage: *responderImage,
			ScriptImage: *scriptImage, RepositoryPVC: *repositoryPVC,
			CredentialsSecret: *credentialsSecret,
			// The controller parent deploys upstream advances with the same identity.
			BuildKitImage: *buildkitImage, RegistryPushSecret: *registryPushSecret,
			RegistryInsecure: *registryInsecure, RegistryHost: *registryHost,
			ControllerDurationSeconds: int(controllerDuration.Seconds()), ExitAfterClosure: *exitAfterClosure, WaitForCompletion: *controllerDuration > 0 || *exitAfterClosure,
			NativeCommand:         []string{"/usr/local/bin/sdo-operator"},
			ReuseExisting:         true,
			RegistrationConfigMap: *registrationConfigMap, RegistrationDirectory: *registrations,
		},
		ApplicationRoot: *app, RepositoryMount: *repositoryMount, WorktreeRoot: *worktreeRoot,
		OperatorRoot: *operatorRoot, RegistrationDirectory: *registrations,
		BuildKitImage: *buildkitImage, RegistryPushSecret: *registryPushSecret,
	}
	if *registrations != "" {
		if !filepath.IsAbs(*registrations) {
			return OperateConfig{}, errors.New("agent registration directory must be absolute")
		}
	}
	return config, nil
}

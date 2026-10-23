package operatorcli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"sdo.dev/operator/deployment"
	"sdo.dev/operator/install"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory/observation"
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
	operationID := flags.String("operation-id", "", "durable operation identity")
	maxAttempts := flags.Int("max-attempts", 3, "maximum deployment attempts")
	controllerImage := flags.String("controller-image", "", "controller image")
	responderImage := flags.String("responder-image", "", "agent worker image")
	validatorImage := flags.String("validator-image", "", "isolated validator image")
	buildkitImage := flags.String("buildkit-image", "", "isolated source image builder")
	registryPushSecret := flags.String("registry-push-secret", "", "image registry credentials Secret")
	registryHost := flags.String("registry-host", "", "trusted source image registry host[:port]")
	repositoryPVC := flags.String("repository-pvc", "", "shared repository PVC")
	repositoryMount := flags.String("repository-mount-path", "", "shared repository PVC mount")
	credentialsSecret := flags.String("credentials-secret", "", "agent credentials Secret")
	model := flags.String("model", "", "agent model")
	provider := flags.String("agent-provider", "codex", "agent provider")
	worktreeRoot := flags.String("worktree-root", "", "broker proposal and ledger root")
	registrations := flags.String("agent-registration-directory", "", "agent registration directory")
	registrationConfigMap := flags.String("agent-registration-configmap", "", "read-only agent registration ConfigMap")
	deployerID := flags.String("deployer-agent-id", "", "deployer registration ID; defaults to the agent registered for source deployment")
	judgeID := flags.String("judge-agent-id", "", "health judge registration ID; defaults to the agent registered for health judgment")
	controllerDuration := flags.Duration("controller-duration", 0, "bounded controller run duration")
	exitAfterClosure := flags.Bool("exit-after-closure", false, "stop after first closure")
	lateFindings := flags.String("late-findings", "off", "late finding pull mode: off or pull")
	repairPolicy := flags.String("repair-policy", "commit", "repair evidence policy")
	reflectionSession := flags.String("reflection-session", "resume", "reflection session mode: fresh or resume")
	reflectionGuidance := flags.String("reflection-guidance", "baseline", "reflection guidance mode")
	forceReflection := flags.Bool("force-reflection", false, "always dispatch reflection after verified closure")
	maxResponseLoops := flags.Int("max-response-loops", 5, "maximum sdo apply batches per incident")
	programFastPath := flags.Bool("program-fast-path", false, "allow trusted programs to resolve incidents")
	programTrustThreshold := flags.Int("program-trust-threshold", 3, "verified successes required to trust a program")
	responseTimeout := flags.Duration("response-timeout", 30*time.Minute, "responder timeout")
	verificationTimeout := flags.Duration("verification-timeout", 2*time.Minute, "independent verification timeout")
	registryInsecure := flags.Bool("registry-insecure", false, "allow an explicitly configured HTTP image registry")
	healthyBaseline := flags.Bool("healthy-baseline", false, "validate incident detectors against recorded healthy snapshots")
	var responderEnv repeatable
	flags.Var(&responderEnv, "responder-env", "responder environment NAME=VALUE")
	if err := flags.Parse(args[1:]); err != nil {
		return OperateConfig{}, err
	}
	if flags.NArg() != 0 {
		return OperateConfig{}, fmt.Errorf("unexpected operate arguments: %v", flags.Args())
	}
	for key, value := range map[string]string{
		"application checkout": *app, "operator root": *operatorRoot, "namespace": *namespace,
		"application": *application, "health objective": *objective, "operation ID": *operationID,
		"controller image": *controllerImage, "responder image": *responderImage,
		"validator image": *validatorImage, "repository PVC": *repositoryPVC,
		"BuildKit image": *buildkitImage, "registry push Secret": *registryPushSecret,
		"repository mount": *repositoryMount, "credentials Secret": *credentialsSecret,
		"model": *model, "agent provider": *provider, "worktree root": *worktreeRoot,
	} {
		if strings.TrimSpace(value) == "" {
			return OperateConfig{}, fmt.Errorf("operate requires %s", key)
		}
	}
	if *maxAttempts < 1 || (*provider != "codex" && *provider != "claude") {
		return OperateConfig{}, errors.New("operate requires positive max attempts and supported agent provider")
	}
	if *controllerDuration < 0 || (*controllerDuration > 0 && controllerDuration.Seconds() < 1) {
		return OperateConfig{}, errors.New("bounded controller duration must be positive whole seconds")
	}
	if *lateFindings != "off" && *lateFindings != "pull" {
		return OperateConfig{}, fmt.Errorf("invalid late findings mode %q", *lateFindings)
	}
	if (*repairPolicy != "commit" && *repairPolicy != "recorded-actions") || *responseTimeout < time.Second || *verificationTimeout < time.Second {
		return OperateConfig{}, errors.New("operate requires supported repair policy and positive controller timeouts")
	}
	if *reflectionSession != "fresh" && *reflectionSession != "resume" {
		return OperateConfig{}, fmt.Errorf("unsupported reflection session mode %q", *reflectionSession)
	}
	if *reflectionGuidance != "baseline" && *reflectionGuidance != "generalize" && *reflectionGuidance != "generalize-spec" {
		return OperateConfig{}, fmt.Errorf("unsupported reflection guidance %q", *reflectionGuidance)
	}
	if !strings.Contains(*buildkitImage, "@sha256:") || *registryPushSecret == *credentialsSecret {
		return OperateConfig{}, errors.New("operate requires a digest-pinned BuildKit image and separate registry push Secret")
	}
	if *registryHost != "" {
		if err := deployment.ValidateRegistryHost(*registryHost); err != nil {
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
	config := OperateConfig{
		Lifecycle: lifecycle.Config{
			OperationID: *operationID, Application: *application, Namespace: *namespace,
			HealthObjective: *objective, DeployerAgentID: *deployerID, JudgeAgentID: *judgeID,
			RegistryHost: *registryHost,
			MaxAttempts:  *maxAttempts,
		},
		Install: install.Config{
			ObservationCeiling: ceiling,
			Namespace:          *namespace, ControlNamespace: control, Application: *application,
			ControllerImage: *controllerImage, ResponderImage: *responderImage,
			ValidatorImage: *validatorImage, RepositoryPVC: *repositoryPVC,
			CredentialsSecret: *credentialsSecret, Model: *model, AgentProvider: *provider,
			RegistryInsecure: *registryInsecure, RegistryHost: *registryHost, ResponderEnv: responderEnv, HealthyBaseline: *healthyBaseline,
			ControllerDurationSeconds: int(controllerDuration.Seconds()), ExitAfterClosure: *exitAfterClosure, WaitForCompletion: *controllerDuration > 0 || *exitAfterClosure,
			LateFindings: *lateFindings, ReflectionSession: *reflectionSession, ReflectionGuidance: *reflectionGuidance, ForceReflection: *forceReflection,
			MaxResponseLoops: *maxResponseLoops,
			ProgramFastPath:  *programFastPath, ProgramTrustThreshold: *programTrustThreshold,
			TimeoutSeconds: int(responseTimeout.Seconds()), VerificationTimeoutSeconds: int(verificationTimeout.Seconds()), RepairPolicy: *repairPolicy,
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
	for _, value := range responderEnv {
		name, _, found := strings.Cut(value, "=")
		if !found || name == "" || strings.ContainsAny(name, "=\r\n") {
			return OperateConfig{}, fmt.Errorf("invalid responder environment %q", value)
		}
	}
	return config, nil
}

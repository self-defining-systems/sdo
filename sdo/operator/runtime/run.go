package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/runtime/internal/lifecycleio"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"sdo.dev/operator/apihistory"
	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/core"
	"sdo.dev/operator/latefindings"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/observation"
	"sdo.dev/operator/onboarding"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/traffic"
	"sdo.dev/operator/shellpath"
	"sdo.dev/operator/toolchain"
	"sdo.dev/operator/worker_registry"
)

type RuntimeOptions struct {
	ShellReportReader shellpath.Reader

	GenericCapture *core.GenericCapture
	Programs       program.ProgramRegistry
	Args           []string
	Stdout         io.Writer
	Stderr         io.Writer
	Provider       SnapshotProvider

	// Client, when set, replaces the kubeconfig clients of the long-running
	// controller (observation, state, dispatch, and leader election), so the
	// production wiring can run against a fake API server.
	Client kubernetes.Interface
	// MetadataClient replaces the Secret metadata transport in embedded runs.
	// When Client is supplied without it, Secrets remain unobserved.
	MetadataClient rest.Interface
	// PlanExecConfig reuses controller credentials for ordinary pod exec.
	PlanExecConfig *rest.Config
	// StateBaselineStartTimeout bounds the state tracker's RBAC probes and
	// initial sync; zero means defaultStateBaselineStartTimeout.
	StateBaselineStartTimeout time.Duration
	// NativeBroker supplies the Go-owned closure path in tests and embedded runs.
	NativeBroker *NativeIncidentBroker
	// Dispatcher, when set, replaces the Kubernetes Job dispatcher so tests
	// can observe what the controller dispatches without a worker cluster.
	Dispatcher Dispatcher
}

const defaultStateBaselineStartTimeout = 30 * time.Second

type repeatedFlag []string

func (values *repeatedFlag) String() string { return fmt.Sprint([]string(*values)) }
func (values *repeatedFlag) Set(value string) error {
	*values = append(*values, value)
	return nil
}

func Run(detectors []sdk.Detector) {
	RunWithPrograms(detectors, nil)
}

func RunWithPrograms(detectors []sdk.Detector, programs program.ProgramRegistry) {
	if err := RunWithOptions(context.Background(), detectors, RuntimeOptions{Programs: programs}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		if errors.Is(err, memory.ErrHealthRefreshRequired) {
			os.Exit(memory.HealthRefreshExitCode)
		}
		os.Exit(1)
	}
}

func RunWithOptions(ctx context.Context, detectors []sdk.Detector, options RuntimeOptions) (runErr error) {
	// The typed launch path fills options.Client itself; that is still a
	// production client and must receive the metadata transport below.
	clientInjected := options.Client != nil
	args := options.Args
	if args == nil {
		args = os.Args[1:]
	}
	stdout := options.Stdout
	if stdout == nil {
		stdout = os.Stdout
	}
	stderr := options.Stderr
	if stderr == nil {
		stderr = os.Stderr
	}
	defer func() {
		if errors.Is(runErr, ErrControllerRolloutRestartRequired) {
			_ = json.NewEncoder(stdout).Encode(map[string]any{"controller_rollout_restart": true})
			runErr = nil
		}
	}()
	var launchConfig *contractsv1alpha1.ControllerConfig
	if len(args) > 0 && args[0] == LaunchConfigFlag {
		// Launch from one typed ControllerConfig the launcher mounted, instead
		// of a hand-mirrored flag list. The config expands to this controller's
		// own flags through the single in-tree mapping, so the flag parsing and
		// validation below stay the one source of flag semantics.
		if len(args) < 2 {
			return fmt.Errorf("%s requires a controller config path", LaunchConfigFlag)
		}
		config, err := LoadControllerConfig(args[1])
		if err != nil {
			return err
		}
		launchConfig = config
		if options.NativeBroker != nil {
			return errors.New("native broker was supplied twice")
		}
		native, client, err := nativeBrokerFromLaunch(ctx, config, options.Client)
		if err != nil {
			return err
		}
		options.NativeBroker, options.Client = native, client
		args = append(controllerConfigToArgs(config), args[2:]...)
	}
	if len(args) > 0 && args[0] == "--run-once" {
		return core.RunWithOptions(ctx, detectors, core.RuntimeOptions{Args: args[1:], Stdout: stdout, Stderr: stderr})
	}
	if len(args) > 0 && args[0] == "--evaluate-once" {
		return evaluateOnce(ctx, detectors, args[1:], stdout, stderr, options.Provider)
	}

	flags := flag.NewFlagSet("sdo-controller", flag.ContinueOnError)
	flags.SetOutput(stderr)
	codePathName := flags.String("code-path", "go", "programming-model plugin set")
	codePathOverrides := flags.String("code-path-overrides", "", "optional named code-path seam overrides as JSON")
	var codePathFlagConfig CodePathConfig
	flags.Func("response-stages", "ordered response stages; overrides the selected code path", func(value string) error {
		codePathFlagConfig.Overrides.ResponseStages = strings.Split(value, ",")
		return nil
	})
	namespace := flags.String("namespace", "", "Kubernetes namespace to observe")
	controlNamespace := flags.String(
		"control-namespace",
		"",
		"namespace holding the controller's Lease, state, maintenance ConfigMap, and responder Jobs; defaults to --namespace",
	)
	appRoot := flags.String("app-root", "", "accepted application repository root")
	application := flags.String("application", "", "application identity; defaults to repository directory name")
	sourceCommit := flags.String("source-commit", "", "source repository commit; defaults to git HEAD")
	deployedCommit := flags.String("deployed-commit", "", "deployed commit; defaults to source commit")
	var deployedManifestPaths repeatedFlag
	flags.Var(&deployedManifestPaths, "deployed-manifest-path", "manifest path selected by the accepted deployment plan")
	flags.String("dispatcher", "", "deprecated; use agent registrations")
	dispatcherMode := flags.String("dispatcher-mode", "job", "dispatcher mode: job (production) or local (development)")
	responderImage := flags.String("responder-image", "", "responder container image for job mode")
	agentRegistrationDirectory := flags.String("agent-registration-directory", "", "directory of strict ProtoJSON agent registrations; defaults to the built-in manifests in the controller image, launched with --responder-image")
	responderAgentID := flags.String("responder-agent-id", "", "registered agent ID for incident response; defaults to the one agent registered for incident requests")
	repositoryPVC := flags.String("repository-pvc", "", "application repository PVC for job mode")
	repositoryMountPath := flags.String("repository-mount-path", "/workspace", "shared repository PVC mount root for job mode")
	repositoryPVCSubPath := flags.String("repository-pvc-subpath", "", "optional shared repository PVC subPath for job mode")
	credentialsSecret := flags.String("responder-credentials-secret", "", "Codex credentials Secret for job mode")
	var responderEnvironment repeatedFlag
	responderStandby := flags.Bool("responder-standby", true, "pre-start a responder; false forces cold Jobs")
	flags.Var(&responderEnvironment, "responder-env", "responder Job environment NAME=VALUE; may be repeated")
	telemetryPath := flags.String(
		"firing-telemetry-path",
		"",
		"detector firing telemetry JSONL stream; defaults to <repository-mount-path>/.sdo-runtime/telemetry/detector-firings.jsonl "+
			"in job mode and to disabled in local mode; \"off\" disables it",
	)
	lifecyclePath := flags.String(
		"lifecycle-telemetry-path",
		"",
		"incident lifecycle telemetry JSONL stream; defaults to <repository-mount-path>/.sdo-runtime/telemetry/lifecycle-events.jsonl "+
			"in job mode and to disabled in local mode; \"off\" disables it",
	)
	duration := flags.Duration("duration", 0, "bounded controller duration; zero runs until cancellation")
	responseTimeout := flags.Duration("response-timeout", 30*time.Minute, "incident responder timeout")
	verificationTimeout := flags.Duration(
		"verification-timeout",
		5*time.Minute,
		"maximum wait for independent health detectors to clear after a response; the incident then closes as failed",
	)
	maxResponseLoops := flags.Int("max-response-loops", DefaultMaxResponseLoops, "maximum sdo apply batches per incident")
	diagnosisDeadline := flags.Duration("diagnosis-deadline", DefaultDiagnosisDeadline, "shared deadline for candidate diagnosis")
	driftBatchWindow := flags.Duration("drift-batch-window", 20*time.Second, "batch window for unexplained drift")
	repairPolicy := flags.String("repair-policy", "recorded-actions", "repair evidence policy: commit or recorded-actions")
	leaseName := flags.String("lease-name", "sdo-controller", "leader-election Lease name")
	leaseDuration := flags.Duration("lease-duration", 60*time.Second, "leader-election Lease duration")
	identity := flags.String("identity", defaultIdentity(), "unique leader-election identity")
	closureStability := flags.Duration("closure-stability-window", 15*time.Second, "stable independent verification required to close a running incident")
	reportGrace := flags.Duration("responder-report-grace", 30*time.Second, "time for a verified responder to finish its report")
	exitAfterClosure := flags.Bool("exit-after-closure", false, "exit after one closure is durably acknowledged")
	restartAfterClosure := flags.Bool(
		"restart-after-closure",
		false,
		"exit after each acknowledged closure in non-native mode",
	)
	forceReflection := flags.Bool("force-reflection", false, "always dispatch reflection after verified closure")
	maintenanceConfigMap := flags.String(
		"maintenance-configmap", MaintenanceConfigMapName, "maintenance ConfigMap in the control namespace",
	)
	maintenancePollInterval := flags.Duration("maintenance-poll-interval", time.Second, "maintenance ConfigMap poll interval")
	resumeSyncTimeout := flags.Duration(
		"resume-sync-timeout", time.Minute, "maximum wait for a fresh application cache when maintenance ends",
	)
	syntheticTraffic := flags.Bool(
		"synthetic-traffic", true, "run the health judge's synthetic-traffic workloads in an isolated prober",
	)
	syntheticWarmup := flags.Duration(
		"synthetic-traffic-warmup", 5*time.Second,
		"maximum wait for a first sample of every synthetic scenario before the first evaluation after start or resume",
	)
	proberBinary := flags.String(
		"prober-binary", "", "compiled routing and traffic prober on the shared repository volume; the controller runs it as an isolated pod",
	)
	toolchainImage := flags.String("toolchain-image", "", "trusted controller image supplying Go to workers and validators (required in job mode)")
	scriptImage := flags.String("script-image", "", "fixed script-tools image; defaults to the trusted toolchain image")
	proberImage := flags.String("prober-image", "", "image that runs the prober binary; defaults to --responder-image")
	proberURL := flags.String("prober-url", "", "use an already running prober at this URL instead of starting one")
	metricsURL := flags.String(
		"metrics-url", "", "Prometheus-compatible API URL that metric and alert detectors query once per evaluation cycle",
	)
	metricsTokenFile := flags.String("metrics-token-file", "", "file holding a bearer token for --metrics-url, read on every request")
	cleanResponderHelpers := flags.Bool(
		"clean-responder-helpers", true,
		"delete pods and Jobs labelled "+ResponderHelperLabel+"=true once the responder completes",
	)
	stateBaseline := flags.Bool(
		"state-baseline", true,
		"attach the application's configuration changes since its last healthy baseline to each incident",
	)
	if err := flags.Parse(args); err != nil {
		return err
	}

	selectedPathConfig, err := codePathFromFlags(*codePathName, *codePathOverrides)
	if err != nil {
		return err
	}
	if codePathFlagConfig.Overrides.ResponseStages != nil {
		selectedPathConfig.Overrides.ResponseStages = codePathFlagConfig.Overrides.ResponseStages
	}
	codePathRegistry := NewCodePathRegistry()
	selectedCodePath, err := codePathRegistry.Resolve(selectedPathConfig)
	if err != nil {
		return err
	}
	if selectedCodePath.Selection.Coverage == "none" {
		options.Programs = nil
	}
	menuEnabled := os.Getenv("SDO_RESPONDER_MENU")
	if menuEnabled != "" && menuEnabled != "0" && menuEnabled != "1" {
		return errors.New("SDO_RESPONDER_MENU must be 0 or 1")
	}

	timeScale, err := developmentTimeScale()
	if err != nil {
		return err
	}
	detectors = scaleDetectors(detectors, timeScale)
	*verificationTimeout = scaledDuration(*verificationTimeout, timeScale)
	*closureStability = scaledDuration(*closureStability, timeScale)
	*reportGrace = scaledDuration(*reportGrace, timeScale)
	if options.NativeBroker != nil && *forceReflection {
		options.NativeBroker.ForceReflection = true
	}
	if *maxResponseLoops < 1 || *diagnosisDeadline <= 0 || *driftBatchWindow <= 0 {
		return errors.New("response loops, diagnosis deadline and drift batch window must be positive")
	}

	if *exitAfterClosure && *restartAfterClosure {
		return fmt.Errorf("--exit-after-closure and --restart-after-closure are mutually exclusive")
	}
	if *controlNamespace == "" {
		*controlNamespace = *namespace
	}
	if !namespacePattern.MatchString(*controlNamespace) {
		return fmt.Errorf("invalid control namespace %q", *controlNamespace)
	}
	if *namespace == "" || *appRoot == "" {
		return fmt.Errorf("namespace and app-root are required")
	}
	if *dispatcherMode != "job" {
		return fmt.Errorf("unsupported dispatcher mode %q", *dispatcherMode)
	}
	jobDispatch := options.Dispatcher == nil
	if jobDispatch {
		if err := toolchain.ValidateImage(*toolchainImage); err != nil {
			return err
		}
	}
	if jobDispatch && (*repositoryPVC == "" || *credentialsSecret == "") {
		return fmt.Errorf("repository-pvc and responder-credentials-secret are required in job mode")
	}
	if jobDispatch && *agentRegistrationDirectory == "" && *responderImage == "" {
		return fmt.Errorf("responder-image is required for the built-in agent registrations")
	}
	if jobDispatch && options.NativeBroker == nil {
		return errors.New("job controller requires the Go incident broker")
	}
	if *repairPolicy != "commit" && *repairPolicy != "recorded-actions" {
		return fmt.Errorf("unsupported repair policy %q", *repairPolicy)
	}
	var registeredWorkers *worker_registry.Registry
	if jobDispatch {
		var registryErr error
		if *agentRegistrationDirectory == "" {
			registeredWorkers, registryErr = worker_registry.LoadBuiltIn("", *responderImage)
		} else {
			registeredWorkers, registryErr = worker_registry.LoadDirectory(*agentRegistrationDirectory)
		}
		if registryErr != nil {
			return fmt.Errorf("load agent registry: %w", registryErr)
		}
		if *responderAgentID == "" {
			if *responderAgentID, registryErr = registeredWorkers.AgentFor(&contractsv1alpha1.IncidentRequest{}); registryErr != nil {
				return fmt.Errorf("select responder agent: %w", registryErr)
			}
		}
		incidentTask, registryErr := registeredWorkers.TaskType(*responderAgentID, &contractsv1alpha1.IncidentRequest{})
		if registryErr != nil {
			return fmt.Errorf("select responder agent: %w", registryErr)
		}
		registration, registryErr := registeredWorkers.Resolve(*responderAgentID, incidentTask)
		if registryErr != nil {
			return fmt.Errorf("select responder agent: %w", registryErr)
		}
		if registration.GetKubernetesJob() == nil {
			return fmt.Errorf("responder agent %q must use Kubernetes Job launch", *responderAgentID)
		}
		if *responderImage == "" {
			*responderImage = registration.GetKubernetesJob().GetImage()
		}
	}
	resolvedRoot, err := filepath.Abs(*appRoot)
	if err != nil {
		return fmt.Errorf("resolve app root: %w", err)
	}
	if *application == "" {
		*application = filepath.Base(resolvedRoot)
	}
	resolvedSourceCommit, resolvedDeployedCommit, err := resolveRepositoryCommits(
		resolvedRoot,
		*sourceCommit,
		*deployedCommit,
	)
	if err != nil {
		return err
	}
	shellReader := options.ShellReportReader
	if selectedCodePath.Selection.DetectorSource == "shell" {
		if shellReader == nil {
			shellReader = shellpath.HTTPReader{URL: shellpath.RunnerURL(*controlNamespace)}
		}
		codePathRegistry.detectors["shell"] = ShellDetectorSource{Reader: shellReader}
		selectedCodePath, err = codePathRegistry.Resolve(selectedPathConfig)
		if err != nil {
			return err
		}
	}
	detectors, err = selectedCodePath.Detectors.Detectors(ctx, DetectorSourceRequest{RepositoryRoot: resolvedRoot, Namespace: *namespace, CompiledDetectors: detectors})
	if err != nil {
		return err
	}
	bootstrapProvider := &core.KubernetesSnapshotProvider{Namespace: *namespace, Client: options.Client}
	leaseClient := options.Client
	if options.Client == nil {
		bootstrapProvider, err = core.NewKubernetesSnapshotProvider(*namespace)
		if err != nil {
			return fmt.Errorf("create Kubernetes snapshot provider: %w", err)
		}
		leaseClient, err = core.NewKubernetesClient()
		if err != nil {
			return fmt.Errorf("create dedicated leader-election client: %w", err)
		}
	}
	planExecConfig, genericCapture, err := initializePlanTransports(options, clientInjected, *namespace)
	if err != nil {
		return err
	}
	stateStartTimeout := options.StateBaselineStartTimeout
	if stateStartTimeout <= 0 {
		stateStartTimeout = defaultStateBaselineStartTimeout
	}
	elector := NewLeaseElector(leaseClient, *controlNamespace, *leaseName, *identity, *leaseDuration)
	for {
		leader, acquireErr := elector.TryAcquireOrRenew(ctx)
		if acquireErr != nil {
			return fmt.Errorf("acquire controller leadership: %w", acquireErr)
		}
		if leader {
			break
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(*leaseDuration / 3):
		}
	}
	defer func() {
		releaseCtx, cancelRelease := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelRelease()
		if err := elector.Release(releaseCtx); err != nil {
			fmt.Fprintln(stderr, err)
		}
	}()
	renewalCtx, cancelRenewal := context.WithCancel(ctx)
	defer cancelRenewal()
	renewalInterval := *leaseDuration / 3
	if renewalInterval <= 0 {
		renewalInterval = time.Second
	}
	if renewalInterval > 5*time.Second {
		renewalInterval = 5 * time.Second
	}
	renewalErrors := maintainLeadership(renewalCtx, elector, renewalInterval)
	stateConfigMapName := ""
	if *controlNamespace == *namespace {
		stateConfigMapName = controllerStateConfigMap
	}
	ceiling, err := observation.CeilingFromEnvironment()
	if err != nil {
		return err
	}
	scope, scopeCommit, err := observation.Accepted(ctx, resolvedRoot)
	if err != nil {
		return err
	}
	if err = scope.Within(ceiling); err != nil {
		return err
	}
	kubernetesCache, err := NewKubernetesCache(KubernetesCacheConfig{
		AcceptedScope: func(ctx context.Context) (sdk.ObservationScope, string, error) {
			return observation.Accepted(ctx, resolvedRoot)
		}, GenericCapture: genericCapture, Scope: scope, Ceiling: ceiling, Generation: scopeCommit, Namespace: *namespace, Client: bootstrapProvider.Client, StateConfigMapName: stateConfigMapName, TrackDrift: true,
	}, detectors)
	if err != nil {
		return fmt.Errorf("create Kubernetes informer cache: %w", err)
	}
	trafficWorkloads := TrafficWorkloadNames(detectors)
	var proberAPI ProberAPI
	var proberAddress func(context.Context, bool) (string, error)
	// Responders outlive prober pod IPs, so they get a stable address.
	var responderProberAddress func(context.Context, bool) (string, error)
	switch {
	case *proberURL != "":
		proberAddress = StaticProberURL(*proberURL)
		proberAPI = HTTPProberClient{BaseURL: proberAddress}
		responderProberAddress = proberAddress
	case *proberBinary != "":
		image := *proberImage
		if image == "" {
			image = *responderImage
		}
		pod := &ProberPod{
			Client: bootstrapProvider.Client, Namespace: *controlNamespace, AppNamespace: *namespace, Image: image,
			RepositoryPVC: *repositoryPVC, RepositoryMountPath: *repositoryMountPath,
			RepositoryPVCSubPath: *repositoryPVCSubPath, Binary: *proberBinary,
			DisableSyntheticTraffic: !*syntheticTraffic,
		}
		proberAddress = pod.Address
		responderProberAddress = pod.ServiceAddress
		proberAPI = HTTPProberClient{BaseURL: proberAddress}
	}
	var trafficObserver *TrafficObserver
	if *syntheticTraffic {
		trafficObserver = NewTrafficObserver(proberAPI, trafficWorkloads, func() { kubernetesCache.Notify(traffic.Watch) }, 0)
	}
	// Service reachability is a routing probe, not a synthetic workload, so
	// it runs whenever a prober exists and a detector consumes it.
	var reachabilityObserver *ServiceReachabilityObserver
	if proberAddress != nil && ConsumesServiceReachability(detectors) {
		identity := servicehealth.ProberIdentity{}
		if *proberURL == "" {
			identity = servicehealth.ProberIdentity{Namespace: *controlNamespace, Labels: proberPodLabels()}
		}
		reachabilityObserver = NewServiceReachabilityObserver(ServiceReachabilityConfig{
			Prober: HTTPProberClient{BaseURL: proberAddress}, Snapshots: kubernetesCache, Identity: identity,
			Notify: func() { kubernetesCache.Notify(servicehealth.ReachabilityWatch) },
		})
		defer reachabilityObserver.Stop()
	}
	var snapshotProvider SnapshotProvider = kubernetesCache
	if trafficObserver != nil || reachabilityObserver != nil {
		baseline, sourceInputs, objective, err := loadReachabilityBaseline(ctx, resolvedRoot, resolvedSourceCommit)
		if err != nil {
			return err
		}
		snapshotProvider = TrafficSnapshotProvider{Base: kubernetesCache, Observer: trafficObserver, Reachability: reachabilityObserver, Baseline: baseline, SourceInputs: sourceInputs, ObjectiveDigest: objective}
	}
	if trafficObserver != nil {
		defer trafficObserver.Stop()
	}
	snapshotProvider, metricsObserver, err := withMetrics(snapshotProvider, detectors, *metricsURL, *metricsTokenFile, false)
	if err != nil {
		return err
	}
	jobEnvironment, err := parseResponderEnvironment(responderEnvironment)
	if err != nil {
		return err
	}
	if jobEnvironment, err = withAgentEnvironment(jobEnvironment, launchConfig); err != nil {
		return err
	}
	if codePathCustomized(selectedPathConfig) {
		jobEnvironment["SDO_CODE_PATH"] = selectedCodePath.Selection.Name
		jobEnvironment["SDO_AGENT_GUIDANCE"] = selectedCodePath.Selection.AgentGuidance
		jobEnvironment["SDO_MEMORY_VALIDATION"] = "go"
		if selectedCodePath.Selection.MemoryValidation != "go" {
			jobEnvironment["SDO_MEMORY_VALIDATION"] = "shell"
		}
	}
	if options.NativeBroker != nil {
		options.NativeBroker.CodePath = selectedPathConfig
		options.NativeBroker.ShellReader = shellReader
	}
	jobEnvironment[ControllerStateEnvironment] = controllerStateLocation(*controlNamespace)
	if launchConfig != nil {
		if options.NativeBroker != nil {
			options.NativeBroker.Programs = options.Programs
		}
		if err := attachNativeReflector(options.NativeBroker, launchConfig, bootstrapProvider.Client, registeredWorkers, *responseTimeout); err != nil {
			return err
		}
	}
	var stateTracker *StateTracker
	if *stateBaseline {
		metadataClient := options.MetadataClient
		if metadataClient == nil && !clientInjected {
			metadataClient, err = core.NewKubernetesMetadataClient()
			if err != nil {
				return fmt.Errorf("create Secret metadata client: %w", err)
			}
		}
		stateTracker = NewStateTracker(StateTrackerConfig{Scope: scope, Ceiling: ceiling, Client: bootstrapProvider.Client, MetadataClient: metadataClient, Namespace: *namespace, Settle: scaledDuration(DefaultStateSettle, timeScale)})
		kubernetesCache.scopedTracker = stateTracker
		defer stateTracker.Stop()
	}
	planSource := GitPlanSource{Repository: resolvedRoot, ManifestPaths: deployedManifestPaths, DeployedCommit: resolvedDeployedCommit}
	var primitiveProber PrimitiveProber
	if proberAddress != nil {
		primitiveProber = HTTPProberClient{BaseURL: proberAddress}
	}
	removalClient, err := newUnexplainedRemovalClient(planExecConfig, genericCapture, *controlNamespace)
	if err != nil {
		return err
	}
	protectedNamespace := ""
	if *controlNamespace != *namespace {
		protectedNamespace = *controlNamespace
	}
	scriptJobImage := *scriptImage
	if scriptJobImage == "" {
		scriptJobImage = *toolchainImage
	}
	if scriptJobImage == "" {
		scriptJobImage = *responderImage
	}
	var controller *Controller
	dispatcher := options.Dispatcher
	if dispatcher == nil {
		if !clientInjected {
			if err := toolchain.Seed(ctx, *repositoryMountPath, *toolchainImage); err != nil {
				return fmt.Errorf("prepare responder toolchain before dispatch: %w", err)
			}
		}
		var catalog memory.CatalogReader
		if options.NativeBroker != nil {
			catalog = memory.OpenCatalogReader(resolvedRoot)
			if selectedCodePath.Selection.DetectorSource == "shell" {
				catalog = memory.OpenShellCatalogReader(resolvedRoot)
			}
		}
		apiProxyImage := *responderImage
		if launchConfig != nil {
			apiProxyImage = launchConfig.GetValidatorImage()
		}
		dispatcher = KubernetesJobDispatcher{
			AcceptedRepository: resolvedRoot,
			CoverageProvider:   selectedCodePath.Coverage,
			APIProxyImage:      apiProxyImage,
			ToolchainImage:     *toolchainImage,
			Client:             bootstrapProvider.Client, Namespace: *controlNamespace, Registry: registeredWorkers, AgentID: *responderAgentID,
			ServiceAccount: "sdo-responder", RepositoryPVC: *repositoryPVC,
			RepositoryMountPath: *repositoryMountPath, RepositoryPVCSubPath: *repositoryPVCSubPath,
			CredentialsSecret: *credentialsSecret, PollInterval: time.Second,
			Environment: jobEnvironment, DispatchEnvironment: ProberEnvironment(responderProberAddress),
			Catalog: catalog, Outcomes: memory.OpenReader(resolvedRoot), RequireMemorySnapshot: options.NativeBroker != nil,
			LoopFactory: func(request IncidentRequest) *ResponderLoop {
				loop := NewResponderLoop(request, options.Programs, snapshotProvider, detectors, PlanExecutor{
					Client: GenericPlanClient{TypedPlanClient: TypedPlanClient{Client: bootstrapProvider.Client, ExecConfig: planExecConfig, ProtectedNamespace: protectedNamespace, ScriptImage: scriptJobImage}, Objects: genericCapture}, Source: planSource,

					Rollouts: KubernetesPlanRollouts{Client: bootstrapProvider.Client}, Verifier: PlanHealthVerifier{Detectors: detectors, Provider: snapshotProvider, Timeout: *verificationTimeout, PollInterval: scaledDuration(time.Second, timeScale)}, Observer: kubernetesCache,
				}, *maxResponseLoops)
				loop.WorkingProgramRoot = request.RepositoryWorktree
				if menuEnabled != "0" {
					loop.MenuConfig = productionResponderMenu()
					loop.MenuConfig.Unexplained = func(ctx context.Context) (UnexplainedRemovalAssessment, error) {
						if controller == nil {
							return UnexplainedRemovalAssessment{}, fmt.Errorf("controller is unavailable")
						}
						assessment, err := controller.unexplainedRemovalAssessment(request.IncidentID, *controlNamespace)
						if err != nil {
							return assessment, err
						}
						if removalClient == nil {
							for _, removal := range assessment.Removals {
								assessment.Blockers = append(assessment.Blockers, UnexplainedRemovalBlocker{FindingFingerprint: removal.FindingFingerprint, Ref: removal.Ref, Reason: RemovalUnavailable})
							}
							assessment.Removals = nil
						}
						return assessment, nil
					}
					loop.MenuConfig.RemoveUnexplained = func(ctx context.Context, selected UnexplainedRemoval, client PlanLifecycleClient, ref sdk.ObjectRef, uid types.UID, rv string) error {
						if removalClient == nil {
							return fmt.Errorf("removal-only client is unavailable")
						}
						return controller.removeUnexplained(ctx, request.IncidentID, *controlNamespace, selected, removalClient, ref, uid, rv)
					}
				}
				loop.VerifyClosure = func(ctx context.Context) ToolResponse {
					if controller == nil {
						return ToolResponse{Command: "verify", Error: "controller verification is unavailable"}
					}
					return controller.cachedResponderVerification(ctx, request.IncidentID, 0)
				}
				loop.WaitClosure = func(ctx context.Context, wait time.Duration) ToolResponse {
					return controller.cachedResponderVerification(ctx, request.IncidentID, wait)
				}
				loop.IncidentStatus = func() *apihistory.IncidentVerified { return controller.incidentVerificationWarning(request.IncidentID) }
				if options.NativeBroker != nil {
					gate, err := memory.IncidentMutationGate(*repositoryMountPath, request.IncidentID)
					if err != nil {
						loop.gateError = err
					} else {
						loop.MutationGate = gate
					}
				}
				loop.CachedFindings = func() ToolResponse { return controller.cachedFindingView(request.IncidentID) }
				loop.ExternalReceipts = func() ([]RepairActionReceipt, bool, string, error) {
					rows, pending, revision, err := memory.IncidentActionReceipts(*repositoryMountPath, request.IncidentID)
					if err != nil {
						return nil, pending, revision, err
					}
					result := incidentResultState(&contractsv1alpha1.IncidentResult{RepairActions: rows})
					return result.RepairActions, pending, revision, nil
				}
				loop.VerifyResult = func(ctx context.Context, result IncidentResult) ToolResponse {
					if controller == nil {
						return ToolResponse{Command: "verify", Error: "controller verification is unavailable"}
					}
					return controller.responderResultVerification(ctx, request.IncidentID, &result)
				}
				loop.CurrentEvidence = func() *IncidentRequest {
					if controller == nil {
						return nil
					}
					controller.mu.Lock()
					defer controller.mu.Unlock()
					return cloneIncidentRequest(controller.currentIncidentRequest)
				}
				loop.DiagnosisDeadline = *diagnosisDeadline
				if options.NativeBroker != nil {
					loop.CorpusSessionRoot = options.NativeBroker.SessionRoot
				}
				loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView {
					return LiveReadView{DetectionContext: snapshot, Context: ctx, Client: bootstrapProvider.Client, DatabaseExecConfig: planExecConfig, ProtectedNamespace: protectedNamespace, Prober: primitiveProber,
						Source: planSource, SourceCommit: request.SourceCommit, Baseline: request.StateChanges, BaselineSource: stateTracker}
				}
				controller.mu.Lock()
				controller.responderLoop = loop
				controller.mu.Unlock()
				return loop
			},
		}
	}
	if !clientInjected && jobDispatch && *responderStandby && standbyEnabled(jobEnvironment) {
		d := dispatcher.(KubernetesJobDispatcher)
		standbyEnvironment := make(map[string]string, len(jobEnvironment)+5)
		for name, value := range jobEnvironment {
			standbyEnvironment[name] = value
		}
		standbyEnvironment["CODEX_HOME"] = "/sdo/session/codex"
		standbyEnvironment["CLAUDE_CONFIG_DIR"] = "/sdo/session/claude"
		standbyEnvironment["SDO_SESSION_SCRATCH"] = "/sdo/session/scratch"
		standbyEnvironment["SDO_TOOLS_DIR"] = "/sdo/session/tools"
		pool := worker_registry.NewStandbyPool(worker_registry.KubernetesExecutor{APIProxyImage: d.APIProxyImage, ToolchainImage: d.ToolchainImage, Client: d.Client, Namespace: d.Namespace, Registry: d.Registry, ServiceAccount: d.ServiceAccount, RepositoryPVC: d.RepositoryPVC, RepositoryMountPath: d.RepositoryMountPath, RepositoryPVCSubPath: d.RepositoryPVCSubPath, CredentialsSecret: d.CredentialsSecret, Environment: standbyEnvironment, CapturedCorpusReadOnly: true}, d.AgentID, *application)
		pool.ResponseBudget = *responseTimeout
		standbyCtx, stopStandby := context.WithCancel(renewalCtx)
		if err := pool.Start(standbyCtx); err != nil {
			stopStandby()
			slog.Warn("responder_standby_unavailable", "error", err)
		} else {
			defer func() { stopStandby(); pool.Wait() }()
			d.Standby = pool
			dispatcher = d
		}
	}

	// Detectors and plan verification read through the same capped runtime view.
	snapshotProvider = ReadSnapshotProvider{Base: snapshotProvider, View: func(snapshot sdk.DetectionContext) program.ReadView {
		return LiveReadView{DetectionContext: snapshot, Context: ctx, Client: bootstrapProvider.Client, DatabaseExecConfig: planExecConfig, ProtectedNamespace: protectedNamespace, Prober: primitiveProber, Source: planSource, SourceCommit: resolvedSourceCommit, SourceRevision: func() string {
			if controller != nil {
				return controller.CurrentAcceptedCommit()
			}
			return resolvedSourceCommit
		}, BaselineSource: stateTracker}
	}}
	start := time.Now().UTC()
	corpusSessionRoot := ""
	if options.NativeBroker != nil {
		corpusSessionRoot = options.NativeBroker.SessionRoot
	}
	controller, err = NewController(ControllerConfig{
		CodePath: selectedPathConfig, CodePathRegistry: codePathRegistry,
		Application: *application, Namespace: *namespace,
		SourceCommit: resolvedSourceCommit, DeployedCommit: resolvedDeployedCommit,
		RepositoryWorktree: resolvedRoot, ResponseTimeout: *responseTimeout,
		CorpusSessionRoot:      corpusSessionRoot,
		VerificationTimeout:    *verificationTimeout,
		ClosureStabilityWindow: *closureStability,
		ResponderReportGrace:   *reportGrace,
		RepairPolicy:           *repairPolicy,
		FiringThreshold:        2, ClearThreshold: 2, BatchDebounce: scaledDuration(500*time.Millisecond, timeScale),
		ConfirmationInterval: scaledDuration(time.Second, timeScale),
		// The submit gate confirms a clearing finding on 3 fresh evaluations
		// over at least 2 s instead of waiting for the detector's interval;
		// closure keeps the health judge's clear persistence.
		GateConfirmation: GateConfirmationPolicy{Evaluations: 3, Window: scaledDuration(2*time.Second, timeScale), Interval: scaledDuration(time.Second, timeScale)},
	}, detectors, snapshotProvider, dispatcher, start)
	if err != nil {
		return err
	}
	// Registered before the run context's cancel, so it runs after it: the
	// caller may remove the repository mount once this function returns.
	defer controller.Wait()
	controller.ConfigureDrift(DriftControllerConfig{Inventory: true, Events: kubernetesCache, Source: planSource, Client: bootstrapProvider.Client, BatchWindow: *driftBatchWindow,
		View: func(snapshot sdk.DetectionContext) program.ReadView {
			if cycle, ok := snapshot.(*CycleReadView); ok {
				return cycle
			}
			return NewCycleReadView(LiveReadView{DetectionContext: snapshot, Context: ctx, Client: bootstrapProvider.Client, DatabaseExecConfig: planExecConfig, ProtectedNamespace: protectedNamespace, Prober: primitiveProber,
				Source: planSource, SourceCommit: controller.CurrentAcceptedCommit(), BaselineSource: stateTracker})
		},
	})
	if options.NativeBroker != nil {
		if err := controller.SetIncidentBroker(*options.NativeBroker); err != nil {
			return err
		}
	} else if jobDispatch {
		return errors.New("job controller requires the Go incident broker")
	}
	if *stateBaseline {
		controller.Baseline = stateTracker
	}
	if *cleanResponderHelpers {
		controller.Helpers = KubernetesHelperCleaner{
			Client: bootstrapProvider.Client, Namespaces: []string{*namespace, *controlNamespace},
		}
	}
	if launchConfig != nil && selectedCodePath.Selection.DetectorSource != "shell" {
		controller.CheckHealth = controllerHealthGuard(controller, resolvedRoot, launchConfig.GetControllerRevision())
	}
	controller.CanAct = elector.IsLeader
	controller.GuardAction = elector.GuardContext
	stateStore := lifecycleio.NewConfigMapStateStore(bootstrapProvider.Client, *controlNamespace, controllerStateConfigMap, filepath.Join(*repositoryMountPath, ".sdo-runtime", "controller-evidence"))
	if err := controller.AttachStateStore(ctx, stateStore); err != nil {
		return fmt.Errorf("restore controller state: %w", err)
	}
	acknowledgement, err := onboarding.Load(ctx, bootstrapProvider.Client, *controlNamespace, *namespace)
	if err != nil {
		return err
	}
	if err := controller.SetNamespaceOnboarding(acknowledgement); err != nil {
		return err
	}
	if err := controller.AttachPendingOnboarding(ctx, bootstrapProvider.Client, *controlNamespace); err != nil {
		return fmt.Errorf("load namespace onboarding evidence: %w", err)
	}
	snapshotProvider = controller.provider
	if launchConfig != nil {
		controller.discardRestoredHealthObservations(start)
	}
	if err := attachFiringTelemetry(controller, *telemetryPath, jobDispatch, *repositoryMountPath); err != nil {
		fmt.Fprintln(stderr, err)
	}
	if options.NativeBroker != nil && jobDispatch {
		controller.SetFiringSink(lateFindingSink{Downstream: controller.firingSink, Store: latefindings.Store{Client: bootstrapProvider.Client, Namespace: *controlNamespace}})
	}
	if err := attachLifecycleTelemetry(controller, *lifecyclePath, jobDispatch, *repositoryMountPath); err != nil {
		fmt.Fprintln(stderr, err)
	}
	if *exitAfterClosure && controller.LastAcknowledgedIncidentID() != "" {
		return nil
	}
	if err := ClosureFailureError(controller); err != nil && *exitAfterClosure {
		return err
	}
	iteration := 0
	encoder := json.NewEncoder(stdout)
	controller.OnError = func(err error) { fmt.Fprintln(stderr, err) }
	controller.OnClosureFailed = func(failure ClosureFailure) {
		document, err := wire.Canonical(closureFailureContract(&failure))
		if err == nil {
			err = encoder.Encode(map[string]any{"controller_closure_failed": json.RawMessage(document)})
		}
		if err != nil {
			fmt.Fprintln(stderr, err)
		}
	}
	controller.OnEvaluation = func(findings []sdk.Finding) {
		if err := encoder.Encode(map[string]any{
			"controller_iteration": iteration,
			"evaluated_detectors":  controller.evaluationDetectorIDs(),
			"returncode":           0,
			"findings":             findings,
		}); err != nil {
			fmt.Fprintln(stderr, err)
		}
		iteration++
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if *duration > 0 {
		cancel()
		runCtx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}
	if options.NativeBroker != nil {
		if err := options.NativeBroker.PruneIncidentSessions(time.Now().UTC()); err != nil {
			fmt.Fprintf(stderr, "prune retained incident sessions: %v\n", err)
		}
		go func() {
			ticker := time.NewTicker(24 * time.Hour)
			defer ticker.Stop()
			for {
				select {
				case <-runCtx.Done():
					return
				case now := <-ticker.C:
					if err := options.NativeBroker.PruneIncidentSessions(now.UTC()); err != nil {
						fmt.Fprintf(stderr, "prune retained incident sessions: %v\n", err)
					}
				}
			}
		}()
	}
	maintenance := MaintenanceWatcher{
		Client: bootstrapProvider.Client, Namespace: *controlNamespace, Name: *maintenanceConfigMap,
	}
	desired, err := maintenance.Read(runCtx)
	if err != nil {
		return err
	}
	// applied is the mode the controller currently operates under. It lags
	// desired only while a resume is waiting for a fresh application cache.
	applied := desired
	cacheStarted := false
	if err := encoder.Encode(desired.Record()); err != nil {
		fmt.Fprintln(stderr, err)
	}
	if metricsObserver != nil {
		if err := encoder.Encode(map[string]any{"metrics": metricsObserver.Summary()}); err != nil {
			fmt.Fprintln(stderr, err)
		}
	}
	// startTraffic makes sure the prober runs, clears its observations, and
	// waits briefly for a first sample of every scenario, so the next
	// evaluation, which may be the all-clear that precedes a fault, judges
	// observed traffic.
	startTraffic := func() {
		if trafficObserver == nil {
			// Routing capture has a short sampling budget, so create and await
			// the isolated pod before detection, even without traffic consumers.
			if proberAddress != nil {
				readyCtx, cancel := context.WithTimeout(runCtx, defaultProberReadyAfter)
				_, err := proberAddress(readyCtx, false)
				cancel()
				if err != nil {
					fmt.Fprintf(stderr, "routing prober startup: %v\n", err)
				}
			}
			if len(trafficWorkloads) > 0 {
				if err := encoder.Encode(map[string]any{
					"synthetic_traffic": "disabled", "synthetic_traffic_workloads": trafficWorkloads,
				}); err != nil {
					fmt.Fprintln(stderr, err)
				}
			}
			return
		}
		started := time.Now()
		resetErr := trafficObserver.Reset(runCtx)
		trafficObserver.Start(runCtx)
		warm := trafficObserver.WaitWarm(runCtx, *syntheticWarmup)
		record := map[string]any{
			"synthetic_traffic": trafficObserver.Summary(), "synthetic_traffic_warm": warm,
			"synthetic_traffic_startup_ms": time.Since(started).Milliseconds(),
		}
		if resetErr != nil {
			record["synthetic_traffic_error"] = resetErr.Error()
		}
		if err := encoder.Encode(record); err != nil {
			fmt.Fprintln(stderr, err)
		}
	}
	stopTraffic := func() {
		if trafficObserver != nil {
			trafficObserver.Stop()
		}
		if reachabilityObserver != nil {
			reachabilityObserver.Stop()
		}
	}
	// startReachability begins fresh dial rounds; a maintenance window may
	// have redeployed the application, so earlier samples are dropped.
	startReachability := func() {
		if reachabilityObserver == nil {
			return
		}
		reachabilityObserver.Reset()
		reachabilityObserver.Start(runCtx)
		if err := encoder.Encode(map[string]any{"service_reachability": "started"}); err != nil {
			fmt.Fprintln(stderr, err)
		}
	}
	// startState begins observing the application's configuration, in
	// parallel with the traffic warm-up, so the first quiet evaluation
	// becomes its healthy baseline. The returned wait joins it and logs the
	// outcome. It never fails operation: without it incidents simply carry
	// no state changes.
	startState := func() (wait func()) {
		if stateTracker == nil {
			return func() {}
		}
		started := time.Now()
		done := make(chan error, 1)
		go func() {
			startCtx, cancelStart := context.WithTimeout(runCtx, stateStartTimeout)
			defer cancelStart()
			done <- stateTracker.Start(startCtx)
		}()
		return func() {
			err := <-done
			record := map[string]any{"state_baseline_startup_ms": time.Since(started).Milliseconds()}
			if err != nil {
				record["state_baseline_error"] = err.Error()
			} else if unobserved := stateTracker.UnobservedKinds(); len(unobserved) > 0 {
				record["state_baseline_unobserved_kinds"] = unobserved
			}
			if err := encoder.Encode(record); err != nil {
				fmt.Fprintln(stderr, err)
			}
		}
	}
	stopState := func() {
		if stateTracker != nil {
			// A redeploy replaces the application's configuration baseline.
			stateTracker.Stop()
			stateTracker.Reset()
		}
	}
	if options.NativeBroker != nil {
		reconciler := RenderedLockReconciler{Source: planSource, Client: bootstrapProvider.Client, Broker: options.NativeBroker.Repository}
		controller.ReconcileRendered = func(ctx context.Context, revision string) (string, error) {
			return reconciler.Reconcile(ctx, revision, *namespace)
		}
	}
	persistControllerState := func() {
		if err := controller.PersistState(ctx); err != nil {
			fmt.Fprintf(stderr, "persist controller state: %v\n", err)
		}
	}
	if err := controller.prepareHealthEffects(runCtx, time.Now().UTC()); err != nil {
		return err
	}
	if !desired.Paused {
		kubernetesCache.Start(runCtx)
		cacheStarted = true
		if err := kubernetesCache.WaitForSync(runCtx); err != nil {
			return fmt.Errorf("sync Kubernetes informer cache: %w", err)
		}
		waitState := startState()
		startTraffic()
		startReachability()
		waitState()
		if launchConfig != nil && options.NativeBroker != nil {
			if err := acceptStartupReachabilityBaseline(runCtx, controller, resolvedRoot, options.NativeBroker.Repository, launchConfig.GetHealthyBaseline(), reachabilityObserver, snapshotProvider, detectors); err != nil {
				return err
			}
		}
		if err := controller.Step(runCtx, time.Now().UTC(), nil); err != nil {
			if runCtx.Err() != nil {
				return nil
			}
			return err
		}
	}
	if err := controller.retireExpiredIncident(runCtx, time.Now().UTC()); err != nil {
		return err
	}
	persistControllerState()
	// A controller launched during a maintenance pause must still complete its rollout handoff: the pause keeps
	// the closure acknowledgment waiting on it, so deferring it until resume restarts the controller in a loop.
	// The handoff verifies the build's provenance against the accepted tree, not an evaluation.
	if options.NativeBroker != nil {
		if err := completeNativeRolloutHandoffs(ctx, *options.NativeBroker, launchConfig, resolvedRoot); err != nil {
			return err
		}
	}
	if err := executeStartupEffects(runCtx, controller, desired.Paused); err != nil {
		if runCtx.Err() != nil {
			return nil
		}
		return err
	}
	maintenanceChanges := maintenance.Watch(runCtx, desired, *maintenancePollInterval, controller.OnError)
	// resume re-establishes observation of the application namespace with a
	// fresh informer generation and evaluates every detector once. The mode
	// record precedes that evaluation so observers can correlate it.
	resume := func() error {
		wasPaused := applied.Paused
		preserved := wasPaused && applied.PreserveBaseline
		syncCtx, cancelSync := context.WithTimeout(runCtx, *resumeSyncTimeout)
		defer cancelSync()
		var syncErr error
		if !wasPaused {
			// A generation-only acknowledgement does not interrupt observation.
			syncErr = kubernetesCache.WaitForSync(syncCtx)
		} else if preserved {
			// Observation continued while evaluation was paused. Keep watch
			// changes and the healthy baseline for the first resumed step.
			syncErr = kubernetesCache.WaitForSync(syncCtx)
		} else if cacheStarted {
			syncErr = kubernetesCache.Resync(syncCtx)
		} else {
			kubernetesCache.Start(runCtx)
			cacheStarted = true
			syncErr = kubernetesCache.WaitForSync(syncCtx)
		}
		if syncErr != nil {
			if runCtx.Err() != nil {
				return nil
			}
			fmt.Fprintf(stderr, "resume observation of namespace %s: %v\n", *namespace, syncErr)
			return nil
		}
		waitState := func() {}
		if wasPaused {
			if !preserved {
				waitState = startState()
			}
			startTraffic()
			startReachability()
			waitState()
		}
		if launchConfig != nil && options.NativeBroker != nil {
			if err := acceptStartupReachabilityBaseline(runCtx, controller, resolvedRoot, options.NativeBroker.Repository, launchConfig.GetHealthyBaseline(), reachabilityObserver, snapshotProvider, detectors); err != nil {
				return err
			}
		}
		kubernetesCache.TakeEvents()
		if wasPaused && !preserved {
			kubernetesCache.TakeObjectChanges()
		}
		applied = desired
		if err := encoder.Encode(applied.Record()); err != nil {
			fmt.Fprintln(stderr, err)
		}
		evaluate := controller.EvaluateAll
		if wasPaused {
			evaluate = controller.ResumeObservation
		}
		if err := evaluate(runCtx, time.Now().UTC()); err != nil {
			if runCtx.Err() != nil {
				return nil
			}
			return err
		}
		persistControllerState()
		if options.NativeBroker != nil {
			if err := completeNativeRolloutHandoffs(ctx, *options.NativeBroker, launchConfig, resolvedRoot); err != nil {
				return err
			}
		}
		if err := executePendingEffects(runCtx, controller); err != nil {
			if runCtx.Err() != nil {
				return nil
			}
			return err
		}
		return nil
	}
	for {
		if *exitAfterClosure && controller.LastAcknowledgedIncidentID() != "" {
			return nil
		}
		var delay time.Duration
		switch {
		case applied != desired:
			// A resume is waiting for the application namespace to become observable.
			delay = 2 * time.Second
		case applied.Paused:
			delay = time.Hour
			if wake := controller.PausedWake(); !wake.IsZero() {
				delay = time.Until(wake)
			}
		default:
			delay = time.Until(controller.NextWake())
		}
		if delay < 0 {
			delay = 0
		}
		timer := time.NewTimer(delay)
		select {
		case <-runCtx.Done():
			stopTimerForRuntimeEvent(runCtx, timer)
			return nil
		case change, ok := <-maintenanceChanges:
			if stopTimerForRuntimeEvent(runCtx, timer) || !ok {
				return nil
			}
			desired = change
			if desired.Paused {
				// A redeploying application would fail synthetic requests;
				// resume starts again from empty windows.
				stopTraffic()
				if !desired.PreserveBaseline {
					stopState()
					kubernetesCache.Stop()
					kubernetesCache.TakeEvents()
					kubernetesCache.TakeObjectChanges()
				}
				applied = desired
				if err := encoder.Encode(applied.Record()); err != nil {
					fmt.Fprintln(stderr, err)
				}
				continue
			}
			if err := resume(); err != nil {
				return err
			}
		case <-kubernetesCache.Notifications():
			if stopTimerForRuntimeEvent(runCtx, timer) {
				return nil
			}
			if applied.Paused {
				kubernetesCache.TakeEvents()
				if !applied.PreserveBaseline {
					kubernetesCache.TakeObjectChanges()
				}
				continue
			}
			if err := controller.StepEvents(runCtx, time.Now().UTC(), kubernetesCache.TakeEvents()); err != nil {
				if runCtx.Err() != nil {
					return nil
				}
				return err
			}
			persistControllerState()
			if err := executePendingEffects(runCtx, controller); err != nil {
				if runCtx.Err() != nil {
					return nil
				}
				return err
			}
		case completion := <-controller.results:
			if stopTimerForRuntimeEvent(runCtx, timer) {
				return nil
			}
			controller.handleDispatchCompletion(completion, time.Now().UTC())
			persistControllerState()
			if err := executeStartupEffects(runCtx, controller, applied.Paused); err != nil {
				if runCtx.Err() != nil {
					return nil
				}
				return err
			}
		case completion := <-controller.workspaceResults:
			if stopTimerForRuntimeEvent(runCtx, timer) {
				return nil
			}
			controller.handleWorkspaceCompletion(completion)
			persistControllerState()
			if err := executeStartupEffects(runCtx, controller, applied.Paused); err != nil {
				if runCtx.Err() != nil {
					return nil
				}
				return err
			}
		case completion := <-controller.closureResults:
			if stopTimerForRuntimeEvent(runCtx, timer) {
				return nil
			}
			controller.handleClosureCompletion(completion)
			persistControllerState()
			if err := ClosureFailureError(controller); err != nil && *exitAfterClosure {
				return err
			}
			if err := executeStartupEffects(runCtx, controller, applied.Paused); err != nil {
				if runCtx.Err() != nil {
					return nil
				}
				return err
			}
		case completion := <-controller.acknowledgmentResults:
			if stopTimerForRuntimeEvent(runCtx, timer) {
				return nil
			}
			controller.handleAcknowledgmentCompletion(completion)
			driveErr := controller.driveQueuedReflections(runCtx, controller.clock())
			persistControllerState()
			if driveErr != nil {
				return driveErr
			}
			if err := executeStartupEffects(runCtx, controller, applied.Paused); err != nil {
				if runCtx.Err() != nil {
					return nil
				}
				return err
			}
			if *exitAfterClosure && completion.err == nil && !controller.reflectionWorkOpen() {
				return nil
			}
			if *restartAfterClosure && options.NativeBroker == nil && completion.err == nil {
				if err := encoder.Encode(map[string]any{"controller_closure_restart": controller.LastAcknowledgedIncidentID()}); err != nil {
					fmt.Fprintln(stderr, err)
				}
				return nil
			}
		case completion := <-controller.reflectionResults:
			if stopTimerForRuntimeEvent(runCtx, timer) {
				return nil
			}
			controller.handleReflectionCompletion(completion)
			driveErr := controller.driveQueuedReflections(runCtx, controller.clock())
			persistControllerState()
			if driveErr != nil {
				return driveErr
			}
			if *exitAfterClosure && completion.err == nil && !controller.reflectionWorkOpen() {
				return nil
			}
		case renewErr := <-renewalErrors:
			if stopTimerForRuntimeEvent(runCtx, timer) {
				return nil
			}
			return renewErr
		case <-timer.C:
			if stopTimerForRuntimeEvent(runCtx, timer) {
				return nil
			}
			if applied != desired {
				if err := resume(); err != nil {
					return err
				}
				continue
			}
			if applied.Paused {
				if err := executePausedEffects(runCtx, controller); err != nil {
					if runCtx.Err() != nil {
						return nil
					}
					return err
				}
				continue
			}
			if err := controller.Step(runCtx, time.Now().UTC(), nil); err != nil {
				if runCtx.Err() != nil {
					return nil
				}
				return err
			}
			persistControllerState()
			if err := executePendingEffects(runCtx, controller); err != nil {
				if runCtx.Err() != nil {
					return nil
				}
				return err
			}
		}
	}
}

// attachFiringTelemetry connects the durable detector firing stream. Telemetry
// is observational, so a stream that cannot be opened is reported and skipped
// rather than stopping the controller.
func attachFiringTelemetry(controller *Controller, path string, defaultEnabled bool, mountPath string) error {
	if path == "off" || (path == "" && !defaultEnabled) {
		return nil
	}
	if path == "" {
		path = filepath.Join(mountPath, ".sdo-runtime", "telemetry", "detector-firings.jsonl")
	}
	sink, err := NewFileFiringSink(path, DefaultFiringStreamBytes)
	if err != nil {
		return fmt.Errorf("detector firing telemetry disabled: %w", err)
	}
	if !controller.RestoredFromState() {
		if err := sink.StartFresh(); err != nil {
			return fmt.Errorf("detector firing telemetry disabled: %w", err)
		}
	}
	controller.SetFiringSink(sink)
	return nil
}

// attachLifecycleTelemetry connects the durable incident lifecycle stream. Like
// the firing stream it is observational, so a stream that cannot be opened is
// reported and skipped rather than stopping the controller.
func attachLifecycleTelemetry(controller *Controller, path string, defaultEnabled bool, mountPath string) error {
	if path == "off" || (path == "" && !defaultEnabled) {
		return nil
	}
	if path == "" {
		path = filepath.Join(mountPath, ".sdo-runtime", "telemetry", "lifecycle-events.jsonl")
	}
	sink, err := NewFileLifecycleSink(path, DefaultLifecycleStreamBytes)
	if err != nil {
		return fmt.Errorf("incident lifecycle telemetry disabled: %w", err)
	}
	if !controller.RestoredFromState() {
		if err := sink.StartFresh(); err != nil {
			return fmt.Errorf("incident lifecycle telemetry disabled: %w", err)
		}
	}
	controller.SetLifecycleSink(sink)
	return nil
}

func stopTimerForRuntimeEvent(ctx context.Context, timer *time.Timer) bool {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	return ctx.Err() != nil
}

func maintainLeadership(
	ctx context.Context,
	elector *LeaseElector,
	interval time.Duration,
) <-chan error {
	errors := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				attemptTimeout := interval / 2
				if attemptTimeout <= 0 {
					attemptTimeout = time.Second
				}
				attemptCtx, cancelAttempt := context.WithTimeout(ctx, attemptTimeout)
				leader, err := elector.TryAcquireOrRenew(attemptCtx)
				cancelAttempt()
				if err != nil {
					if elector.IsLeader() {
						continue
					}
					errors <- fmt.Errorf("renew controller leadership before Lease expiry: %w", err)
					return
				}
				if !leader {
					errors <- fmt.Errorf("controller leadership lost")
					return
				}
			}
		}
	}()
	return errors
}

var environmentNamePattern = regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)

var namespacePattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)

func parseResponderEnvironment(entries []string) (map[string]string, error) {
	reserved := map[string]bool{
		"SDO_REQUEST_CONFIGMAP":    true,
		"SDO_RESULT_CONFIGMAP":     true,
		"SDO_NAMESPACE":            true,
		ControllerStateEnvironment: true,
	}
	result := make(map[string]string, len(entries))
	for _, entry := range entries {
		name, value, ok := strings.Cut(entry, "=")
		if !ok || !environmentNamePattern.MatchString(name) {
			return nil, fmt.Errorf("responder environment must use NAME=VALUE with a valid uppercase name: %q", entry)
		}
		if reserved[name] {
			return nil, fmt.Errorf("responder environment %q is managed by the controller", name)
		}
		result[name] = value
	}
	return result, nil
}

func resolveRepositoryCommits(repository string, sourceCommit string, deployedCommit string) (string, string, error) {
	resolvedSource := strings.TrimSpace(sourceCommit)
	if resolvedSource == "" {
		completed := exec.Command("git", "-C", repository, "rev-parse", "HEAD")
		output, err := completed.CombinedOutput()
		if err != nil {
			return "", "", fmt.Errorf("resolve source commit: %w: %s", err, strings.TrimSpace(string(output)))
		}
		resolvedSource = strings.TrimSpace(string(output))
	}
	resolvedDeployed := strings.TrimSpace(deployedCommit)
	if resolvedDeployed == "" {
		resolvedDeployed = resolvedSource
	}
	if resolvedSource == "" || resolvedDeployed == "" {
		return "", "", fmt.Errorf("source and deployed commits are required")
	}
	return resolvedSource, resolvedDeployed, nil
}

type evaluationDispatcher struct{}

func (evaluationDispatcher) Dispatch(context.Context, IncidentRequest) (IncidentResult, error) {
	return IncidentResult{}, fmt.Errorf("evaluate-once does not dispatch incidents")
}

// evaluationStatus is the bounded command verdict, not a persisted detector
// observation. Unavailable means this invocation lacks a required capability;
// pending means a detector can run but its observation window is incomplete.
type evaluationStatus string

const evaluationUnavailable evaluationStatus = "unavailable"

type unavailableEvaluation struct {
	DetectorID string `json:"detector_id"`
	Reason     string `json:"reason"`
}

type evaluationResult struct {
	Findings       []sdk.Finding           `json:"findings"`
	Status         evaluationStatus        `json:"status"`
	Unavailable    []unavailableEvaluation `json:"unavailable,omitempty"`
	DetectorStates []DetectorEvaluation    `json:"detector_states,omitempty"`
}

func evaluateOnce(
	ctx context.Context,
	detectors []sdk.Detector,
	args []string,
	stdout io.Writer,
	stderr io.Writer,
	provider SnapshotProvider,
) error {
	flags := flag.NewFlagSet("sdo-controller-evaluate-once", flag.ContinueOnError)
	flags.SetOutput(stderr)
	namespace := flags.String("namespace", "", "Kubernetes namespace to observe")
	application := flags.String("application", "", "application identity")
	var deployedManifestPaths repeatedFlag
	flags.Var(&deployedManifestPaths, "deployed-manifest-path", "accepted deployed manifest path; repeat for each selection")
	appRoot := flags.String("app-root", "", "accepted application repository root")
	proberURL := flags.String("prober-url", "", "live traffic prober URL for bounded health evaluation")
	sourceDriftAcknowledgement := flags.String("source-drift-acknowledgement", "", "installer onboarding acknowledgement ProtoJSON file for the read-only source inventory check")
	sourceDriftReport := flags.String("source-drift-report", "", "read-only accepted-source inventory check; write full JSON to this path and print a summary")
	acceptSeed := flags.Bool("accept-seed", false, "reject a seed unless the entire detector bundle evaluates without findings")
	metricsURL := flags.String("metrics-url", "", "Prometheus-compatible API URL for metric and alert health evaluation")
	metricsTokenFile := flags.String("metrics-token-file", "", "file holding a bearer token for --metrics-url")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *namespace == "" {
		return fmt.Errorf("namespace is required")
	}
	if *acceptSeed && *sourceDriftReport != "" {
		return errors.New("seed acceptance cannot use source-drift-report: inventory is not detector health evidence")
	}
	if *application == "" {
		*application = *namespace
	}
	if provider == nil {
		kubernetesProvider, err := core.NewKubernetesSnapshotProvider(*namespace)
		if err != nil {
			return fmt.Errorf("create Kubernetes snapshot provider: %w", err)
		}
		if *appRoot != "" {
			scope, revision, err := observation.Accepted(ctx, *appRoot)
			if err != nil {
				return err
			}
			ceiling, err := observation.CeilingFromEnvironment()
			if err != nil {
				return err
			}
			if err = scope.Within(ceiling); err != nil {
				return err
			}
			kubernetesProvider.Scope = scope
			kubernetesProvider.Ceiling = ceiling
			kubernetesProvider.Generation = revision
		}
		provider = kubernetesProvider
	}
	var readClient kubernetes.Interface
	if kube, ok := provider.(*core.KubernetesSnapshotProvider); ok {
		kube.PodAdmission = sdk.ConsumesPodAdmission(detectors)
		readClient = kube.Client
	}
	var readSource PlanSource
	readCommit := ""
	if *appRoot != "" {
		revision, _, err := resolveRepositoryCommits(*appRoot, "", "")
		if err != nil {
			return err
		}
		readCommit = revision
		readSource = GitPlanSource{Repository: *appRoot, ManifestPaths: deployedManifestPaths}
	}
	if *sourceDriftReport != "" {
		if *appRoot == "" || readClient == nil {
			return errors.New("source-drift-report requires app-root and a Kubernetes read client")
		}
		source := GitPlanSource{Repository: *appRoot, ManifestPaths: deployedManifestPaths}
		inventory, err := source.AcceptedInventory(ctx, readCommit, *namespace)
		if err != nil {
			return err
		}
		cache, err := NewKubernetesCache(KubernetesCacheConfig{Namespace: *namespace, Client: readClient, TrackDrift: true}, nil)
		if err != nil {
			return err
		}
		cache.Start(ctx)
		defer cache.Stop()
		if err := cache.WaitForSync(ctx); err != nil {
			return err
		}
		index := NewSourceInventory(*namespace, readCommit, inventory.Objects)
		index.EvidenceGaps = append(index.EvidenceGaps, inventory.EvidenceGaps...)
		if *sourceDriftAcknowledgement != "" {
			data, err := os.ReadFile(*sourceDriftAcknowledgement)
			if err != nil {
				return err
			}
			record, err := onboarding.Decode(data, *namespace)
			if err != nil {
				return err
			}
			if err := index.SetNamespaceOnboarding(record); err != nil {
				return err
			}
		}
		return WriteSourceInventoryReport(stdout, *sourceDriftReport, index, cache.SourceDriftObjects())
	}
	trafficProvider := TrafficSnapshotProvider{Base: provider}
	if *appRoot != "" {
		baseline, sourceInputs, objective, err := loadReachabilityBaseline(ctx, *appRoot, readCommit)
		if err != nil {
			return err
		}
		trafficProvider.Baseline, trafficProvider.SourceInputs, trafficProvider.ObjectiveDigest = baseline, sourceInputs, objective
	}
	if workloads := TrafficWorkloadNames(detectors); len(workloads) > 0 {
		if *proberURL == "" {
			return errors.New("traffic health evaluation requires a live prober URL")
		}
		observer := NewTrafficObserver(HTTPProberClient{BaseURL: StaticProberURL(*proberURL)}, workloads, nil, 0)
		if !observer.WaitWarm(ctx, 30*time.Second) {
			return errors.New("traffic prober did not produce a complete bounded health window")
		}
		trafficProvider.Observer = observer
	}
	var unavailable []unavailableEvaluation
	if ConsumesServiceReachability(detectors) {
		if *proberURL == "" {
			for _, detector := range detectors {
				if ConsumesServiceReachability([]sdk.Detector{detector}) {
					unavailable = append(unavailable, unavailableEvaluation{DetectorID: detector.Spec().ID, Reason: "service reachability requires --prober-url"})
				}
			}
			fmt.Fprintln(stderr, "service reachability unavailable: evaluate-once requires --prober-url")
		} else {
			// Gather the gate's history now: Failures rounds, a second apart,
			// judged by the same observer and detector as the controller.
			reachability := NewServiceReachabilityObserver(ServiceReachabilityConfig{
				Prober: HTTPProberClient{BaseURL: StaticProberURL(*proberURL)}, Snapshots: provider,
				Interval: evaluateOnceReachabilityInterval,
			})
			if err := reachability.Collect(ctx); err != nil {
				return fmt.Errorf("collect service reachability: %w", err)
			}
			trafficProvider.Reachability = reachability
		}
	}
	if trafficProvider.Observer != nil || trafficProvider.Reachability != nil {
		provider = trafficProvider
	}
	provider, _, err := withMetrics(provider, detectors, *metricsURL, *metricsTokenFile, true)
	if err != nil {
		return err
	}
	provider = ReadSnapshotProvider{Base: provider, View: func(snapshot sdk.DetectionContext) program.ReadView {
		if readClient == nil && readSource == nil && *proberURL == "" {
			return program.ReadFrom(snapshot)
		}
		var probe PrimitiveProber
		if *proberURL != "" {
			probe = HTTPProberClient{BaseURL: StaticProberURL(*proberURL)}
		}
		return LiveReadView{DetectionContext: snapshot, Context: ctx, Client: readClient, Prober: probe, Source: readSource, SourceCommit: readCommit}
	}}
	start := time.Now().UTC()
	controller, err := NewController(ControllerConfig{
		Application: *application, Namespace: *namespace,
		SourceCommit: "evaluation", DeployedCommit: "evaluation",
		RepositoryWorktree: ".", ResponseTimeout: time.Minute,
		FiringThreshold: 1, ClearThreshold: 1,
	}, detectors, provider, evaluationDispatcher{}, start)
	if err != nil {
		return err
	}
	findings := make([]sdk.Finding, 0)
	var detectorErrors []error
	controller.OnEvaluation = func(sample []sdk.Finding) { findings = append([]sdk.Finding{}, sample...) }
	controller.OnError = func(err error) { detectorErrors = append(detectorErrors, err) }
	if err := controller.Step(ctx, start, nil); err != nil {
		return err
	}
	if len(detectorErrors) > 0 {
		return fmt.Errorf("health detector evaluation failed: %w", detectorErrors[0])
	}
	status := evaluationStatus(DetectorEvaluationClear)
	var unconfirmed []DetectorEvaluation
	for _, evaluation := range controller.history {
		if evaluation.Status == DetectorEvaluationPending {
			status = evaluationStatus(DetectorEvaluationPending)
		}
		if evaluation.Status == DetectorEvaluationPending || evaluation.Reason != "" {
			unconfirmed = append(unconfirmed, evaluation)
		}
	}
	if len(unavailable) > 0 {
		status = evaluationUnavailable
	}
	for _, finding := range findings {
		if finding.Status == sdk.FindingActive {
			status = evaluationStatus(DetectorEvaluationFiring)
			break
		}
	}
	if err := json.NewEncoder(stdout).Encode(evaluationResult{Status: status, Findings: findings, Unavailable: unavailable, DetectorStates: unconfirmed}); err != nil {
		return err
	}
	if *acceptSeed && status != evaluationStatus(DetectorEvaluationClear) {
		return fmt.Errorf("seed acceptance rejected: status %s; %d detector finding(s); unavailable: %v", status, len(findings), unavailable)
	}
	return nil
}

func defaultIdentity() string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "sdo-controller"
	}
	return hostname + "-" + strconv.Itoa(os.Getpid())
}

// executeStartupEffects keeps restored responder work pending during a pause.
func executeStartupEffects(ctx context.Context, controller *Controller, paused bool) error {
	if paused {
		return executePausedEffects(ctx, controller)
	}
	return executePendingEffects(ctx, controller)
}

// executePausedEffects finishes already-started closure work, including report
// grace, without observing the application or starting responder work.
func executePausedEffects(ctx context.Context, controller *Controller) (err error) {
	if err := controller.stepPaused(ctx, controller.clock()); err != nil {
		return err
	}
	defer func() {
		if errors.Is(err, ErrEffectNotDurable) {
			err = nil
		}
	}()
	if closure, ok := controller.PendingClosureEffect(); ok {
		if err := controller.ExecuteClosureEffect(ctx, closure); err != nil {
			return fmt.Errorf("execute persisted closure effect: %w", err)
		}
		return nil
	}
	if acknowledgment, ok := controller.PendingClosureAcknowledgmentEffect(); ok {
		if err := controller.ExecuteClosureAcknowledgmentEffect(ctx, acknowledgment); err != nil {
			return fmt.Errorf("execute persisted closure acknowledgment effect: %w", err)
		}
		return nil
	}
	return nil
}

func executePendingEffects(ctx context.Context, controller *Controller) (err error) {
	// Stale health evidence owns retirement before the generic expiry fallback.
	if err := controller.prepareHealthEffects(ctx, time.Now().UTC()); err != nil {
		return err
	}
	// Completions pump effects without a detector Step. Recheck the deadline
	// here too, before retrying a prepared workspace or responder request.
	if err := controller.retireExpiredIncident(ctx, controller.clock()); err != nil {
		return err
	}
	// A failed state write leaves effects pending for the next successful save.
	defer func() {
		if errors.Is(err, ErrEffectNotDurable) {
			err = nil
		}
	}()
	if effect, ok := controller.PendingWorkspaceEffect(); ok {
		if err := controller.ExecuteWorkspaceEffect(ctx, effect); err != nil {
			return fmt.Errorf("execute persisted workspace effect: %w", err)
		}
		return nil
	}
	effect, ok := controller.PendingDispatchEffect()
	if ok {
		if err := controller.ExecuteDispatchEffect(ctx, effect); err != nil {
			return fmt.Errorf("execute persisted dispatch effect: %w", err)
		}
		return nil
	}
	if closure, ok := controller.PendingClosureEffect(); ok {
		if err := controller.ExecuteClosureEffect(ctx, closure); err != nil {
			return fmt.Errorf("execute persisted closure effect: %w", err)
		}
		return nil
	}
	if acknowledgment, ok := controller.PendingClosureAcknowledgmentEffect(); ok {
		if err := controller.ExecuteClosureAcknowledgmentEffect(ctx, acknowledgment); err != nil {
			return fmt.Errorf("execute persisted closure acknowledgment effect: %w", err)
		}
		return nil
	}
	return nil
}

// Existing responder-env configuration also provides a cold-only rollout switch.
func standbyEnabled(environment map[string]string) bool {
	return environment["SDO_RESPONDER_STANDBY"] != "0"
}

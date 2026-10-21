package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"k8s.io/client-go/kubernetes"

	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/core"
	"sdo.dev/operator/latefindings"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/traffic"
	"sdo.dev/operator/worker_registry"
)

type RuntimeOptions struct {
	Programs program.ProgramRegistry
	Args     []string
	Stdout   io.Writer
	Stderr   io.Writer
	Provider SnapshotProvider
	// Client, when set, replaces the kubeconfig clients of the long-running
	// controller (observation, state, dispatch, and leader election), so the
	// production wiring can run against a fake API server.
	Client kubernetes.Interface
	// StateBaselineStartTimeout bounds the state tracker's RBAC probes and
	// initial sync; zero means defaultStateBaselineStartTimeout.
	StateBaselineStartTimeout time.Duration
	// NativeBroker supplies the Go-owned closure path in tests and embedded runs.
	NativeBroker *NativeIncidentBroker
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
		os.Exit(1)
	}
}

func RunWithOptions(ctx context.Context, detectors []sdk.Detector, options RuntimeOptions) (runErr error) {
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
		return core.RunWithOptions(ctx, detectors, core.RuntimeOptions{Args: args[1:], Stdout: stdout, Stderr: stderr, PlaybookResolver: memory.PlaybookIDs})
	}
	if len(args) > 0 && args[0] == "--evaluate-once" {
		return evaluateOnce(ctx, detectors, args[1:], stdout, stderr, options.Provider)
	}

	flags := flag.NewFlagSet("sdo-controller", flag.ContinueOnError)
	flags.SetOutput(stderr)
	namespace := flags.String("namespace", "", "Kubernetes namespace to observe")
	controlNamespace := flags.String(
		"control-namespace",
		"",
		"namespace holding the controller's Lease, state, maintenance ConfigMap, and responder Jobs; defaults to --namespace",
	)
	appRoot := flags.String("app-root", "", "application repository root")
	application := flags.String("application", "", "application identity; defaults to repository directory name")
	sourceCommit := flags.String("source-commit", "", "source repository commit; defaults to git HEAD")
	deployedCommit := flags.String("deployed-commit", "", "deployed commit; defaults to source commit")
	dispatcherCommand := flags.String("dispatcher", "", "incident responder executable")
	dispatcherMode := flags.String("dispatcher-mode", "job", "dispatcher mode: job (production) or local (development)")
	responderImage := flags.String("responder-image", "", "responder container image for job mode")
	agentRegistrationDirectory := flags.String("agent-registration-directory", "", "directory of strict ProtoJSON agent registrations")
	responderAgentID := flags.String("responder-agent-id", builtInResponderID, "registered agent ID for incident response")
	repositoryPVC := flags.String("repository-pvc", "", "application repository PVC for job mode")
	repositoryMountPath := flags.String("repository-mount-path", "/workspace", "shared repository PVC mount root for job mode")
	repositoryPVCSubPath := flags.String("repository-pvc-subpath", "", "optional shared repository PVC subPath for job mode")
	credentialsSecret := flags.String("responder-credentials-secret", "", "Codex credentials Secret for job mode")
	var dispatcherArgs repeatedFlag
	flags.Var(&dispatcherArgs, "dispatcher-arg", "incident responder argument; may be repeated")
	var responderEnvironment repeatedFlag
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
		2*time.Minute,
		"maximum wait for independent health detectors to clear after a response",
	)
	maxFollowUps := flags.Int(
		"max-follow-ups",
		0,
		"maximum follow-up responders for health findings that stay active after a response; zero disables follow-ups",
	)
	maxResponseLoops := flags.Int("max-response-loops", DefaultMaxResponseLoops, "maximum sdo apply batches per incident")
	diagnosisDeadline := flags.Duration("diagnosis-deadline", DefaultDiagnosisDeadline, "shared deadline for candidate diagnosis")
	diagnosisSettleDelay := flags.Duration("diagnosis-settle-delay", DefaultDiagnosisSettleDelay, "delay before retrying inconclusive initial diagnoses")
	followUpCooldown := flags.Duration(
		"follow-up-cooldown",
		30*time.Second,
		"wait after a response completes before its still-active health findings get a follow-up responder",
	)
	admissionLoadHigh := flags.Float64(
		"admission-load-high", 0,
		"load-aware admission: host-load pressure (PSI some avg10, 0-100) at or above which a ready incident is held; zero disables the load term",
	)
	admissionLoadLow := flags.Float64(
		"admission-load-low", 0,
		"load-aware admission: host-load pressure below which a held incident resumes (hysteresis); must not exceed --admission-load-high",
	)
	admissionBacklogHigh := flags.Int(
		"admission-release-backlog-high", 0,
		"load-aware admission: stranded-worktree release backlog at or above which a ready incident is held; zero disables the backlog term",
	)
	admissionBacklogLow := flags.Int(
		"admission-release-backlog-low", 0,
		"load-aware admission: release backlog below which a held incident resumes (hysteresis); must not exceed --admission-release-backlog-high",
	)
	admissionRecheckInterval := flags.Duration(
		"admission-recheck-interval", 2*time.Second,
		"load-aware admission: how often a held incident re-evaluates admission against a fresh load sample",
	)
	admissionPressurePath := flags.String(
		"admission-pressure-path", "",
		"load-aware admission: Linux PSI pressure file for the host-load gauge; defaults to /proc/pressure/cpu when the load term is enabled",
	)
	repairPolicy := flags.String("repair-policy", "commit", "repair evidence policy: commit or recorded-actions")
	leaseName := flags.String("lease-name", "sdo-controller", "leader-election Lease name")
	leaseDuration := flags.Duration("lease-duration", 60*time.Second, "leader-election Lease duration")
	identity := flags.String("identity", defaultIdentity(), "unique leader-election identity")
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
		"prober-binary", "", "compiled traffic prober on the shared repository volume; the controller runs it as an isolated pod",
	)
	proberImage := flags.String("prober-image", "", "image that runs the prober binary; defaults to --responder-image")
	proberURL := flags.String("prober-url", "", "use an already running prober at this URL instead of starting one")
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
	timeScale, err := developmentTimeScale()
	if err != nil {
		return err
	}
	detectors = scaleDetectors(detectors, timeScale)
	*diagnosisSettleDelay = scaledDuration(*diagnosisSettleDelay, timeScale)
	*verificationTimeout = scaledDuration(*verificationTimeout, timeScale)
	*followUpCooldown = scaledDuration(*followUpCooldown, timeScale)
	if options.NativeBroker != nil && *forceReflection {
		options.NativeBroker.ForceReflection = true
	}
	if *maxResponseLoops < 1 || *diagnosisDeadline <= 0 {
		return errors.New("max-response-loops and diagnosis-deadline must be positive")
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
	if *dispatcherMode == "local" && *dispatcherCommand == "" {
		return fmt.Errorf("dispatcher is required in local mode")
	}
	if *dispatcherMode == "job" && (*repositoryPVC == "" || *credentialsSecret == "") {
		return fmt.Errorf("repository-pvc and responder-credentials-secret are required in job mode")
	}
	if *dispatcherMode == "job" && *agentRegistrationDirectory == "" && (*dispatcherCommand == "" || *responderImage == "") {
		return fmt.Errorf("dispatcher and responder-image are required for the built-in agent registration")
	}
	if *dispatcherMode == "job" && options.NativeBroker == nil {
		return errors.New("job controller requires the Go incident broker")
	}
	if *repairPolicy != "commit" && *repairPolicy != "recorded-actions" {
		return fmt.Errorf("unsupported repair policy %q", *repairPolicy)
	}
	argv := append([]string{*dispatcherCommand}, dispatcherArgs...)
	var registeredWorkers *worker_registry.Registry
	if *dispatcherMode == "job" {
		var registryErr error
		if *agentRegistrationDirectory == "" {
			registeredWorkers, registryErr = worker_registry.New(
				builtInResponderRegistration(*responderImage, argv),
				builtInReflectorRegistration(*responderImage),
			)
		} else {
			registeredWorkers, registryErr = worker_registry.LoadDirectory(*agentRegistrationDirectory)
		}
		if registryErr != nil {
			return fmt.Errorf("load agent registry: %w", registryErr)
		}
		registration, registryErr := registeredWorkers.Resolve(*responderAgentID, incidentTaskType)
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
	kubernetesCache, err := NewKubernetesCache(KubernetesCacheConfig{
		Namespace: *namespace, Client: bootstrapProvider.Client, StateConfigMapName: stateConfigMapName,
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
	case !*syntheticTraffic || len(trafficWorkloads) == 0:
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
		}
		proberAddress = pod.Address
		responderProberAddress = pod.ServiceAddress
		proberAPI = HTTPProberClient{BaseURL: proberAddress}
	}
	trafficObserver := NewTrafficObserver(proberAPI, trafficWorkloads, func() { kubernetesCache.Notify(traffic.Watch) }, 0)
	var snapshotProvider SnapshotProvider = kubernetesCache
	if trafficObserver != nil {
		snapshotProvider = TrafficSnapshotProvider{Base: kubernetesCache, Observer: trafficObserver}
		defer trafficObserver.Stop()
	}
	jobEnvironment, err := parseResponderEnvironment(responderEnvironment)
	if err != nil {
		return err
	}
	jobEnvironment[ControllerStateEnvironment] = controllerStateLocation(*controlNamespace)
	if launchConfig != nil {
		if err := attachNativeReflector(options.NativeBroker, launchConfig, bootstrapProvider.Client, registeredWorkers, *responseTimeout); err != nil {
			return err
		}
	}
	var stateTracker *StateTracker
	var dispatcher Dispatcher
	switch *dispatcherMode {
	case "local":
		var environment []string
		if proberAPI != nil && *proberURL != "" {
			environment = append(environment, ProberURLEnvironment+"="+*proberURL)
		}
		environment = append(environment, ControllerStateEnvironment+"="+controllerStateLocation(*controlNamespace))
		dispatcher = SubprocessDispatcher{Argv: argv, Env: environment, Timeout: *responseTimeout}
	case "job":
		var catalog memory.CatalogReader
		if options.NativeBroker != nil {
			catalog = memory.OpenCatalogReader(resolvedRoot)
		}
		var primitiveProber PrimitiveProber
		if proberAddress != nil {
			primitiveProber = HTTPProberClient{BaseURL: proberAddress}
		}
		dispatcher = KubernetesJobDispatcher{
			Client: bootstrapProvider.Client, Namespace: *controlNamespace, Registry: registeredWorkers, AgentID: *responderAgentID,
			ServiceAccount: "sdo-responder", RepositoryPVC: *repositoryPVC,
			RepositoryMountPath: *repositoryMountPath, RepositoryPVCSubPath: *repositoryPVCSubPath,
			CredentialsSecret: *credentialsSecret, PollInterval: time.Second,
			Environment: jobEnvironment, DispatchEnvironment: ProberEnvironment(responderProberAddress),
			Catalog: catalog, Outcomes: memory.OpenReader(resolvedRoot), RequireMemorySnapshot: options.NativeBroker != nil,
			LoopFactory: func(request IncidentRequest) *ResponderLoop {
				loop := NewResponderLoop(request, options.Programs, snapshotProvider, detectors, PlanExecutor{
					Client: TypedPlanClient{Client: bootstrapProvider.Client}, Source: GitPlanSource{Repository: resolvedRoot, Images: stateTracker},
					Rollouts: KubernetesPlanRollouts{Client: bootstrapProvider.Client}, Verifier: PlanHealthVerifier{Detectors: detectors, Provider: snapshotProvider},
				}, *maxResponseLoops)
				loop.DiagnosisDeadline = *diagnosisDeadline
				loop.DiagnosisSettleDelay = *diagnosisSettleDelay
				if options.NativeBroker != nil {
					loop.CorpusSessionRoot = options.NativeBroker.SessionRoot
				}
				loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView {
					return LiveReadView{DetectionContext: snapshot, Context: ctx, Client: bootstrapProvider.Client, Prober: primitiveProber,
						Source: GitPlanSource{Repository: resolvedRoot, Images: stateTracker}, SourceCommit: request.SourceCommit, Baseline: request.StateChanges, BaselineSource: stateTracker}
				}
				return loop
			},
		}
	default:
		return fmt.Errorf("unsupported dispatcher mode %q", *dispatcherMode)
	}
	admission := AdmissionConfig{
		Enabled:                     *admissionLoadHigh > 0 || *admissionBacklogHigh > 0,
		HighWatermark:               *admissionLoadHigh,
		LowWatermark:                *admissionLoadLow,
		ReleaseBacklogHighWatermark: *admissionBacklogHigh,
		ReleaseBacklogLowWatermark:  *admissionBacklogLow,
		RecheckInterval:             *admissionRecheckInterval,
	}
	start := time.Now().UTC()
	corpusSessionRoot := ""
	if options.NativeBroker != nil {
		corpusSessionRoot = options.NativeBroker.SessionRoot
	}
	controller, err := NewController(ControllerConfig{
		Application: *application, Namespace: *namespace,
		SourceCommit: resolvedSourceCommit, DeployedCommit: resolvedDeployedCommit,
		RepositoryWorktree: resolvedRoot, ResponseTimeout: *responseTimeout,
		CorpusSessionRoot:   corpusSessionRoot,
		VerificationTimeout: *verificationTimeout,
		MaxFollowUps:        *maxFollowUps, FollowUpCooldown: *followUpCooldown,
		RepairPolicy:    *repairPolicy,
		FiringThreshold: 2, ClearThreshold: 2, BatchDebounce: scaledDuration(500*time.Millisecond, timeScale),
		ConfirmationInterval: scaledDuration(time.Second, timeScale),
		// The submit gate confirms a clearing finding on 3 fresh evaluations
		// over at least 2 s instead of waiting for the detector's interval;
		// closure keeps the health judge's clear persistence.
		GateConfirmation: GateConfirmationPolicy{Evaluations: 3, Window: scaledDuration(2*time.Second, timeScale), Interval: scaledDuration(time.Second, timeScale)},
		Admission:        admission,
	}, detectors, snapshotProvider, dispatcher, start)
	if err != nil {
		return err
	}
	// The load term reads host pressure from a Linux PSI file; the backlog term
	// needs no gauge. A gauge is attached only when the load term is enabled.
	if admission.Enabled && admission.HighWatermark > 0 {
		controller.SetLoadGauge(ProcPressureGauge{Path: *admissionPressurePath})
	}
	if options.NativeBroker != nil {
		if err := controller.SetIncidentBroker(*options.NativeBroker); err != nil {
			return err
		}
	} else if *dispatcherMode == "job" {
		return errors.New("job controller requires the Go incident broker")
	}
	if *stateBaseline {
		stateTracker = NewStateTracker(StateTrackerConfig{Client: bootstrapProvider.Client, Namespace: *namespace, Settle: scaledDuration(DefaultStateSettle, timeScale)})
		controller.Baseline = stateTracker
		defer stateTracker.Stop()
	}
	if *cleanResponderHelpers {
		controller.Helpers = KubernetesHelperCleaner{
			Client: bootstrapProvider.Client, Namespaces: []string{*namespace, *controlNamespace},
		}
	}
	controller.CanAct = elector.IsLeader
	controller.GuardAction = elector.GuardContext
	stateStore := NewConfigMapStateStore(bootstrapProvider.Client, *controlNamespace, controllerStateConfigMap)
	if err := controller.AttachStateStore(ctx, stateStore); err != nil {
		return fmt.Errorf("restore controller state: %w", err)
	}
	if err := attachFiringTelemetry(controller, *telemetryPath, *dispatcherMode, *repositoryMountPath); err != nil {
		fmt.Fprintln(stderr, err)
	}
	if options.NativeBroker != nil && *dispatcherMode == "job" {
		controller.SetFiringSink(lateFindingSink{Downstream: controller.firingSink, Store: latefindings.Store{Client: bootstrapProvider.Client, Namespace: *controlNamespace}})
	}
	if err := attachLifecycleTelemetry(controller, *lifecyclePath, *dispatcherMode, *repositoryMountPath); err != nil {
		fmt.Fprintln(stderr, err)
	}
	if *exitAfterClosure && controller.LastAcknowledgedIncidentID() != "" {
		return nil
	}
	if err := ClosureFailureError(controller); err != nil && *exitAfterClosure {
		return err
	}
	iteration := 0
	reviewGate := &detectorReviewGate{exit: *exitAfterClosure || *duration > 0, log: stderr}
	encoder := json.NewEncoder(stdout)
	controller.OnError = func(err error) { fmt.Fprintln(stderr, err) }
	controller.OnClosureFailed = func(failure ClosureFailure) {
		if err := encoder.Encode(map[string]any{"controller_closure_failed": failure}); err != nil {
			fmt.Fprintln(stderr, err)
		}
	}
	controller.OnEvaluation = func(findings []sdk.Finding) {
		if err := encoder.Encode(map[string]any{
			"controller_iteration": iteration,
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
	// startTraffic makes sure the prober runs, clears its observations, and
	// waits briefly for a first sample of every scenario, so the next
	// evaluation, which may be the all-clear that precedes a fault, judges
	// observed traffic.
	startTraffic := func() {
		if trafficObserver == nil {
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
			// A maintenance window may redeploy the application, so the old
			// baseline no longer describes it.
			stateTracker.Stop()
			stateTracker.Reset()
		}
	}
	persistControllerState := func() {
		if err := controller.PersistState(ctx); err != nil {
			fmt.Fprintf(stderr, "persist controller state: %v\n", err)
		}
	}
	if !desired.Paused {
		kubernetesCache.Start(runCtx)
		cacheStarted = true
		if err := kubernetesCache.WaitForSync(runCtx); err != nil {
			return fmt.Errorf("sync Kubernetes informer cache: %w", err)
		}
		waitState := startState()
		startTraffic()
		waitState()
		if err := controller.Step(runCtx, time.Now().UTC(), nil); err != nil {
			if runCtx.Err() != nil {
				return nil
			}
			return err
		}
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
	if err := reviewGate.check(controller); err != nil {
		return err
	}
	if err := executePendingEffects(runCtx, controller); err != nil {
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
		syncCtx, cancelSync := context.WithTimeout(runCtx, *resumeSyncTimeout)
		defer cancelSync()
		var syncErr error
		if cacheStarted {
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
		waitState := startState()
		startTraffic()
		waitState()
		kubernetesCache.TakeEvents()
		applied = desired
		if err := encoder.Encode(applied.Record()); err != nil {
			fmt.Fprintln(stderr, err)
		}
		if err := controller.EvaluateAll(runCtx, time.Now().UTC()); err != nil {
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
		if err := reviewGate.check(controller); err != nil {
			return err
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
				stopState()
				kubernetesCache.Stop()
				kubernetesCache.TakeEvents()
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
				continue
			}
			if err := controller.StepEvents(runCtx, time.Now().UTC(), kubernetesCache.TakeEvents()); err != nil {
				if runCtx.Err() != nil {
					return nil
				}
				return err
			}
			persistControllerState()
			if err := reviewGate.check(controller); err != nil {
				return err
			}
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
			if err := reviewGate.check(controller); err != nil {
				return err
			}
			if err := executePendingEffects(runCtx, controller); err != nil {
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
			if err := reviewGate.check(controller); err != nil {
				return err
			}
			if err := executePendingEffects(runCtx, controller); err != nil {
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
			if err := executePendingEffects(runCtx, controller); err != nil {
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
			persistControllerState()
			if err := executePendingEffects(runCtx, controller); err != nil {
				if runCtx.Err() != nil {
					return nil
				}
				return err
			}
			if *exitAfterClosure && completion.err == nil {
				return nil
			}
			if *restartAfterClosure && options.NativeBroker == nil && completion.err == nil {
				if err := encoder.Encode(map[string]any{"controller_closure_restart": controller.LastAcknowledgedIncidentID()}); err != nil {
					fmt.Fprintln(stderr, err)
				}
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
			if err := reviewGate.check(controller); err != nil {
				return err
			}
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
func attachFiringTelemetry(controller *Controller, path string, dispatcherMode string, mountPath string) error {
	if path == "off" || (path == "" && dispatcherMode != "job") {
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
func attachLifecycleTelemetry(controller *Controller, path string, dispatcherMode string, mountPath string) error {
	if path == "off" || (path == "" && dispatcherMode != "job") {
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

// detectorReviewGate ends a one-shot run when health did not clear after the
// responder. A persistent controller keeps observing instead and reports the
// review once: its Job restarts it on exit, so exiting crash-looped it past
// the backoff limit and stopped detection for every later incident. The
// incident still closes, marked late-verified, if health clears.
type detectorReviewGate struct {
	exit     bool
	log      io.Writer
	reported string
}

func (gate *detectorReviewGate) check(controller *Controller) error {
	required, reason := controller.DetectorReviewStatus()
	if !required {
		gate.reported = ""
		return nil
	}
	if gate.exit {
		return fmt.Errorf("detector review required: %s", reason)
	}
	if gate.reported != reason {
		gate.reported = reason
		fmt.Fprintf(gate.log, "detector review required (controller keeps observing): %s\n", reason)
	}
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
	flags.String("app-root", "", "application repository root")
	proberURL := flags.String("prober-url", "", "live traffic prober URL for bounded health evaluation")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *namespace == "" {
		return fmt.Errorf("namespace is required")
	}
	if *application == "" {
		*application = *namespace
	}
	if provider == nil {
		kubernetesProvider, err := core.NewKubernetesSnapshotProvider(*namespace)
		if err != nil {
			return fmt.Errorf("create Kubernetes snapshot provider: %w", err)
		}
		provider = kubernetesProvider
	}
	if workloads := TrafficWorkloadNames(detectors); len(workloads) > 0 {
		if *proberURL == "" {
			return errors.New("traffic health evaluation requires a live prober URL")
		}
		observer := NewTrafficObserver(HTTPProberClient{BaseURL: StaticProberURL(*proberURL)}, workloads, nil, 0)
		if !observer.WaitWarm(ctx, 30*time.Second) {
			return errors.New("traffic prober did not produce a complete bounded health window")
		}
		provider = TrafficSnapshotProvider{Base: provider, Observer: observer}
	}
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
	status := DetectorEvaluationClear
	for _, finding := range findings {
		if finding.Status == sdk.FindingActive {
			status = DetectorEvaluationFiring
			break
		}
	}
	return json.NewEncoder(stdout).Encode(map[string]any{"status": status, "findings": findings})
}

func defaultIdentity() string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "sdo-controller"
	}
	return hostname + "-" + strconv.Itoa(os.Getpid())
}

// executePausedEffects runs only operational-memory effects: a paused
// controller neither prepares nor dispatches a responder.
func executePausedEffects(ctx context.Context, controller *Controller) (err error) {
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
	if release, ok := controller.PendingReleaseEffect(); ok {
		if err := controller.ExecuteReleaseEffect(ctx, release); err != nil {
			return fmt.Errorf("execute persisted release effect: %w", err)
		}
	}
	return nil
}

func executePendingEffects(ctx context.Context, controller *Controller) (err error) {
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
	if release, ok := controller.PendingReleaseEffect(); ok {
		if err := controller.ExecuteReleaseEffect(ctx, release); err != nil {
			return fmt.Errorf("execute persisted release effect: %w", err)
		}
	}
	return nil
}

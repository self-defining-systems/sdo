package runtime

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/core"
	"sdo.dev/operator/latefindings"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/observation"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/toolchain"
	"sdo.dev/operator/worker_registry"
)

// nativeBrokerFromLaunch makes the compiled controller child select the Go
// broker from its typed launch contract. Native mode and proposal-only worker
// mounts are one validation decision, before leader election or dispatch.
func nativeBrokerFromLaunch(_ context.Context, config *contracts.ControllerConfig, client kubernetes.Interface) (*NativeIncidentBroker, kubernetes.Interface, error) {
	if config == nil {
		return nil, nil, errors.New("native broker requires typed controller config")
	}
	if _, err := codePathFromContract(config.GetCodePath()); err != nil {
		return nil, nil, err
	}
	if config.GetBrokerMode() != "native" || config.GetWorkerMountMode() != "proposal-only" {
		return nil, nil, errors.New("native broker requires proposal-only worker mounts")
	}
	if config.GetValidatorImage() == "" || config.GetSchemaIdentity() == "" || config.GetResponderBackend() == "" || config.GetResponderModel() == "" ||
		config.GetBrokerWorktreeRoot() == "" || config.GetAppRoot() == "" || config.GetRepositoryPvc() == "" || config.GetRepositoryMountPath() == "" {
		return nil, nil, errors.New("native broker requires validator, schema, responder identity, proposal root, and repository mount")
	}
	if err := toolchain.ValidateImage(config.GetToolchainImage()); err != nil {
		return nil, nil, err
	}
	if client == nil {
		var err error
		client, err = core.NewKubernetesClient()
		if err != nil {
			return nil, nil, fmt.Errorf("native broker Kubernetes client: %w", err)
		}
	}
	if _, err := AgentWorkerEnvironment(config); err != nil {
		return nil, nil, err
	}
	controlNamespace := config.GetControlNamespace()
	if controlNamespace == "" {
		controlNamespace = config.GetNamespace()
	}
	job := broker.KubernetesValidationJobClient{
		ToolchainImage: config.GetToolchainImage(),
		Client:         client, Namespace: controlNamespace, Image: config.GetValidatorImage(),
		RepositoryPVC: config.GetRepositoryPvc(), RepositoryMountPath: config.GetRepositoryMountPath(),
		RepositoryPVCSubPath: config.GetRepositoryPvcSubpath(), SchemaIdentity: config.GetSchemaIdentity(),
		HealthyBaseline: config.GetHealthyBaseline(),
		OnPhase:         recordClosurePhase,
		IsolationProof:  &broker.IsolationProof{},
	}
	ceiling, policyErr := observation.CeilingFromEnvironment()
	if policyErr != nil {
		return nil, nil, policyErr
	}

	selectedCodePath, err := NewCodePathRegistryWithDependencies(CodePathDependencies{ValidateGo: broker.JobValidator(job)}).Resolve(codePathConfigFromContract(config.GetCodePath()))
	if err != nil {
		return nil, nil, err
	}
	memoryCodePath := "go"
	if selectedCodePath.Selection.MemoryValidation != "go" {
		memoryCodePath = "shell"
	}
	repository, err := broker.New(config.GetAppRoot(), config.GetBrokerWorktreeRoot(), selectedCodePath.Memory.Validate, broker.ObservationPolicy{CodePath: memoryCodePath, Ceiling: ceiling, ApplicationNamespace: config.GetNamespace()})
	if err != nil {
		return nil, nil, fmt.Errorf("native repository broker: %w", err)
	}
	ledgerRoot, err := broker.NativeLedgerRoot(config.GetBrokerWorktreeRoot())
	if err != nil {
		return nil, nil, err
	}
	return &NativeIncidentBroker{
		Repository: repository, LedgerRoot: ledgerRoot,
		CodePath: codePathConfigFromContract(config.GetCodePath()),
		Deriver:  ProtoOutcomeDeriver{}, ResponderBackend: config.GetResponderBackend(), ResponderModel: config.GetResponderModel(),
		ForceReflection: config.GetForceReflection(), PhaseLog: os.Stdout,
		LateFindingsVerifier: latefindings.Store{Client: client, Namespace: controlNamespace},
		SessionRoot:          config.GetRepositoryMountPath(), SessionRetention: DefaultSessionRetention,
	}, client, nil
}

func attachNativeReflector(native *NativeIncidentBroker, config *contracts.ControllerConfig, client kubernetes.Interface, registry *worker_registry.Registry, timeout time.Duration) error {
	if native == nil {
		return nil
	}
	if config == nil || client == nil || registry == nil || native.LedgerRoot == "" {
		return errors.New("native reflector requires typed launch config, Kubernetes client, agent registry, and ledger root")
	}
	reflectorID, err := registry.AgentFor(&contracts.ReflectionRequest{})
	if err != nil {
		return fmt.Errorf("native reflection agent registration: %w", err)
	}
	reflectionTask, err := registry.TaskType(reflectorID, &contracts.ReflectionRequest{})
	if err != nil {
		return fmt.Errorf("native reflection agent registration: %w", err)
	}
	if timeout <= 0 {
		timeout = 30 * time.Minute
	}
	namespace := config.GetControlNamespace()
	if namespace == "" {
		namespace = config.GetNamespace()
	}
	agentEnvironment, err := AgentWorkerEnvironment(config)
	if err != nil {
		return err
	}
	freshExecutor := worker_registry.KubernetesExecutor{
		ToolchainImage: config.GetToolchainImage(),
		Client:         client, Namespace: namespace, Registry: registry,
		ServiceAccount: "sdo-responder", RepositoryPVC: config.GetRepositoryPvc(), RepositoryMountPath: config.GetRepositoryMountPath(),
		RepositoryPVCSubPath: config.GetRepositoryPvcSubpath(), CredentialsSecret: config.GetResponderCredentialsSecret(), HealthyBaseline: config.GetHealthyBaseline(),
		Environment: mergeEnvironment(agentEnvironment, map[string]string{"CODEX_HOME": "/tmp/codex", "CLAUDE_CONFIG_DIR": "/tmp/claude"}),
		OnPhase:     recordClosurePhase,
	}
	sessionExecutor := func(incidentID string, resume bool) (ReflectionTaskExecutor, error) {
		subPath, err := memory.PrepareIncidentSession(config.GetRepositoryMountPath(), config.GetRepositoryPvcSubpath(), incidentID)
		if err != nil {
			return nil, err
		}
		executor := freshExecutor
		executor.SessionPVCSubPath = subPath
		executor.CapturedCorpusReadOnly = true
		executor.Environment = mergeEnvironment(agentEnvironment, map[string]string{"CODEX_HOME": "/tmp/codex", "CLAUDE_CONFIG_DIR": "/tmp/claude", "SDO_TURN_USAGE_LOG": "/sdo/session/reflection-usage.jsonl"})
		if resume {
			executor.Environment["CODEX_HOME"] = "/sdo/session/codex"
			executor.Environment["CLAUDE_CONFIG_DIR"] = "/sdo/session/claude"
		}
		return executor, nil
	}
	catalog := memory.OpenCatalogReader(config.GetAppRoot())
	selected, err := codePathFromContract(config.GetCodePath())
	if err != nil {
		return err
	}
	if selected.Selection.DetectorSource == "shell" {
		catalog = memory.OpenShellCatalogReader(config.GetAppRoot())
	}
	native.Reflector = RegisteredReflectionWorker{
		TaskStore: &lifecycleio.TaskStore{Root: filepath.Join(native.LedgerRoot, "reflection-tasks")},
		Executor:  freshExecutor,
		AgentID:   reflectorID, TaskType: reflectionTask, Outcomes: memory.NewJSONStore(config.GetAppRoot()), Timeout: timeout,
		SessionMode:       config.GetReflectionSession(),
		Guidance:          config.GetReflectionGuidance(),
		Catalog:           catalog,
		CorpusSessionRoot: config.GetRepositoryMountPath(),
		SessionExecutor: func(incidentID string) (ReflectionTaskExecutor, error) {
			return sessionExecutor(incidentID, true)
		},
		FreshSessionExecutor: func(incidentID string) (ReflectionTaskExecutor, error) { return sessionExecutor(incidentID, false) },
		UsageLogPath: func(incidentID string) string {
			digest := fmt.Sprintf("%x", sha256.Sum256([]byte(incidentID)))[:32]
			return filepath.Join(config.GetRepositoryMountPath(), ".sdo-runtime", "sessions", digest, "reflection-usage.jsonl")
		},
	}
	return nil
}

// AgentWorkerEnvironment is the provider and model the controller was configured
// with, as the environment every agent worker Job reads. Workers default to Codex
// when SDO_AGENT_PROVIDER is absent, so a configured provider must always travel
// with the Job. An unconfigured provider adds nothing; an unknown one is an error.
func AgentWorkerEnvironment(config *contracts.ControllerConfig) (map[string]string, error) {
	environment, err := worker_registry.AgentEnvironment(config.GetResponderBackend(), config.GetResponderModel())
	if err != nil {
		return nil, err
	}
	selected, err := codePathFromContract(config.GetCodePath())
	if err != nil {
		return nil, err
	}
	if config.GetCodePath() != nil {
		environment["SDO_CODE_PATH"] = selected.Selection.Name
		environment["SDO_AGENT_GUIDANCE"] = selected.Selection.AgentGuidance
		environment["SDO_MEMORY_VALIDATION"] = "go"
		if selected.Selection.MemoryValidation != "go" {
			environment["SDO_MEMORY_VALIDATION"] = "shell"
		}
	}
	return environment, nil
}

// withAgentEnvironment makes the launch config authoritative for provider and
// model over any hand-written responder environment entries.
func withAgentEnvironment(environment map[string]string, config *contracts.ControllerConfig) (map[string]string, error) {
	if config == nil {
		return environment, nil
	}
	agent, err := AgentWorkerEnvironment(config)
	if err != nil {
		return nil, err
	}
	return mergeEnvironment(environment, agent), nil
}

func mergeEnvironment(layers ...map[string]string) map[string]string {
	merged := map[string]string{}
	for _, layer := range layers {
		for name, value := range layer {
			merged[name] = value
		}
	}
	return merged
}

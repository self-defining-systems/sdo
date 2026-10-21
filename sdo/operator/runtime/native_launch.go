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
	"sdo.dev/operator/worker_registry"
)

// nativeBrokerFromLaunch makes the compiled controller child select the Go
// broker from its typed launch contract. Native mode and proposal-only worker
// mounts are one validation decision, before leader election or dispatch.
func nativeBrokerFromLaunch(_ context.Context, config *contracts.ControllerConfig, client kubernetes.Interface) (*NativeIncidentBroker, kubernetes.Interface, error) {
	if config == nil {
		return nil, nil, errors.New("native broker requires typed controller config")
	}
	if config.GetBrokerMode() != "native" || config.GetWorkerMountMode() != "proposal-only" {
		return nil, nil, errors.New("native broker requires proposal-only worker mounts")
	}
	if config.GetValidatorImage() == "" || config.GetSchemaIdentity() == "" || config.GetResponderBackend() == "" || config.GetResponderModel() == "" ||
		config.GetBrokerWorktreeRoot() == "" || config.GetAppRoot() == "" || config.GetRepositoryPvc() == "" || config.GetRepositoryMountPath() == "" {
		return nil, nil, errors.New("native broker requires validator, schema, responder identity, proposal root, and repository mount")
	}
	if client == nil {
		var err error
		client, err = core.NewKubernetesClient()
		if err != nil {
			return nil, nil, fmt.Errorf("native broker Kubernetes client: %w", err)
		}
	}
	controlNamespace := config.GetControlNamespace()
	if controlNamespace == "" {
		controlNamespace = config.GetNamespace()
	}
	job := broker.KubernetesValidationJobClient{
		Client: client, Namespace: controlNamespace, Image: config.GetValidatorImage(),
		RepositoryPVC: config.GetRepositoryPvc(), RepositoryMountPath: config.GetRepositoryMountPath(),
		RepositoryPVCSubPath: config.GetRepositoryPvcSubpath(), SchemaIdentity: config.GetSchemaIdentity(),
		HealthyBaseline: config.GetHealthyBaseline(),
		OnPhase:         recordClosurePhase,
	}
	repository, err := broker.New(config.GetAppRoot(), config.GetBrokerWorktreeRoot(), broker.JobValidator(job))
	if err != nil {
		return nil, nil, fmt.Errorf("native repository broker: %w", err)
	}
	ledgerRoot, err := broker.NativeLedgerRoot(config.GetBrokerWorktreeRoot())
	if err != nil {
		return nil, nil, err
	}
	return &NativeIncidentBroker{
		Repository: repository, LedgerRoot: ledgerRoot,
		Deriver: ProtoOutcomeDeriver{}, ResponderBackend: config.GetResponderBackend(), ResponderModel: config.GetResponderModel(),
		PriorOutcomes: memory.OpenReader(config.GetAppRoot()), ForceReflection: config.GetForceReflection(), PhaseLog: os.Stdout,
		LateFindingsVerifier: latefindings.Store{Client: client, Namespace: controlNamespace},
		SessionRoot:          config.GetRepositoryMountPath(), SessionRetention: DefaultSessionRetention,
	}, client, nil
}

func attachNativeReflector(native *NativeIncidentBroker, config *contracts.ControllerConfig, client kubernetes.Interface, registry *worker_registry.Registry, timeout time.Duration) error {
	if native == nil {
		return nil
	}
	if config == nil || client == nil || registry == nil {
		return errors.New("native reflector requires typed launch config, Kubernetes client, and agent registry")
	}
	if _, err := registry.Resolve("sdo-reflector", "outcome-reflection"); err != nil {
		return fmt.Errorf("native reflection agent registration: %w", err)
	}
	if timeout <= 0 {
		timeout = 30 * time.Minute
	}
	namespace := config.GetControlNamespace()
	if namespace == "" {
		namespace = config.GetNamespace()
	}
	freshExecutor := worker_registry.KubernetesExecutor{
		Client: client, Namespace: namespace, Registry: registry,
		ServiceAccount: "sdo-responder", RepositoryPVC: config.GetRepositoryPvc(), RepositoryMountPath: config.GetRepositoryMountPath(),
		RepositoryPVCSubPath: config.GetRepositoryPvcSubpath(), CredentialsSecret: config.GetResponderCredentialsSecret(),
		Environment: map[string]string{"CODEX_HOME": "/tmp/codex", "CLAUDE_CONFIG_DIR": "/tmp/claude"},
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
		executor.Environment = map[string]string{"CODEX_HOME": "/tmp/codex", "CLAUDE_CONFIG_DIR": "/tmp/claude", "SDO_TURN_USAGE_LOG": "/sdo/session/reflection-usage.jsonl"}
		if resume {
			executor.Environment["CODEX_HOME"] = "/sdo/session/codex"
			executor.Environment["CLAUDE_CONFIG_DIR"] = "/sdo/session/claude"
		}
		return executor, nil
	}
	native.Reflector = RegisteredReflectionWorker{
		Executor: freshExecutor,
		AgentID:  "sdo-reflector", Outcomes: memory.NewJSONStore(config.GetAppRoot()), Timeout: timeout,
		SessionMode:       config.GetReflectionSession(),
		Guidance:          config.GetReflectionGuidance(),
		Catalog:           memory.OpenCatalogReader(config.GetAppRoot()),
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

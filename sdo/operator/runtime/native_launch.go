package runtime

import (
	"context"
	"errors"
	"fmt"
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
	}
	native.Reflector = RegisteredReflectionWorker{
		Executor: freshExecutor,
		AgentID:  "sdo-reflector", Outcomes: memory.NewJSONStore(config.GetAppRoot()), Timeout: timeout,
		SessionMode: config.GetReflectionSession(),
		Guidance:    config.GetReflectionGuidance(),
		Catalog:     memory.OpenCatalogReader(config.GetAppRoot()),
		SessionExecutor: func(incidentID string) (ReflectionTaskExecutor, error) {
			subPath, err := memory.PrepareIncidentSession(config.GetRepositoryMountPath(), config.GetRepositoryPvcSubpath(), incidentID)
			if err != nil {
				return nil, err
			}
			resumed := freshExecutor
			resumed.SessionPVCSubPath = subPath
			resumed.Environment = map[string]string{"CODEX_HOME": "/sdo/session/codex", "CLAUDE_CONFIG_DIR": "/sdo/session/claude"}
			return resumed, nil
		},
	}
	return nil
}

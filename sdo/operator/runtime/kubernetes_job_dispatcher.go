package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"k8s.io/client-go/kubernetes"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/latefindings"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/worker_registry"
)

const (
	jobRequestDataKey = worker_registry.TaskDataKey
	jobResultDataKey  = worker_registry.ResultDataKey
)

// KubernetesJobDispatcher adapts the incident protocol to the shared,
// implementation-neutral worker Job transport.
type KubernetesJobDispatcher struct {
	Client                kubernetes.Interface
	Namespace             string
	Registry              *worker_registry.Registry
	AgentID               string
	ServiceAccount        string
	RepositoryPVC         string
	RepositoryMountPath   string
	RepositoryPVCSubPath  string
	CredentialsSecret     string
	Environment           map[string]string
	DispatchEnvironment   func(context.Context) map[string]string
	PollInterval          time.Duration
	Catalog               memory.CatalogReader
	RequireMemorySnapshot bool
}

func (d KubernetesJobDispatcher) Dispatch(ctx context.Context, request IncidentRequest) (IncidentResult, error) {
	if d.Registry == nil || d.AgentID == "" {
		return IncidentResult{}, errors.New("incident dispatch requires an agent registry and responder agent ID")
	}
	if err := validateIncidentRequest(request); err != nil {
		return IncidentResult{}, fmt.Errorf("validate incident request: %w", err)
	}
	var snapshotArgs []*contracts.OperationalMemorySnapshot
	if d.RequireMemorySnapshot && d.Catalog == nil {
		return IncidentResult{}, errors.New("native responder requires logical operational memory")
	}
	if d.Catalog != nil {
		snapshot, err := d.Catalog.Snapshot(ctx)
		if err != nil {
			return IncidentResult{}, fmt.Errorf("read logical operational memory: %w", err)
		}
		snapshotArgs = append(snapshotArgs, snapshot)
	}
	task, _, err := encodeIncidentAgentTask(request, d.Registry, d.AgentID, snapshotArgs...)
	if err != nil {
		return IncidentResult{}, err
	}
	if err := d.Registry.ValidateTask(task); err != nil {
		return IncidentResult{}, fmt.Errorf("validate agent task: %w", err)
	}
	environment := make(map[string]string, len(d.Environment))
	for name, value := range d.Environment {
		environment[name] = value
	}
	if d.DispatchEnvironment != nil {
		for name, value := range d.DispatchEnvironment(ctx) {
			environment[name] = value
		}
	}
	lateConfigMap := ""
	sessionSubPath := ""
	if d.RequireMemorySnapshot {
		sessionSubPath, err = memory.PrepareIncidentSession(d.RepositoryMountPath, d.RepositoryPVCSubPath, request.IncidentID)
		if err != nil {
			return IncidentResult{}, fmt.Errorf("prepare isolated responder session: %w", err)
		}
		environment["CODEX_HOME"] = "/sdo/session/codex"
		environment["CLAUDE_CONFIG_DIR"] = "/sdo/session/claude"
		environment[latefindings.ReceiptsPathEnv] = latefindings.DefaultReceiptsPath
		store := latefindings.Store{Client: d.Client, Namespace: d.Namespace}
		if err := store.Ensure(ctx, request.IncidentID); err != nil {
			return IncidentResult{}, fmt.Errorf("prepare logical late findings transport: %w", err)
		}
		lateConfigMap = latefindings.ConfigMapName(request.IncidentID)
	}
	executor := worker_registry.KubernetesExecutor{
		Client: d.Client, Namespace: d.Namespace, Registry: d.Registry,
		ServiceAccount: d.ServiceAccount, RepositoryPVC: d.RepositoryPVC,
		RepositoryMountPath: d.RepositoryMountPath, RepositoryPVCSubPath: d.RepositoryPVCSubPath,
		CredentialsSecret: d.CredentialsSecret, Environment: environment, PollInterval: d.PollInterval, SessionPVCSubPath: sessionSubPath,
		LateFindingsConfigMap: lateConfigMap,
	}
	agentResult, err := executor.Execute(ctx, task)
	if err != nil {
		var failed *worker_registry.JobFailedError
		if errors.As(err, &failed) {
			return IncidentResult{}, &ResponderJobFailedError{JobName: failed.JobName}
		}
		return IncidentResult{}, err
	}
	result, err := incidentResultFromAgent(agentResult, task, d.Registry)
	if err != nil {
		return IncidentResult{}, err
	}
	if d.RequireMemorySnapshot && result.LateFindingsPull != nil {
		pull := result.LateFindingsPull
		store := latefindings.Store{Client: d.Client, Namespace: d.Namespace}
		if err := store.VerifyPull(ctx, request.IncidentID, pull.PullCount, pull.ReportDigest, pull.DetectorIDs, pull.PlaybookIDs); err != nil {
			return IncidentResult{}, fmt.Errorf("verify logical late findings pull: %w", err)
		}
	}
	if err := validateIncidentResultFor(result, request); err != nil {
		return IncidentResult{}, fmt.Errorf("validate incident result: %w", err)
	}
	return result, nil
}

// ResponderJobFailedError preserves the controller's terminal dispatch signal.
type ResponderJobFailedError struct{ JobName string }

func (err *ResponderJobFailedError) Error() string {
	return fmt.Sprintf("responder Job %q failed", err.JobName)
}

func IncidentJobName(incidentID string) string { return worker_registry.JobName(incidentID) }

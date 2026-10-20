package runtime

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
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
	Image                 string
	Command               []string
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
	Outcomes              memory.Reader
	RequireMemorySnapshot bool
	LoopFactory           func(IncidentRequest) *ResponderLoop
}

func (d KubernetesJobDispatcher) Dispatch(ctx context.Context, request IncidentRequest) (IncidentResult, error) {
	if err := request.Validate(); err != nil {
		return IncidentResult{}, fmt.Errorf("validate incident request: %w", err)
	}
	if d.AgentID == "" {
		d.AgentID = builtInResponderID
	}
	var loop *ResponderLoop
	if d.LoopFactory != nil {
		loop = d.LoopFactory(request)
		if loop != nil {
			if d.Outcomes != nil {
				stored, readErr := d.Outcomes.List(ctx)
				if readErr != nil {
					return IncidentResult{}, fmt.Errorf("read program trust outcomes: %w", readErr)
				}
				records := make([]*contracts.OutcomeRecord, 0, len(stored))
				for _, outcome := range stored {
					if outcome.Record != nil {
						records = append(records, outcome.Record)
					}
				}
				loop.Trust = ProgramTrustStats(records)
			}
			request.InitialProgramRuns = loop.InitialDiagnose(ctx)
			fillProgramRevisions(ctx, request.InitialProgramRuns, request.RepositoryWorktree, request.SourceCommit)
			request.MaxResponseLoops = loop.MaxBatches
		}
	}
	if d.Registry == nil {
		var err error
		d.Registry, err = worker_registry.New(builtInResponderRegistration(d.Image, d.Command))
		if err != nil {
			return IncidentResult{}, fmt.Errorf("register built-in responder: %w", err)
		}
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
	task, _, err := encodeIncidentAgentTask(request, d.AgentID, snapshotArgs...)
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
		environment["SDO_SESSION_SCRATCH"] = "/sdo/session/scratch"
		store := latefindings.Store{Client: d.Client, Namespace: d.Namespace}
		if err := store.Ensure(ctx, request.IncidentID); err != nil {
			return IncidentResult{}, fmt.Errorf("prepare logical late findings transport: %w", err)
		}
		lateConfigMap = latefindings.ConfigMapName(request.IncidentID)
	}
	if loop != nil && sessionSubPath != "" {
		base := d.RepositoryPVCSubPath
		if base == "" {
			base = "."
		}
		relative, pathErr := filepath.Rel(base, sessionSubPath)
		if pathErr != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return IncidentResult{}, fmt.Errorf("incident tool session is outside repository mount")
		}
		directory := filepath.Join(d.RepositoryMountPath, relative, "tools")
		toolCtx, stopTools := context.WithCancel(ctx)
		defer stopTools()
		server := SessionToolServer{Directory: directory, Handle: loop.Handle}
		if err := server.Prepare(); err != nil {
			return IncidentResult{}, fmt.Errorf("prepare responder tools: %w", err)
		}
		go func() { _ = server.Serve(toolCtx) }()
		environment["SDO_TOOLS_DIR"] = "/sdo/session/tools"
	}
	executor := worker_registry.KubernetesExecutor{
		Client: d.Client, Namespace: d.Namespace, Registry: d.Registry,
		ServiceAccount: d.ServiceAccount, RepositoryPVC: d.RepositoryPVC,
		RepositoryMountPath: d.RepositoryMountPath, RepositoryPVCSubPath: d.RepositoryPVCSubPath,
		CredentialsSecret: d.CredentialsSecret, Environment: environment, PollInterval: d.PollInterval, SessionPVCSubPath: sessionSubPath,
		CapturedCorpusReadOnly: d.RequireMemorySnapshot,
		LateFindingsConfigMap:  lateConfigMap,
	}
	agentResult, err := executor.Execute(ctx, task)
	if err != nil {
		var failed *worker_registry.JobFailedError
		if errors.As(err, &failed) {
			return IncidentResult{}, &ResponderJobFailedError{JobName: failed.JobName}
		}
		return IncidentResult{}, err
	}
	payload, err := canonicalProtoJSON(agentResult)
	if err != nil {
		return IncidentResult{}, fmt.Errorf("encode agent result: %w", err)
	}
	result, err := decodeIncidentAgentResult(payload, task, d.Registry)
	if err != nil {
		return IncidentResult{}, err
	}
	if loop != nil {
		result.ProgramToolCalls = loop.ToolCalls()
		result.ProgramRuns = loop.ProgramRuns()
		fillProgramRevisions(ctx, result.ProgramRuns, request.RepositoryWorktree, request.SourceCommit)
		result.PlanApplications = loop.PlanApplications()
		result.RepairActions = ProgramRepairReceipts(result.PlanApplications, result.RepairActions)
		result.ProgramFailureSignals = loop.FailureSignals()
		result.ResponseLoopsUsed = loop.LoopsUsed()
		result.ResponseLoopExhausted = loop.Exhausted()
		if result.ResponseLoopExhausted {
			result.Status = IncidentFailed
			result.Error = "response loop budget exhausted with unresolved findings"
		}
	}
	if d.RequireMemorySnapshot && result.LateFindingsPull != nil {
		pull := result.LateFindingsPull
		store := latefindings.Store{Client: d.Client, Namespace: d.Namespace}
		if err := store.VerifyPull(ctx, request.IncidentID, pull.PullCount, pull.ReportDigest, pull.DetectorIDs, pull.PlaybookIDs); err != nil {
			return IncidentResult{}, fmt.Errorf("verify logical late findings pull: %w", err)
		}
	}
	if err := result.ValidateFor(request); err != nil {
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

package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	CoverageProvider CoverageProvider

	Standby               *worker_registry.StandbyPool
	APIProxyImage         string
	ToolchainImage        string
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
	Outcomes              memory.Reader
	RequireMemorySnapshot bool
	LoopFactory           func(IncidentRequest) *ResponderLoop
}

// PrepareDispatch freezes the worker task before the controller records its
// pending dispatch. Retries use this task even if observations later change.
func (d KubernetesJobDispatcher) PrepareDispatch(ctx context.Context, request IncidentRequest) (IncidentRequest, error) {
	if err := validateIncidentRequest(request); err != nil {
		return IncidentRequest{}, fmt.Errorf("validate incident request: %w", err)
	}
	if request.PreparedAgentTask != "" {
		return request, nil
	}
	if d.AgentID == "" {
		if d.Registry == nil {
			return IncidentRequest{}, errors.New("incident dispatcher requires an agent registry")
		}
		var err error
		d.AgentID, err = d.Registry.AgentFor(&contracts.IncidentRequest{})
		if err != nil {
			return IncidentRequest{}, err
		}
	}
	if d.Client != nil {
		name := worker_registry.JobName(request.IncidentID) + "-request"
		existing, err := d.Client.CoreV1().ConfigMaps(d.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err == nil {
			payload := existing.Data[jobRequestDataKey]
			task := new(contracts.AgentTask)
			if payload == "" || protojson.Unmarshal([]byte(payload), task) != nil ||
				task.GetTaskId() != request.IncidentID || task.GetAgentId() != d.AgentID ||
				task.GetWorkspace().GetCheckoutPath() != request.RepositoryWorktree ||
				task.GetWorkspace().GetBaseCommit() != request.RepositoryBaseCommit {
				return IncidentRequest{}, fmt.Errorf("existing agent request ConfigMap %q differs from pending incident", name)
			}
			typed := new(contracts.IncidentRequest)
			if err := task.GetRequest().UnmarshalTo(typed); err != nil {
				return IncidentRequest{}, fmt.Errorf("decode existing incident request: %w", err)
			}
			if typed.GetIncidentId() != request.IncidentID || typed.GetRepositoryBaseCommit() != request.RepositoryBaseCommit {
				return IncidentRequest{}, fmt.Errorf("existing agent request ConfigMap %q differs from pending incident", name)
			}
			request.InitialProgramRuns = nil
			for _, run := range typed.GetInitialProgramRuns() {
				request.InitialProgramRuns = append(request.InitialProgramRuns, programRunState(run))
			}
			request.PlaybookCoverage = cloneCoverage(typed.GetPlaybookCoverage())
			request.Findings = findingsState(typed.GetFindings())
			request.MaxResponseLoops = int(typed.GetMaxResponseLoops())
			request.PreparedAgentTask = payload
			return request, nil
		}
		if !apierrors.IsNotFound(err) {
			return IncidentRequest{}, fmt.Errorf("get existing agent request ConfigMap: %w", err)
		}
	}
	coverage := d.CoverageProvider
	if coverage == nil {
		coverage = goCodePathCoverage{}
	}
	if d.LoopFactory != nil {
		if loop := d.LoopFactory(request); loop != nil {
			request = loop.prepareDispatch(ctx, request, d.programCounts(ctx), coverage)
		}
	}
	if request.PlaybookCoverage == nil {
		request.PlaybookCoverage = coverage.Coverage(request.Findings, nil, nil, nil)
	}
	var snapshots []*contracts.OperationalMemorySnapshot
	if d.RequireMemorySnapshot && d.Catalog == nil {
		return IncidentRequest{}, errors.New("native responder requires logical operational memory")
	}
	if d.Catalog != nil {
		snapshot, err := memory.SnapshotAt(ctx, d.Catalog, request.RepositoryBaseCommit)
		if err != nil {
			return IncidentRequest{}, fmt.Errorf("read logical operational memory: %w", err)
		}
		snapshots = append(snapshots, snapshot)
	}
	_, payload, err := encodeIncidentAgentTask(request, d.Registry, d.AgentID, snapshots...)
	if err != nil {
		return IncidentRequest{}, err
	}
	request.PreparedAgentTask = string(payload)
	return request, nil
}

// prepareDispatch snapshots diagnosis and its coverage together; the factory
// may already have exposed this loop to the controller's observation goroutine.
func (l *ResponderLoop) prepareDispatch(ctx context.Context, request IncidentRequest, counts map[string]ProgramCount, coverage CoverageProvider) IncidentRequest {
	l.operationMu.Lock()
	defer l.operationMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ProgramCounts = counts
	l.capturePhase = "open@detect"
	defer func() { l.capturePhase = "" }()
	request.InitialProgramRuns = l.diagnose(ctx, l.findings)
	fillProgramRevisions(ctx, request.InitialProgramRuns, request.RepositoryWorktree, request.SourceCommit)
	request.PlaybookCoverage = coverage.Coverage(request.Findings, request.InitialProgramRuns, l.lastDiagnosisView, counts)
	request.MaxResponseLoops = l.MaxBatches
	return request
}

func (d KubernetesJobDispatcher) Dispatch(ctx context.Context, request IncidentRequest) (IncidentResult, error) {
	result, err := d.dispatch(ctx, request)
	usage, usageErr := d.ResponderUsage(request.IncidentID)
	if usageErr != nil {
		return result, fmt.Errorf("recover responder usage: %w", usageErr)
	}
	if usage != nil {
		result.Usage = *usage
	}
	return result, err
}

func (d KubernetesJobDispatcher) dispatch(ctx context.Context, request IncidentRequest) (IncidentResult, error) {
	if d.Registry == nil || d.AgentID == "" {
		return IncidentResult{}, errors.New("incident dispatch requires an agent registry and responder agent ID")
	}
	if err := validateIncidentRequest(request); err != nil {
		return IncidentResult{}, fmt.Errorf("validate incident request: %w", err)
	}
	if d.AgentID == "" {
		if d.Registry == nil {
			return IncidentResult{}, errors.New("incident dispatcher requires an agent registry")
		}
		var err error
		d.AgentID, err = d.Registry.AgentFor(&contracts.IncidentRequest{})
		if err != nil {
			return IncidentResult{}, err
		}
	}
	if request.PreparedAgentTask == "" {
		var err error
		request, err = d.PrepareDispatch(ctx, request)
		if err != nil {
			return IncidentResult{}, err
		}
	}
	task := new(contracts.AgentTask)
	var err error
	if err := protojson.Unmarshal([]byte(request.PreparedAgentTask), task); err != nil {
		return IncidentResult{}, fmt.Errorf("decode prepared agent task: %w", err)
	}
	if task.GetTaskId() != request.IncidentID || task.GetWorkspace().GetCheckoutPath() != request.RepositoryWorktree ||
		task.GetWorkspace().GetBaseCommit() != request.RepositoryBaseCommit {
		return IncidentResult{}, errors.New("prepared agent task differs from pending incident")
	}
	var loop *ResponderLoop
	if d.LoopFactory != nil {
		loop = d.LoopFactory(request)
		if loop != nil {
			defer loop.CloseWorkingPrograms()
		}
		if loop != nil {
			loop.initializeDispatch(d.programCounts(ctx), request.MaxResponseLoops, request.InitialProgramRuns)
			if err := d.restoreToolState(ctx, request, loop); err != nil {
				return IncidentResult{}, fmt.Errorf("restore responder tool state: %w", err)
			}
		}
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
		rejoin := false
		rejoin, err = (worker_registry.KubernetesExecutor{RepositoryMountPath: d.RepositoryMountPath}).HasBoundStandby(task)
		if err != nil {
			return IncidentResult{}, err
		}
		if rejoin {
			sessionSubPath, err = memory.PrepareIncidentSession(d.RepositoryMountPath, d.RepositoryPVCSubPath, request.IncidentID)
		} else {
			sessionSubPath, err = memory.PrepareIncidentAPIAttempt(d.RepositoryMountPath, d.RepositoryPVCSubPath, request.IncidentID)
		}
		if err != nil {
			return IncidentResult{}, fmt.Errorf("prepare isolated responder session: %w", err)
		}
		environment["CODEX_HOME"] = "/sdo/session/codex"
		environment["CLAUDE_CONFIG_DIR"] = "/sdo/session/claude"
		environment["SDO_SESSION_SCRATCH"] = "/sdo/session/scratch"
		environment["SDO_RESPONDER_USAGE_DIR"] = "/sdo/session/usage"
		environment[latefindings.ReceiptsPathEnv] = latefindings.DefaultReceiptsPath
		store := latefindings.Store{Client: d.Client, Namespace: d.Namespace}
		if err := store.Ensure(ctx, request.IncidentID); err != nil {
			return IncidentResult{}, fmt.Errorf("prepare logical late findings transport: %w", err)
		}
		lateConfigMap = latefindings.ConfigMapName(request.IncidentID)
	}
	executor := worker_registry.KubernetesExecutor{
		APIProxyImage:  d.APIProxyImage,
		ToolchainImage: d.ToolchainImage,
		Client:         d.Client, Namespace: d.Namespace, Registry: d.Registry,
		ServiceAccount: d.ServiceAccount, RepositoryPVC: d.RepositoryPVC,
		RepositoryMountPath: d.RepositoryMountPath, RepositoryPVCSubPath: d.RepositoryPVCSubPath,
		CredentialsSecret: d.CredentialsSecret, Environment: environment, PollInterval: d.PollInterval, SessionPVCSubPath: sessionSubPath,
		CapturedCorpusReadOnly: d.RequireMemorySnapshot,
		LateFindingsConfigMap:  lateConfigMap,
		TaskPayload:            request.PreparedAgentTask,
	}
	if d.Standby != nil {
		executor, err = d.Standby.TryBind(ctx, executor, task)
		if err != nil {
			return IncidentResult{}, fmt.Errorf("bind standby responder: %w", err)
		}
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
		var tools sync.WaitGroup
		// Dispatch owns these goroutines: they write corpus captures and tool
		// responses under the repository mount, so none may outlive it.
		defer func() { stopTools(); tools.Wait() }()
		server := SessionToolServer{Directory: directory, Handle: func(toolCtx context.Context, toolRequest ToolRequest) ToolResponse {
			received := time.Now().UTC()
			response := loop.Handle(toolCtx, toolRequest)
			if toolRequest.Command == "session-open" && response.Error == "" {
				if err := worker_registry.RecordSessionStartup(d.RepositoryMountPath, d.RepositoryPVCSubPath, sessionSubPath, received, time.Now().UTC()); err != nil {
					slog.Warn("responder_startup_session_record", "incident_id", request.IncidentID, "error", err)
				}
			}
			return response
		}}
		if err := server.Prepare(); err != nil {
			return IncidentResult{}, fmt.Errorf("prepare responder tools: %w", err)
		}
		tools.Add(1)
		go func() { defer tools.Done(); _ = server.Serve(toolCtx) }()
		environment["SDO_TOOLS_DIR"] = "/sdo/session/tools"
		environment["SDO_RESPONDER_SESSION"] = "1"
		if loop.MenuConfig.valid() {
			environment["SDO_MITIGATION_SESSION"] = "1"
		}
	}

	result, err := d.executeResponder(ctx, executor, task, loop, request)
	if err != nil {
		var failed *worker_registry.JobFailedError
		if errors.As(err, &failed) {
			result := IncidentResult{SchemaVersion: ProtocolSchemaVersion, IncidentID: request.IncidentID, Status: IncidentFailed, Error: failed.Error()}
			if loop != nil {
				loop.locked(func() { result.ResponderSessionID = loop.menuSession.GetProviderSessionId() })
			}
			return result, &ResponderJobFailedError{JobName: failed.JobName}
		}
		return IncidentResult{}, err
	}
	if loop != nil {
		result.ProgramToolCalls = loop.ToolCalls()
		result.ProgramRuns = loop.ProgramRuns()
		fillProgramRevisions(ctx, result.ProgramRuns, request.RepositoryWorktree, request.SourceCommit)
		result.PlanApplications = loop.PlanApplications()
		result.RepairActions = loop.ApplicationRepairReceipts(result.RepairActions)
		if result.Status == IncidentCompleted {
			loop.CreditEquivalentRepairs(ctx, result.RepairActions)
			result.PlanApplications = loop.PlanApplications()
			result.ProgramToolCalls = loop.ToolCalls()
			result.ProgramRuns = loop.ProgramRuns()
			fillProgramRevisions(ctx, result.ProgramRuns, request.RepositoryWorktree, request.SourceCommit)
		}
		result.RepairActions = append(result.RepairActions, loop.MenuRepairActions()...)
		result.ProgramFailureSignals = loop.FailureSignals()
		attempts := loop.continuationAttempts()
		result.ResponseLoopsUsed = attempts
		result.ResolutionPath = "responder"
		result.ResponseLoopExhausted = result.ResponseLoopExhausted || (loop.Exhausted() && result.Status != IncidentCompleted)
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

package runtime

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sdo.dev/operator/contracts/wire"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
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
	ProgramFastPath       bool
	ProgramTrustThreshold int
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
			request.MaxResponseLoops = int(typed.GetMaxResponseLoops())
			request.PreparedAgentTask = payload
			return request, nil
		}
		if !apierrors.IsNotFound(err) {
			return IncidentRequest{}, fmt.Errorf("get existing agent request ConfigMap: %w", err)
		}
	}
	if d.LoopFactory != nil {
		if loop := d.LoopFactory(request); loop != nil {
			trust, err := d.programTrust(ctx, request)
			if err != nil {
				return IncidentRequest{}, err
			}
			loop.Trust = trust
			request.InitialProgramRuns = loop.InitialDiagnose(ctx)
			fillProgramRevisions(ctx, request.InitialProgramRuns, request.RepositoryWorktree, request.SourceCommit)
			request.MaxResponseLoops = loop.MaxBatches
		}
	}
	var snapshots []*contracts.OperationalMemorySnapshot
	if d.RequireMemorySnapshot && d.Catalog == nil {
		return IncidentRequest{}, errors.New("native responder requires logical operational memory")
	}
	if d.Catalog != nil {
		snapshot, err := d.Catalog.Snapshot(ctx)
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

func (d KubernetesJobDispatcher) Dispatch(ctx context.Context, request IncidentRequest) (IncidentResult, error) {
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
			trust, err := d.programTrust(ctx, request)
			if err != nil {
				return IncidentResult{}, err
			}
			loop.Trust = trust
			if request.MaxResponseLoops > 0 {
				loop.MaxBatches = request.MaxResponseLoops
			}
			loop.runs = append([]ProgramRun(nil), request.InitialProgramRuns...)
			if err := d.restoreToolState(ctx, request, loop); err != nil {
				return IncidentResult{}, fmt.Errorf("restore responder tool state: %w", err)
			}
		}
	}
	if err := d.Registry.ValidateTask(task); err != nil {
		return IncidentResult{}, fmt.Errorf("validate agent task: %w", err)
	}
	if d.ProgramFastPath && loop != nil {
		fastResult, summary, resolved, err := d.tryFastPath(ctx, request, loop)
		if err != nil {
			return IncidentResult{}, err
		}
		if resolved {
			return fastResult, nil
		}
		if summary != "" {
			request.FastPathSummary = summary
			prepared := new(contracts.IncidentRequest)
			if err := task.GetRequest().UnmarshalTo(prepared); err != nil {
				return IncidentResult{}, fmt.Errorf("decode prepared incident for fast-path handoff: %w", err)
			}
			prepared.FastPathSummary = summary
			task.Request, err = anypb.New(prepared)
			if err != nil {
				return IncidentResult{}, err
			}
			payload, err := wire.Canonical(task)
			if err != nil {
				return IncidentResult{}, err
			}
			request.PreparedAgentTask = string(payload)
			task = new(contracts.AgentTask)
			if err := protojson.Unmarshal(payload, task); err != nil {
				return IncidentResult{}, fmt.Errorf("decode fast-path handoff task: %w", err)
			}
			if err := d.Registry.ValidateTask(task); err != nil {
				return IncidentResult{}, fmt.Errorf("validate fast-path handoff task: %w", err)
			}
		}
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
		sessionSubPath, err = memory.PrepareIncidentAPIAttempt(d.RepositoryMountPath, d.RepositoryPVCSubPath, request.IncidentID)
		if err != nil {
			return IncidentResult{}, fmt.Errorf("prepare isolated responder session: %w", err)
		}
		environment["CODEX_HOME"] = "/sdo/session/codex"
		environment["CLAUDE_CONFIG_DIR"] = "/sdo/session/claude"
		environment["SDO_SESSION_SCRATCH"] = "/sdo/session/scratch"
		environment[latefindings.ReceiptsPathEnv] = latefindings.DefaultReceiptsPath
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
		var tools sync.WaitGroup
		// Dispatch owns these goroutines: they write corpus captures and tool
		// responses under the repository mount, so none may outlive it.
		defer func() { stopTools(); tools.Wait() }()
		server := SessionToolServer{Directory: directory, Handle: loop.Handle}
		if err := server.Prepare(); err != nil {
			return IncidentResult{}, fmt.Errorf("prepare responder tools: %w", err)
		}
		tools.Add(1)
		go func() { defer tools.Done(); _ = server.Serve(toolCtx) }()
		environment["SDO_TOOLS_DIR"] = "/sdo/session/tools"
		environment["SDO_MITIGATION_SESSION"] = "1"
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
	result, err := d.executeResponder(ctx, executor, task, loop, request)
	if err != nil {
		var failed *worker_registry.JobFailedError
		if errors.As(err, &failed) {
			return IncidentResult{}, &ResponderJobFailedError{JobName: failed.JobName}
		}
		return IncidentResult{}, err
	}
	if loop != nil {
		result.ProgramToolCalls = loop.ToolCalls()
		result.ProgramRuns = loop.ProgramRuns()
		fillProgramRevisions(ctx, result.ProgramRuns, request.RepositoryWorktree, request.SourceCommit)
		result.PlanApplications = loop.PlanApplications()
		result.RepairActions = ProgramRepairReceipts(result.PlanApplications, result.RepairActions)
		if result.Status == IncidentCompleted {
			loop.CreditEquivalentRepairs(ctx, result.RepairActions)
			result.PlanApplications = loop.PlanApplications()
			result.ProgramToolCalls = loop.ToolCalls()
			result.ProgramRuns = loop.ProgramRuns()
			fillProgramRevisions(ctx, result.ProgramRuns, request.RepositoryWorktree, request.SourceCommit)
		}
		result.RepairActions = append(result.RepairActions, loop.MenuRepairActions()...)
		result.ProgramFailureSignals = loop.FailureSignals()
		loop.mu.Lock()
		attempts := loop.continuation.Attempts
		loop.mu.Unlock()
		result.ResponseLoopsUsed = max(loop.LoopsUsed(), attempts)
		result.ResolutionPath = "responder"
		result.FastPathPrograms = append([]string(nil), loop.fastPathPrograms...)
		result.FastPathSkippedReason = loop.fastPathSkippedReason
		result.FastPathRelationCoveredFindings = append([]RelationCoveredFinding(nil), loop.fastPathRelationCoveredFindings...)
		result.ResponseLoopExhausted = result.ResponseLoopExhausted || loop.Exhausted()
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

func (d KubernetesJobDispatcher) programTrust(ctx context.Context, request IncidentRequest) (map[string]ProgramTrust, error) {
	if d.Outcomes == nil {
		return nil, nil
	}
	stored, err := d.Outcomes.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("read program trust outcomes: %w", err)
	}
	stored, err = memory.AfterAdoption(ctx, request.RepositoryWorktree, stored)
	if err != nil {
		return nil, fmt.Errorf("read adoption learning boundary: %w", err)
	}
	records := make([]*contracts.OutcomeRecord, 0, len(stored))
	for _, outcome := range stored {
		if outcome.Record != nil {
			records = append(records, outcome.Record)
		}
	}
	return ProgramTrustStats(records, d.ProgramTrustThreshold), nil
}

// ResponderJobFailedError preserves the controller's terminal dispatch signal.
type ResponderJobFailedError struct{ JobName string }

func (err *ResponderJobFailedError) Error() string {
	return fmt.Sprintf("responder Job %q failed", err.JobName)
}

func IncidentJobName(incidentID string) string { return worker_registry.JobName(incidentID) }

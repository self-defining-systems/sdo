package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"strings"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
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

	Standby *worker_registry.StandbyPool
	// AcceptedRepository is the durable broker-owned checkout used for trust.
	// Proposal checkouts belong to individual incidents and may be retired.
	AcceptedRepository    string
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
	beforeClaim           func()
}

// PrepareDispatch freezes the worker task before the controller records its
// pending dispatch. Retries use this task even if observations later change.
func (d KubernetesJobDispatcher) PrepareDispatch(ctx context.Context, request IncidentRequest) (IncidentRequest, error) {
	defer dispatchPreparationTiming(request.IncidentID, "prepare_task")()
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
			if payload == "" || wire.UnmarshalDurable([]byte(payload), task) != nil ||
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
			request.PriorStages = cloneStageReports(typed.GetPriorStages())
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
			// Preparation also registers this loop with the controller. Preserve
			// accepted automatic-stage evidence for closure before any worker is
			// published; task preparation must not replace it with an empty loop.
			if err := d.restoreToolState(ctx, request, loop); err != nil {
				return IncidentRequest{}, err
			}
			if err := d.reopenWithdrawnRecovery(ctx, request, loop); err != nil {
				return IncidentRequest{}, err
			}
			trust, credits, err := d.programLearning(ctx, request)
			if err != nil {
				return IncidentRequest{}, err
			}
			loop.setCredits(credits)
			request = loop.prepareDispatch(ctx, request, trust, coverage)
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
		finished := dispatchPreparationTiming(request.IncidentID, "memory_snapshot")
		snapshot, err := memory.SnapshotAt(ctx, d.Catalog, request.RepositoryBaseCommit)
		finished()
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
func (l *ResponderLoop) prepareDispatch(ctx context.Context, request IncidentRequest, trust map[string]ProgramTrust, coverage CoverageProvider) IncidentRequest {
	l.operationMu.Lock()
	defer l.operationMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.Trust = trust
	if request.ExternalMessage != "" {
		request.MaxResponseLoops = l.MaxBatches
		return request
	}
	l.capturePhase = "open@detect"
	defer func() { l.capturePhase = "" }()
	request.InitialProgramRuns = l.diagnose(ctx, l.findings, false)
	fillProgramRevisions(ctx, request.InitialProgramRuns, request.RepositoryWorktree, request.SourceCommit)
	request.PlaybookCoverage = coverage.Coverage(request.Findings, request.InitialProgramRuns, l.lastDiagnosisView, trust)
	request.MaxResponseLoops = l.MaxBatches
	return request
}

// DispatchWithClaim records invocation only after recovery admission, immediately
// before a first worker claim or an existing execution is rejoined.
func (d KubernetesJobDispatcher) DispatchWithClaim(ctx context.Context, request IncidentRequest, claimed func()) (IncidentResult, error) {
	d.beforeClaim = claimed
	return d.Dispatch(ctx, request)
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
	if err := wire.UnmarshalDurable([]byte(request.PreparedAgentTask), task); err != nil {
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
			trust, credits, err := d.programLearning(ctx, request)
			if err != nil {
				return IncidentResult{}, err
			}
			loop.setCredits(credits)
			loop.initializeDispatch(trust, request.MaxResponseLoops, request.InitialProgramRuns)
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
		environment["SDO_SESSION_SCRATCH"] = "/sdo/session/scratch"
		environment["SDO_RESPONDER_USAGE_DIR"] = "/sdo/session/usage"
	}
	executor := worker_registry.KubernetesExecutor{
		APIProxyImage:  d.APIProxyImage,
		ToolchainImage: d.ToolchainImage,
		Client:         d.Client, Namespace: d.Namespace, Registry: d.Registry,
		ServiceAccount: d.ServiceAccount, RepositoryPVC: d.RepositoryPVC,
		RepositoryMountPath: d.RepositoryMountPath, RepositoryPVCSubPath: d.RepositoryPVCSubPath,
		CredentialsSecret: d.CredentialsSecret, Environment: environment, PollInterval: d.PollInterval, SessionPVCSubPath: sessionSubPath,
		CapturedCorpusReadOnly: d.RequireMemorySnapshot,
		TaskPayload:            request.PreparedAgentTask,
	}
	// Last observation before any standby binding or cold worker publication.
	// This covers prepared-task rejoin and controller verification follow-ups
	// through the same recovery decision as the ordinary response stage.
	recovery, recoveryErr := d.beforeFirstResponderClaim(ctx, request, task, loop)
	if recoveryErr != nil {
		return IncidentResult{}, recoveryErr
	}
	if recovery != nil {
		request.PriorStages = recovery.PriorStages
		request.Findings = recovery.Findings
		if deterministicStageReport(recovery.PriorStages).GetRecoveryWait().GetExitReason() == contracts.RecoveryWait_EXIT_REASON_CLEARED {
			return automaticClaimResult(recovery)
		}
	}
	if d.beforeClaim != nil {
		d.beforeClaim()
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
			received := lifecycleio.Now().UTC()
			response := loop.Handle(toolCtx, toolRequest)
			if toolRequest.Command == "session-open" && response.Error == "" {
				if err := worker_registry.RecordSessionStartup(d.RepositoryMountPath, d.RepositoryPVCSubPath, sessionSubPath, received, lifecycleio.Now().UTC()); err != nil {
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
			return result, &ResponderJobFailedError{JobName: failed.JobName, Cause: failed}
		}
		return IncidentResult{}, err
	}
	if loop != nil {
		result.ProgramToolCalls = loop.ToolCalls()
		result.ProgramRuns = loop.ProgramRuns()
		fillProgramRevisions(ctx, result.ProgramRuns, request.RepositoryWorktree, request.SourceCommit)
		result.PlanApplications = loop.ApplicationEvidence()
		result.RepairActions = loop.ApplicationRepairReceipts(result.RepairActions)
		if result.Status == IncidentCompleted {
			loop.CreditEquivalentRepairs(ctx, result.RepairActions)
			result.PlanApplications = loop.ApplicationEvidence()
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
	if err := validateIncidentResultFor(result, request); err != nil {
		return IncidentResult{}, fmt.Errorf("validate incident result: %w", err)
	}
	if recovery != nil {
		result.automaticStages = cloneStageReports(recovery.PriorStages)
	}
	return result, nil
}

func (d KubernetesJobDispatcher) programTrust(ctx context.Context, request IncidentRequest) (map[string]ProgramTrust, error) {
	trust, _, err := d.programLearning(ctx, request)
	return trust, err
}

// programLearning derives, from one read of the durable outcomes, what each
// learned playbook has earned: trust counters and the credited (symptom,
// component) pairs its key is built from.
func (d KubernetesJobDispatcher) programLearning(ctx context.Context, request IncidentRequest) (map[string]ProgramTrust, ProgramCredits, error) {
	// External alerts deliberately carry no Go detector/coverage authority.
	// Trust is only diagnostic metadata for that coverage, not an execution gate.
	// Do not walk all outcomes and executable corpus history twice before
	// claiming a worker that cannot consume that evidence.
	if request.ExternalMessage != "" || d.Outcomes == nil {
		return nil, nil, nil
	}
	defer dispatchPreparationTiming(request.IncidentID, "program_trust")()
	stored, _, err := memory.ListOutcomes(ctx, d.Outcomes, "program trust")
	if err != nil {
		return nil, nil, fmt.Errorf("read program trust outcomes: %w", err)
	}
	if d.AcceptedRepository != "" {
		request.RepositoryWorktree = d.AcceptedRepository
	}
	stored, err = memory.AfterAdoption(ctx, request.RepositoryWorktree, stored)
	if err != nil {
		return nil, nil, fmt.Errorf("read adoption learning boundary: %w", err)
	}
	records := make([]*contracts.OutcomeRecord, 0, len(stored))
	for _, outcome := range stored {
		if outcome.Record != nil {
			records = append(records, outcome.Record)
		}
	}
	stats := ProgramTrustStats(records)
	if request.RepositoryWorktree != "" && gitRevision.MatchString(request.SourceCommit) {
		if err := creditLearnedPrograms(ctx, request, records, stats); err != nil {
			// Origin credit is optional. Keep ordinary settled trust when the
			// extra Git evidence is unavailable; never invent origin credit.
			slog.WarnContext(ctx, "optional program learning credit unavailable", "incident_id", request.IncidentID, "error", err)
		}
	}
	return stats, ProgramCreditIndex(records), nil
}

// ResponderJobFailedError preserves the controller's terminal dispatch signal.
type ResponderJobFailedError struct {
	JobName string
	Cause   error
}

func (err *ResponderJobFailedError) Error() string {
	if err.Cause != nil {
		return fmt.Sprintf("responder Job %q failed: %v", err.JobName, err.Cause)
	}
	return fmt.Sprintf("responder Job %q failed", err.JobName)
}

func (err *ResponderJobFailedError) Unwrap() error { return err.Cause }

func IncidentJobName(incidentID string) string { return worker_registry.JobName(incidentID) }

func (d KubernetesJobDispatcher) reopenWithdrawnRecovery(ctx context.Context, request IncidentRequest, loop *ResponderLoop) error {
	if loop.MutationGate == nil {
		return nil
	}
	verified, err := loop.MutationGate.Verified()
	if err != nil {
		return fmt.Errorf("read recovery fence: %w", err)
	}
	if verified == nil {
		return nil
	}
	if loop.IncidentStatus == nil || loop.CurrentEvidence == nil {
		return errors.New("reopening recovery requires the controller's current incident evidence")
	}
	current := loop.CurrentEvidence()
	if loop.IncidentStatus() != nil || current == nil || current.IncidentID != request.IncidentID {
		return errors.New("recovery remains fenced by the controller")
	}
	if d.Client == nil {
		return errors.New("reopening recovery requires independently observed responder quiescence")
	}
	work, err := d.IncidentWork(ctx, request.IncidentID)
	if err != nil {
		return fmt.Errorf("observe responder before reopening recovery: %w", err)
	}
	if work.Active {
		return errors.New("responder work must be quiescent before reopening recovery")
	}
	if err := loop.MutationGate.Reopen(); err != nil {
		return fmt.Errorf("reopen withdrawn recovery fence: %w", err)
	}
	return nil
}

// Keep preparation visible separately from worker startup and model time.
func dispatchPreparationTiming(incident, phase string) func() {
	started := lifecycleio.Now()
	return func() {
		slog.Info("responder_dispatch_preparation", "incident_id", incident, "phase", phase, "duration_ms", lifecycleio.Since(started).Milliseconds())
	}
}

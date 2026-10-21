package runtime

import (
	"fmt"
	"strings"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

const ProtocolSchemaVersion = "sdo.dev/v1alpha1"

type DetectorEvaluationStatus string

const (
	DetectorEvaluationFiring DetectorEvaluationStatus = "firing"
	DetectorEvaluationClear  DetectorEvaluationStatus = "clear"
	DetectorEvaluationError  DetectorEvaluationStatus = "error"
)

type IncidentStatus string

const (
	IncidentCompleted IncidentStatus = "completed"
	IncidentFailed    IncidentStatus = "failed"
	IncidentCancelled IncidentStatus = "cancelled"
)

type DetectorEvaluation struct {
	DetectorID   string                   `json:"detector_id"`
	EvaluatedAt  time.Time                `json:"evaluated_at"`
	Status       DetectorEvaluationStatus `json:"status"`
	Fingerprints []string                 `json:"fingerprints"`
	Error        string                   `json:"error,omitempty"`
}

type SurfacedPlaybook struct {
	PlaybookID        string                   `json:"playbook_id"`
	ParameterBindings map[string]sdk.ObjectRef `json:"parameter_bindings,omitempty"`
}

type PriorOutcomeEvidence struct {
	IncidentID            string   `json:"incident_id"`
	MatchReason           string   `json:"match_reason"`
	RootCauseSummaries    []string `json:"root_cause_summaries"`
	RepairActionSummaries []string `json:"repair_action_summaries"`
	AppliedPlaybookIDs    []string `json:"applied_playbook_ids"`
	SourceCommit          string   `json:"source_commit"`
	ExactSourceMatch      bool     `json:"exact_source_match"`
}

type IncidentRequest struct {
	SchemaVersion        string                 `json:"schema_version"`
	Application          string                 `json:"application"`
	Namespace            string                 `json:"namespace"`
	IncidentID           string                 `json:"incident_id"`
	Findings             []sdk.Finding          `json:"findings"`
	DriftEvents          []sdk.DriftEvent       `json:"drift_events,omitempty"`
	DetectorHistory      []DetectorEvaluation   `json:"detector_history"`
	SurfacedPlaybooks    []SurfacedPlaybook     `json:"surfaced_playbooks"`
	RelevantOutcomes     []PriorOutcomeEvidence `json:"relevant_outcomes"`
	SourceCommit         string                 `json:"source_commit"`
	DeployedCommit       string                 `json:"deployed_commit"`
	RepositoryWorktree   string                 `json:"repository_worktree"`
	RepositoryBaseCommit string                 `json:"repository_base_commit"`
	ResponseDeadline     time.Time              `json:"response_deadline"`
	CancellationToken    string                 `json:"cancellation_token"`
	RepairPolicy         string                 `json:"repair_policy"`
	// StateChanges is the application's configuration diff against its last
	// healthy baseline when the incident opened; nil without a baseline.
	StateChanges *StateChanges `json:"state_changes,omitempty"`
	// FollowUp is set only on a follow-up incident dispatched for findings that
	// stayed active after an earlier responder completed.
	FollowUp           *FollowUpContext `json:"follow_up,omitempty"`
	InitialProgramRuns []ProgramRun     `json:"initial_program_runs,omitempty"`
	FastPathSummary    string           `json:"fast_path_summary,omitempty"`
	MaxResponseLoops   int              `json:"max_response_loops,omitempty"`
	// PreparedAgentTask is the exact ProtoJSON task persisted before a Job may be created.
	PreparedAgentTask string `json:"prepared_agent_task,omitempty"`
}

// FollowUpContext links a follow-up responder request to the incident it
// continues. The request's Findings are the residual findings; PriorSummary
// is a bounded, factual digest of what the earlier responders reported.
type FollowUpContext struct {
	OriginalIncidentID string `json:"original_incident_id"`
	ParentIncidentID   string `json:"parent_incident_id"`
	// Attempt is the 1-based follow-up ordinal; MaxFollowUps is the bound.
	Attempt      int    `json:"attempt"`
	MaxFollowUps int    `json:"max_follow_ups"`
	PriorSummary string `json:"prior_responder_summary"`
}

// RootCauseEvidence is one live observation supporting a root cause: a
// detector finding, a synthetic scenario, a state change, or a command and
// its output. Static artifacts are only context.
type RootCauseEvidence struct {
	Kind        string `json:"kind"`
	Source      string `json:"source"`
	Observation string `json:"observation"`
}

type ConfirmedRootCause struct {
	Summary   string          `json:"summary"`
	Resources []sdk.ObjectRef `json:"resources"`
	// Evidence is empty only in results predating evidence-bearing diagnoses.
	Evidence []RootCauseEvidence `json:"evidence,omitempty"`
	// ExplainedDetectors must clear after the fix.
	ExplainedDetectors []string `json:"explained_detectors,omitempty"`
	StaticContext      []string `json:"static_context,omitempty"`
}

type AppliedPlaybook struct {
	PlaybookID        string                   `json:"playbook_id"`
	ParameterBindings map[string]sdk.ObjectRef `json:"parameter_bindings"`
	ScriptNames       []string                 `json:"script_names"`
}

type VerificationEvidence struct {
	Name       string    `json:"name"`
	Passed     bool      `json:"passed"`
	Details    string    `json:"details"`
	ObservedAt time.Time `json:"observed_at"`
}

// UsageMetrics is the shared protobuf message even in controller state, so
// newly reported accounting fields survive dispatch and outcome derivation.
type UsageMetrics = contracts.UsageMetrics

type TimingMetrics struct {
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
}

type RepairActionReceipt struct {
	ActionID string `json:"action_id"`
	Kind     string `json:"kind"`
	Target   string `json:"target"`
	// Resources are the objects the action mutated; diagnosis verification
	// credits a root cause only to a repair that touched its resources.
	Resources   []sdk.ObjectRef `json:"resources,omitempty"`
	Summary     string          `json:"summary"`
	Details     string          `json:"details"`
	StartedAt   time.Time       `json:"started_at"`
	CompletedAt time.Time       `json:"completed_at"`
	Success     bool            `json:"success"`
	Reversible  bool            `json:"reversible"`
}

type IncidentResult struct {
	SchemaVersion         string                    `json:"schema_version"`
	IncidentID            string                    `json:"incident_id"`
	Status                IncidentStatus            `json:"status"`
	ConfirmedRootCauses   []ConfirmedRootCause      `json:"confirmed_root_causes"`
	AppliedPlaybooks      []AppliedPlaybook         `json:"applied_playbooks"`
	RepairChanges         []string                  `json:"repair_changes"`
	RepairActions         []RepairActionReceipt     `json:"repair_actions"`
	FinalDetectorStates   []DetectorEvaluation      `json:"final_detector_states"`
	ProposedArtifactIDs   []string                  `json:"proposed_artifact_ids"`
	VerificationEvidence  []VerificationEvidence    `json:"verification_evidence"`
	Usage                 UsageMetrics              `json:"usage"`
	Timing                TimingMetrics             `json:"timing"`
	ResponderSessionID    string                    `json:"responder_session_id,omitempty"`
	LateFindingsPull      *LateFindingsPull         `json:"late_findings_pull,omitempty"`
	Error                 string                    `json:"error,omitempty"`
	ProgramToolCalls      []ToolCall                `json:"program_tool_calls,omitempty"`
	ProgramRuns           []ProgramRun              `json:"program_runs,omitempty"`
	PlanApplications      []RecordedPlanApplication `json:"plan_applications,omitempty"`
	ProgramFailureSignals []string                  `json:"program_failure_signals,omitempty"`
	ResponseLoopsUsed     int                       `json:"response_loops_used,omitempty"`
	ResponseLoopExhausted bool                      `json:"response_loop_exhausted,omitempty"`
	ResolutionPath        string                    `json:"resolution_path,omitempty"`
	FastPathPrograms      []string                  `json:"fast_path_programs,omitempty"`
}

// LateFindingsPull is a worker's claim that it consumed a controller-authored
// logical telemetry report. The dispatcher checks its digest and IDs against
// the controller's report history before accepting the result.
type LateFindingsPull struct {
	IncidentID   string   `json:"incident_id"`
	PullCount    uint32   `json:"pull_count"`
	DetectorIDs  []string `json:"detector_ids"`
	PlaybookIDs  []string `json:"playbook_ids"`
	ReportDigest string   `json:"report_digest"`
}

// ObservedStateChange is one object the controller saw changed from the
// healthy baseline while an incident was open, and when it first saw it.
type ObservedStateChange struct {
	Kind            string    `json:"kind"`
	Name            string    `json:"name"`
	FirstObservedAt time.Time `json:"first_observed_at"`
}

type IncidentClosure struct {
	Request             IncidentRequest      `json:"request"`
	Result              *IncidentResult      `json:"result,omitempty"`
	DispatchError       string               `json:"dispatch_error,omitempty"`
	FinalDetectorStates []DetectorEvaluation `json:"final_detector_states"`
	// IncidentDetectorStates is the latest post-response evaluation of each
	// non-health detector that raised a finding in this incident. It is
	// learning evidence for the broker, not a closure gate.
	IncidentDetectorStates []DetectorEvaluation `json:"incident_detector_states,omitempty"`
	// DetectorTimeline summarizes, per finding, when each detector fired in this
	// incident window and how that relates to dispatch. The booleans derive
	// from it for analysis: a learned incident detector that fired before the
	// responder was dispatched, one that fired only while it ran, or only
	// health detectors firing.
	DetectorTimeline                    []DetectorTimelineEntry `json:"detector_timeline,omitempty"`
	IncidentDetectorFiredBeforeDispatch bool                    `json:"incident_detector_fired_before_dispatch"`
	IncidentDetectorFiredAfterDispatch  bool                    `json:"incident_detector_fired_after_dispatch"`
	NoIncidentDetectorFired             bool                    `json:"no_incident_detector_fired"`
	// FinalStateChanges is the configuration diff against the healthy
	// baseline at verification time: a composite fault's later
	// component can land a few seconds after dispatch, while the incident is
	// still open, so it is missing from Request.StateChanges but present
	// here. nil without a baseline. Computed from the in-memory informer
	// cache, so it adds no wall-clock time to closure.
	FinalStateChanges    *StateChanges `json:"final_state_changes,omitempty"`
	DetectedAt           time.Time     `json:"detected_at"`
	DispatchedAt         time.Time     `json:"dispatched_at"`
	ResponderCompletedAt time.Time     `json:"responder_completed_at"`
	VerifiedAt           time.Time     `json:"verified_at"`
	// HealthClearedAt is when the closure gate's detectors began their final
	// streak of clear evaluations. Only a repair action that started by then
	// can back a root cause. nil when no streak is on record.
	HealthClearedAt *time.Time `json:"health_cleared_at,omitempty"`
	// ObservedStateChanges is every object the controller saw differ from
	// the healthy baseline while the incident was open, with its first
	// observation: the dispatch diff, later evaluations' diffs, and the
	// closing view. A state-change citation is evidence of the cause only if
	// it was observed before the responder's own repair of that object
	// started; otherwise it is that repair's edit. Omitted without a
	// baseline, when state changes cannot be checked at all.
	ObservedStateChanges []ObservedStateChange `json:"observed_state_changes,omitempty"`
	// DetectorReviewRequiredAt is set when health did not clear within the
	// verification window after the responder completed. Health that
	// clears later was not verifiably restored by the responder.
	DetectorReviewRequiredAt *time.Time `json:"detector_review_required_at,omitempty"`
	DetectorReviewReason     string     `json:"detector_review_reason,omitempty"`
	// CleanedHelpers are the responder helper objects the controller
	// deleted after the responder completed, as Kind/namespace/name.
	CleanedHelpers []string `json:"cleaned_helpers,omitempty"`
}

func (request IncidentRequest) Validate() error {
	if request.SchemaVersion != ProtocolSchemaVersion {
		return fmt.Errorf("unsupported schema version %q", request.SchemaVersion)
	}
	if strings.TrimSpace(request.Application) == "" || strings.TrimSpace(request.Namespace) == "" {
		return fmt.Errorf("application and namespace are required")
	}
	if strings.TrimSpace(request.IncidentID) == "" {
		return fmt.Errorf("incident id is required")
	}
	if len(request.Findings) == 0 || len(request.DetectorHistory) == 0 {
		return fmt.Errorf("findings and detector history are required")
	}
	if request.ResponseDeadline.IsZero() || strings.TrimSpace(request.CancellationToken) == "" {
		return fmt.Errorf("response deadline and cancellation token are required")
	}
	if strings.TrimSpace(request.RepositoryWorktree) == "" || strings.TrimSpace(request.RepositoryBaseCommit) == "" {
		return fmt.Errorf("repository worktree and base commit are required")
	}
	if request.RepairPolicy != "commit" && request.RepairPolicy != "recorded-actions" {
		return fmt.Errorf("unsupported repair policy %q", request.RepairPolicy)
	}
	return nil
}

func (result IncidentResult) ValidateFor(request IncidentRequest) error {
	if result.SchemaVersion != ProtocolSchemaVersion {
		return fmt.Errorf("unsupported schema version %q", result.SchemaVersion)
	}
	if result.IncidentID != request.IncidentID {
		return fmt.Errorf("incident id %q does not match request %q", result.IncidentID, request.IncidentID)
	}
	if result.Status != IncidentCompleted && result.Status != IncidentFailed && result.Status != IncidentCancelled {
		return fmt.Errorf("unsupported incident status %q", result.Status)
	}
	if pull := result.LateFindingsPull; pull != nil && (pull.IncidentID != request.IncidentID || pull.PullCount == 0 || len(pull.ReportDigest) != 64) {
		return fmt.Errorf("late findings pull has invalid incident identity or digest")
	}
	if result.Timing.StartedAt.IsZero() || result.Timing.CompletedAt.IsZero() {
		return fmt.Errorf("timing is required")
	}
	if result.Timing.CompletedAt.Before(result.Timing.StartedAt) {
		return fmt.Errorf("completion time is before start time")
	}
	actionIDs := make(map[string]struct{}, len(result.RepairActions))
	for _, action := range result.RepairActions {
		if strings.TrimSpace(action.ActionID) == "" || strings.TrimSpace(action.Kind) == "" ||
			strings.TrimSpace(action.Target) == "" || strings.TrimSpace(action.Summary) == "" ||
			strings.TrimSpace(action.Details) == "" {
			return fmt.Errorf("repair action identity, kind, target, summary, and details are required")
		}
		if action.StartedAt.IsZero() || action.CompletedAt.IsZero() || action.CompletedAt.Before(action.StartedAt) {
			return fmt.Errorf("repair action timing is invalid")
		}
		if _, exists := actionIDs[action.ActionID]; exists {
			return fmt.Errorf("repair action id %q is duplicated", action.ActionID)
		}
		actionIDs[action.ActionID] = struct{}{}
	}
	return nil
}

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
	DetectorID   string
	EvaluatedAt  time.Time
	Status       DetectorEvaluationStatus
	Fingerprints []string
	Error        string
}

type SurfacedPlaybook struct {
	PlaybookID        string
	ParameterBindings map[string]sdk.ObjectRef
}

type PriorOutcomeEvidence struct {
	IncidentID            string
	MatchReason           string
	RootCauseSummaries    []string
	RepairActionSummaries []string
	AppliedPlaybookIDs    []string
	SourceCommit          string
	ExactSourceMatch      bool
}

type IncidentRequest struct {
	SchemaVersion        string
	Application          string
	Namespace            string
	IncidentID           string
	Findings             []sdk.Finding
	DetectorHistory      []DetectorEvaluation
	SurfacedPlaybooks    []SurfacedPlaybook
	RelevantOutcomes     []PriorOutcomeEvidence
	SourceCommit         string
	DeployedCommit       string
	RepositoryWorktree   string
	RepositoryBaseCommit string
	ResponseDeadline     time.Time
	CancellationToken    string
	RepairPolicy         string
	// StateChanges is the application's configuration diff against its last
	// healthy baseline when the incident opened; nil without a baseline.
	StateChanges *StateChanges
	// FollowUp is set only on a follow-up incident dispatched for findings that
	// stayed active after an earlier responder completed.
	FollowUp *FollowUpContext
}

// FollowUpContext links a follow-up responder request to the incident it
// continues. The request's Findings are the residual findings; PriorSummary
// is a bounded, factual digest of what the earlier responders reported.
type FollowUpContext struct {
	OriginalIncidentID string
	ParentIncidentID   string
	// Attempt is the 1-based follow-up ordinal; MaxFollowUps is the bound.
	Attempt      int
	MaxFollowUps int
	PriorSummary string
}

// RootCauseEvidence is one live observation supporting a root cause: a
// detector finding, a synthetic scenario, a state change, or a command and
// its output. Static artifacts are only context.
type RootCauseEvidence struct {
	Kind        string
	Source      string
	Observation string
}

type ConfirmedRootCause struct {
	Summary   string
	Resources []sdk.ObjectRef
	// Evidence is empty only in results predating evidence-bearing diagnoses.
	Evidence []RootCauseEvidence
	// ExplainedDetectors must clear after the fix.
	ExplainedDetectors []string
	StaticContext      []string
}

type AppliedPlaybook struct {
	PlaybookID        string
	ParameterBindings map[string]sdk.ObjectRef
	ScriptNames       []string
}

type VerificationEvidence struct {
	Name       string
	Passed     bool
	Details    string
	ObservedAt time.Time
}

// UsageMetrics is the shared protobuf message even in controller state, so
// newly reported accounting fields survive dispatch and outcome derivation.
type UsageMetrics = contracts.UsageMetrics

type TimingMetrics struct {
	StartedAt   time.Time
	CompletedAt time.Time
}

type RepairActionReceipt struct {
	ActionID string
	Kind     string
	Target   string
	// Resources are the objects the action mutated; diagnosis verification
	// credits a root cause only to a repair that touched its resources.
	Resources   []sdk.ObjectRef
	Summary     string
	Details     string
	StartedAt   time.Time
	CompletedAt time.Time
	Success     bool
	Reversible  bool
}

type IncidentResult struct {
	SchemaVersion        string
	IncidentID           string
	Status               IncidentStatus
	ConfirmedRootCauses  []ConfirmedRootCause
	AppliedPlaybooks     []AppliedPlaybook
	RepairChanges        []string
	RepairActions        []RepairActionReceipt
	FinalDetectorStates  []DetectorEvaluation
	ProposedArtifactIDs  []string
	VerificationEvidence []VerificationEvidence
	Usage                UsageMetrics
	Timing               TimingMetrics
	ResponderSessionID   string
	LateFindingsPull     *LateFindingsPull
	Error                string
}

// LateFindingsPull is a worker's claim that it consumed a controller-authored
// logical telemetry report. The dispatcher checks its digest and IDs against
// the controller's report history before accepting the result.
type LateFindingsPull struct {
	IncidentID   string
	PullCount    uint32
	DetectorIDs  []string
	PlaybookIDs  []string
	ReportDigest string
}

// ObservedStateChange is one object the controller saw changed from the
// healthy baseline while an incident was open, and when it first saw it.
type ObservedStateChange struct {
	Kind            string
	Name            string
	FirstObservedAt time.Time
}

type IncidentClosure struct {
	Request             IncidentRequest
	Result              *IncidentResult
	DispatchError       string
	FinalDetectorStates []DetectorEvaluation
	// IncidentDetectorStates is the latest post-response evaluation of each
	// non-health detector that raised a finding in this incident. It is
	// learning evidence for the broker, not a closure gate.
	IncidentDetectorStates []DetectorEvaluation
	// DetectorTimeline summarizes, per finding, when each detector fired in this
	// incident window and how that relates to dispatch. The booleans derive
	// from it for analysis: a learned incident detector that fired before the
	// responder was dispatched, one that fired only while it ran, or only
	// health detectors firing.
	DetectorTimeline                    []DetectorTimelineEntry
	IncidentDetectorFiredBeforeDispatch bool
	IncidentDetectorFiredAfterDispatch  bool
	NoIncidentDetectorFired             bool
	// FinalStateChanges is the configuration diff against the healthy
	// baseline at verification time: a composite fault's later
	// component can land a few seconds after dispatch, while the incident is
	// still open, so it is missing from Request.StateChanges but present
	// here. nil without a baseline. Computed from the in-memory informer
	// cache, so it adds no wall-clock time to closure.
	FinalStateChanges    *StateChanges
	DetectedAt           time.Time
	DispatchedAt         time.Time
	ResponderCompletedAt time.Time
	VerifiedAt           time.Time
	// HealthClearedAt is when the closure gate's detectors began their final
	// streak of clear evaluations. Only a repair action that started by then
	// can back a root cause. nil when no streak is on record.
	HealthClearedAt *time.Time
	// ObservedStateChanges is every object the controller saw differ from
	// the healthy baseline while the incident was open, with its first
	// observation: the dispatch diff, later evaluations' diffs, and the
	// closing view. A state-change citation is evidence of the cause only if
	// it was observed before the responder's own repair of that object
	// started; otherwise it is that repair's edit. Omitted without a
	// baseline, when state changes cannot be checked at all.
	ObservedStateChanges []ObservedStateChange
	// DetectorReviewRequiredAt is set when health did not clear within the
	// verification window after the responder completed. Health that
	// clears later was not verifiably restored by the responder.
	DetectorReviewRequiredAt *time.Time
	DetectorReviewReason     string
	// CleanedHelpers are the responder helper objects the controller
	// deleted after the responder completed, as Kind/namespace/name.
	CleanedHelpers []string
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

package runtime

import (
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/sdk"
)

// ProtocolSchemaVersion is the const the contract messages declare for
// schema_version; the proto is its only definition.
var ProtocolSchemaVersion = wire.StringConst(&contracts.IncidentRequest{}, "schema_version")

type DetectorEvaluationStatus string

const (
	DetectorEvaluationFiring  DetectorEvaluationStatus = "firing"
	DetectorEvaluationClear   DetectorEvaluationStatus = "clear"
	DetectorEvaluationError   DetectorEvaluationStatus = "error"
	DetectorEvaluationPending DetectorEvaluationStatus = "pending"
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
	Reason       string `json:"Reason,omitempty"`
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
	DisabledSetupDetectors []*contracts.DisabledSetupDetector
	NamespaceOnboarding    *contracts.NamespaceOnboarding
	// ObservationGeneration binds the incident to accepted scope and installer policy.
	ObservationGeneration string
	SchemaVersion         string
	Application           string
	Namespace             string
	IncidentID            string
	Findings              []sdk.Finding
	DriftEvents           []sdk.DriftEvent
	DetectorHistory       []DetectorEvaluation
	SurfacedPlaybooks     []SurfacedPlaybook
	RelevantOutcomes      []PriorOutcomeEvidence
	SourceCommit          string
	DeployedCommit        string
	RepositoryWorktree    string
	RepositoryBaseCommit  string
	ResponseDeadline      time.Time
	CancellationToken     string
	RepairPolicy          string
	// StateChanges is the application's configuration diff against its last
	// healthy baseline when the incident opened; nil without a baseline.
	StateChanges          *StateChanges
	InitialProgramRuns    []ProgramRun
	PlaybookCoverage      *contracts.PlaybookCoverage
	PriorResponseProgress []*contracts.ResponseProgress
	PriorStages           []*contracts.StageReport
	MaxResponseLoops      int
	// PreparedAgentTask is the exact ProtoJSON task persisted before a Job may be created.
	PreparedAgentTask string
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
	ScriptDigest  string
	FailureReason string
	ActionID      string
	Kind          string
	Target        string
	// Resources are the objects the action mutated; diagnosis verification
	// credits a root cause only to a repair that touched its resources.
	Resources   []sdk.ObjectRef
	Summary     string
	Details     string
	StartedAt   time.Time
	CompletedAt time.Time
	Success     bool
	Reversible  bool
	// Owners are written resources' controller owner chains read at write
	// time; cause attribution follows them instead of generated names.
	Owners []ResourceOwners
}

// ResourceOwners is one written resource's owner chain, nearest owner first.
type ResourceOwners struct {
	Resource   sdk.ObjectRef
	OwnerChain []sdk.ObjectRef
}

type IncidentResult struct {
	DriftReprompt         *contracts.DriftReprompt
	UnresolvedFindings    []sdk.Finding
	DriftEvents           []sdk.DriftEvent
	SchemaVersion         string
	IncidentID            string
	Status                IncidentStatus
	ConfirmedRootCauses   []ConfirmedRootCause
	AppliedPlaybooks      []AppliedPlaybook
	RepairChanges         []string
	RepairActions         []RepairActionReceipt
	FinalDetectorStates   []DetectorEvaluation
	ProposedArtifactIDs   []string
	VerificationEvidence  []VerificationEvidence
	Usage                 UsageMetrics
	Timing                TimingMetrics
	ResponderSessionID    string
	LateFindingsPull      *LateFindingsPull
	Error                 string
	ProgramToolCalls      []ToolCall
	ProgramRuns           []ProgramRun
	PlanApplications      []RecordedPlanApplication
	ProgramFailureSignals []string
	ResponseLoopsUsed     int
	ResponseLoopExhausted bool
	ResolutionPath        string
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

type IncidentClosure struct {
	VerifiedWhileResponderRunning bool
	ClosureStabilityWindow        time.Duration
	// ObservationGeneration identifies the scope used for independent verification.
	ObservationGeneration string
	Request               IncidentRequest
	Result                *IncidentResult
	DispatchError         string
	FinalDetectorStates   []DetectorEvaluation
	// IncidentDetectorStates is the latest post-response evaluation of each
	// non-health detector that raised a finding in this incident. It is
	// learning evidence for the broker, not a closure gate.
	IncidentDetectorStates []DetectorEvaluation
	// PartialDetectorStates preserves confirmed per-finding observations on failed
	// closures without granting global health or source-publication authority.
	PartialDetectorStates []DetectorEvaluation
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
	// streak of clear evaluations. nil when no streak is on record.
	HealthClearedAt *time.Time
	// CleanedHelpers are the responder helper objects the controller
	// deleted after the responder completed, as Kind/namespace/name.
	CleanedHelpers []string // UnconfirmedReason is set when the closure-time recheck never saw a
	// complete observation window within its bound: the closure commits as
	// unverified (final states moved to partial) instead of being superseded.
	UnconfirmedReason contracts.ClosureUnconfirmedReason
	UnconfirmedDetail string
	// SourceCheckNotes lists what the accepted-source checks found when the
	// closure committed: checks that were unavailable, and objects that still
	// differ from the accepted source. They are information on the outcome, not
	// a verdict.
	SourceCheckNotes []string
}

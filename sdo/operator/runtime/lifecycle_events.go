package runtime

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// The incident lifecycle event stream makes SDO's internal lifecycle coherently
// observable. The detector firing stream records whether and when detectors
// fired; this stream records the controller's own incident lifecycle
// transitions -- when an incident opened, when its worktree was prepared, when
// the responder was dispatched and completed, when a follow-up superseded it,
// when a stranded worktree was released, and when the incident closed, its
// outcome committed, and the closure was acknowledged. Joined by incident id
// (and, for firings, the same id field), the two streams plus the outcome commit
// each closure_committed event names let an operator reconstruct one incident's
// full timeline offline from durable artifacts alone.
//
// Like the firing stream, it is operational telemetry, not operational memory:
// it never enters .sdo/, carries no benchmark labels or verdicts, and is derived
// only from the controller's own deterministic lifecycle state. Every field is a
// controller-internal fact (incident ids, git commit shas, worktree paths,
// finding keys); nothing here is transport- or benchmark-specific.

const (
	LifecycleSchemaVersion = "sdo.lifecycle-event/v1"
	// DefaultLifecycleStreamBytes bounds each stream file; with one rotated
	// predecessor the stream never exceeds twice this size.
	DefaultLifecycleStreamBytes int64 = 4 << 20
)

// LifecyclePhase names one incident lifecycle transition. Each (incident, phase)
// pair occurs at most once per incident, so an event id built from them is
// stable and a replay after a crash re-derives the identical id.
type LifecyclePhase string

const (
	// PhaseOpened: a batch of findings opened a new incident (or follow-up).
	PhaseOpened LifecyclePhase = "incident_opened"
	// PhaseAdmissionDeferred: a ready batch was held under resource pressure
	// (load or release-backlog high watermark) before any worktree or dispatch.
	PhaseAdmissionDeferred LifecyclePhase = "admission_deferred"
	// PhaseAdmissionResumed: a held incident cleared the low watermarks and
	// proceeded to dispatch.
	PhaseAdmissionResumed LifecyclePhase = "admission_resumed"
	// PhaseWorkspacePrepared: the broker prepared the incident's isolated worktree.
	PhaseWorkspacePrepared LifecyclePhase = "workspace_prepared"
	// PhaseDispatched: the responder was launched for the incident.
	PhaseDispatched LifecyclePhase = "responder_dispatched"
	// PhaseResponderCompleted: the responder finished (successfully or as a terminal Job failure).
	PhaseResponderCompleted LifecyclePhase = "responder_completed"
	// PhaseSuperseded: a follow-up incident replaced this one before it closed.
	PhaseSuperseded LifecyclePhase = "incident_superseded"
	// PhaseWorktreeReleased: a stranded, superseded worktree was reaped by the broker.
	PhaseWorktreeReleased LifecyclePhase = "worktree_released"
	// PhaseDetectorReviewRequired: health did not clear within the verification window.
	PhaseDetectorReviewRequired LifecyclePhase = "detector_review_required"
	// PhaseClosed: the controller cut the incident closure after verifying health.
	PhaseClosed LifecyclePhase = "incident_closed"
	// PhaseClosureCommitted: the broker committed the authoritative outcome for the closure.
	PhaseClosureCommitted LifecyclePhase = "closure_committed"
	// PhaseAcknowledged: the committed closure was acknowledged and the incident cleared.
	PhaseAcknowledged LifecyclePhase = "incident_acknowledged"
)

// phaseRank orders phases that can share a timestamp so a reconstructed timeline
// is deterministic. It is the natural lifecycle order.
func (p LifecyclePhase) rank() int {
	switch p {
	case PhaseOpened:
		return 0
	case PhaseAdmissionDeferred:
		return 1
	case PhaseAdmissionResumed:
		return 2
	case PhaseWorkspacePrepared:
		return 3
	case PhaseDispatched:
		return 4
	case PhaseResponderCompleted:
		return 5
	case PhaseSuperseded:
		return 6
	case PhaseWorktreeReleased:
		return 7
	case PhaseDetectorReviewRequired:
		return 8
	case PhaseClosed:
		return 9
	case PhaseClosureCommitted:
		return 10
	case PhaseAcknowledged:
		return 11
	default:
		return 12
	}
}

// LifecycleEvent is one line of the incident lifecycle stream. The envelope
// (schema version, event id, event, recorded-at, application, namespace,
// incident id) mirrors FiringRecord so the two streams join cleanly; the
// remaining fields are phase-specific and omitted when empty.
type LifecycleEvent struct {
	SchemaVersion       string         `json:"schema_version"`
	EventID             string         `json:"event_id"`
	Event               LifecyclePhase `json:"event"`
	RecordedAt          time.Time      `json:"recorded_at"`
	EvaluationIteration int            `json:"evaluation_iteration,omitempty"`
	Application         string         `json:"application"`
	Namespace           string         `json:"namespace"`
	IncidentID          string         `json:"incident_id"`

	// ParentIncidentID links a follow-up (on PhaseOpened) and a superseded parent
	// (on PhaseSuperseded, naming the child in SupersededBy) into one chain.
	ParentIncidentID string `json:"parent_incident_id,omitempty"`
	FollowUpAttempt  int    `json:"follow_up_attempt,omitempty"`
	SupersededBy     string `json:"superseded_by,omitempty"`
	Reason           string `json:"reason,omitempty"`

	// Opened describes the batch that opened the incident. SurfacedPlaybooks
	// contains logical playbook IDs.
	FindingKeys       []string `json:"finding_keys,omitempty"`
	SurfacedPlaybooks []string `json:"surfaced_playbooks,omitempty"`

	// Worktree/BaseCommit record the prepared isolated worktree; the commit
	// fields link a committed closure to its accepted outcome.
	Worktree         string `json:"worktree,omitempty"`
	BaseCommit       string `json:"base_commit,omitempty"`
	ProposalCommit   string `json:"proposal_commit,omitempty"`
	OutcomeCommit    string `json:"outcome_commit,omitempty"`
	ReflectionCommit string `json:"reflection_commit,omitempty"`

	// Closed/review summarize verification without any benchmark verdict.
	HealthClearedAt        *time.Time `json:"health_cleared_at,omitempty"`
	DetectorReviewRequired bool       `json:"detector_review_required,omitempty"`
	DispatchError          bool       `json:"dispatch_error,omitempty"`

	// Load/backpressure fields describe an admission decision under resource
	// pressure (admission_deferred / admission_resumed). They carry only
	// controller-internal load facts: the gauge reading, the governing watermark,
	// and the stranded-worktree release backlog; never a benchmark verdict.
	LoadPressure   float64 `json:"load_pressure,omitempty"`
	LoadThreshold  float64 `json:"load_threshold,omitempty"`
	ReleaseBacklog int     `json:"release_backlog,omitempty"`
}

// LifecycleSink receives lifecycle events. Record must be idempotent in
// LifecycleEvent.EventID: a controller that crashes between emitting an event
// and persisting its state replays the same transition with the same id.
type LifecycleSink interface {
	RecordLifecycle(LifecycleEvent) error
}

// lifecycleEventID derives a content-addressed id from an incident's id, the
// phase, and any discriminator (the superseding child, say). Each (incident,
// phase[, discriminator]) tuple is emitted at most once, so the id is stable
// across a crash and a replay drops the duplicate.
func lifecycleEventID(parts ...string) string {
	return contentAddressedID(parts...)
}

// FileLifecycleSink is the lifecycle stream's durable sink, a typed wrapper over
// the shared durableEventStream. It shares the firing stream's bounded rotation,
// torn-line repair, content-addressed deduplication, and StartFresh archival.
type FileLifecycleSink struct {
	stream *durableEventStream
}

func NewFileLifecycleSink(path string, maxBytes int64) (*FileLifecycleSink, error) {
	stream, err := openDurableEventStream(path, maxBytes)
	if err != nil {
		return nil, err
	}
	return &FileLifecycleSink{stream: stream}, nil
}

func (s *FileLifecycleSink) RecordLifecycle(event LifecycleEvent) error {
	if event.EventID == "" {
		return fmt.Errorf("lifecycle event has no event id")
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode lifecycle event: %w", err)
	}
	return s.stream.append(event.EventID, payload)
}

// StartFresh archives a stream left by an earlier controller lifecycle.
func (s *FileLifecycleSink) StartFresh() error {
	return s.stream.StartFresh()
}

// SetLifecycleSink attaches the incident lifecycle telemetry sink.
func (c *Controller) SetLifecycleSink(sink LifecycleSink) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lifecycleSink = sink
}

// emitLifecycleLocked records one lifecycle transition. Callers hold c.mu; the
// sink takes its own lock, so there is no lock-order hazard. A nil sink makes
// this a no-op, matching emitFiringLocked.
func (c *Controller) emitLifecycleLocked(event LifecycleEvent) {
	if c.lifecycleSink == nil {
		return
	}
	event.SchemaVersion = LifecycleSchemaVersion
	event.Application = c.config.Application
	event.Namespace = c.config.Namespace
	event.EvaluationIteration = c.evaluationIteration
	if event.EventID == "" {
		event.EventID = lifecycleEventID(event.IncidentID, string(event.Event))
	}
	if err := c.lifecycleSink.RecordLifecycle(event); err != nil && c.OnError != nil {
		c.OnError(fmt.Errorf("record incident lifecycle event: %w", err))
	}
}

// TimelineEntry is one event in a reconstructed incident timeline, from either
// durable stream. It carries the source record so a caller can inspect the full
// detail after locating the transition.
type TimelineEntry struct {
	At      time.Time
	EventID string
	Kind    string
	// Source is "lifecycle" or "firing".
	Source    string
	Lifecycle *LifecycleEvent
	Firing    *FiringRecord
}

// ReconstructIncidentTimeline joins the lifecycle and firing streams for one
// incident id into a single, deterministically ordered timeline. This is the
// offline reconstruction an operator runs against the durable artifacts: no
// cluster, no clock, no RNG -- the same inputs always yield the same timeline.
// Follow-up children are distinct incident ids; a caller walks the chain by
// following ParentIncidentID / SupersededBy on the reconstructed events.
func ReconstructIncidentTimeline(
	incidentID string, lifecycle []LifecycleEvent, firings []FiringRecord,
) []TimelineEntry {
	var entries []TimelineEntry
	for index := range lifecycle {
		event := lifecycle[index]
		if event.IncidentID != incidentID {
			continue
		}
		cloned := event
		entries = append(entries, TimelineEntry{
			At: event.RecordedAt, EventID: event.EventID, Kind: string(event.Event),
			Source: "lifecycle", Lifecycle: &cloned,
		})
	}
	for index := range firings {
		record := firings[index]
		if record.IncidentID != incidentID {
			continue
		}
		cloned := record
		entries = append(entries, TimelineEntry{
			At: record.RecordedAt, EventID: record.EventID, Kind: string(record.Event),
			Source: "firing", Firing: &cloned,
		})
	}
	sort.SliceStable(entries, func(left int, right int) bool {
		if !entries[left].At.Equal(entries[right].At) {
			return entries[left].At.Before(entries[right].At)
		}
		leftRank, rightRank := timelineRank(entries[left]), timelineRank(entries[right])
		if leftRank != rightRank {
			return leftRank < rightRank
		}
		return entries[left].EventID < entries[right].EventID
	})
	return entries
}

// timelineRank orders events that share a timestamp. Firing transitions sit
// between opened and closed phases so a batched firing reads after the incident
// opened and before it closes, which is the order the controller produced them.
func timelineRank(entry TimelineEntry) int {
	if entry.Lifecycle != nil {
		return entry.Lifecycle.Event.rank()
	}
	// Any firing record: between opened and responder_completed, at the dispatch rank.
	return PhaseDispatched.rank()
}

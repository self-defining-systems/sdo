package runtime

import (
	"fmt"
	"sort"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
)

// The incident lifecycle event stream makes SDO's internal lifecycle coherently
// observable. The detector firing stream records whether and when detectors
// fired; this stream records the controller's own incident lifecycle
// transitions -- when an incident opened, when its worktree was prepared, when
// the responder was dispatched and completed, and when the incident closed, its
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

// LifecycleSchemaVersion is the const rule of LifecycleEvent.schema_version.
var LifecycleSchemaVersion = wire.StringConst(&contracts.LifecycleEvent{}, "schema_version")

const (
	// DefaultLifecycleStreamBytes bounds each stream file; with one rotated
	// predecessor the stream never exceeds twice this size.
	DefaultLifecycleStreamBytes int64 = 4 << 20
)

// The lifecycle phases name the values of LifecycleEvent.event, whose permitted
// set is the `in` rule in telemetry.proto (TestLifecyclePhasesMatchProto).
// A phase names one incident lifecycle transition. Each (incident, phase)
// pair occurs at most once per incident, so an event id built from them is
// stable and a replay after a crash re-derives the identical id.
const (
	// PhaseOpened: a batch of findings opened a new incident.
	PhaseOpened = "incident_opened"
	// PhaseWorkspacePrepared: the broker prepared the incident's isolated worktree.
	PhaseWorkspacePrepared = "workspace_prepared"
	// PhaseDispatched: the responder was launched for the incident.
	PhaseDispatched = "responder_dispatched"
	// PhaseResponderCompleted: the responder finished (successfully or as a terminal Job failure).
	PhaseResponderCompleted = "responder_completed"
	// PhaseClosed: the controller cut the incident closure after verifying health or timing out.
	PhaseClosed = "incident_closed"
	// PhaseClosureCommitted: the broker committed the authoritative outcome for the closure.
	PhaseClosureCommitted = "closure_committed"
	// PhaseAcknowledged: the committed closure was acknowledged and the incident cleared.
	PhaseAcknowledged = "incident_acknowledged"
)

// phaseRank orders phases that can share a timestamp so a reconstructed timeline
// is deterministic. It is the natural lifecycle order.
func phaseRank(p string) int {
	switch p {
	case PhaseOpened:
		return 0
	case PhaseWorkspacePrepared:
		return 3
	case PhaseDispatched:
		return 4
	case PhaseResponderCompleted:
		return 5
	case PhaseClosed:
		return 6
	case PhaseClosureCommitted:
		return 7
	case PhaseAcknowledged:
		return 8
	default:
		return 9
	}
}

// LifecycleSink receives lifecycle events. Record must be idempotent in
// LifecycleEvent.event_id: a controller that crashes between emitting an event
// and persisting its state replays the same transition with the same id.
type LifecycleSink interface {
	RecordLifecycle(*contracts.LifecycleEvent) error
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

func (s *FileLifecycleSink) RecordLifecycle(event *contracts.LifecycleEvent) error {
	payload, err := wire.Marshal(event)
	if err != nil {
		return err
	}
	return s.stream.append(event.GetEventId(), payload)
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
func (c *Controller) emitLifecycleLocked(event *contracts.LifecycleEvent) {
	if c.lifecycleSink == nil {
		return
	}
	event.SchemaVersion = LifecycleSchemaVersion
	event.Application = c.config.Application
	event.Namespace = c.config.Namespace
	event.EvaluationIteration = int32(c.evaluationIteration)
	if event.EventId == "" {
		event.EventId = lifecycleEventID(event.IncidentId, event.Event)
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
	Lifecycle *contracts.LifecycleEvent
	Firing    *contracts.FiringRecord
}

// ReconstructIncidentTimeline joins the lifecycle and firing streams for one
// incident id into a single, deterministically ordered timeline. This is the
// offline reconstruction an operator runs against the durable artifacts: no
// cluster, no clock, no RNG -- the same inputs always yield the same timeline.
func ReconstructIncidentTimeline(
	incidentID string, lifecycle []*contracts.LifecycleEvent, firings []*contracts.FiringRecord,
) []TimelineEntry {
	var entries []TimelineEntry
	for index := range lifecycle {
		event := lifecycle[index]
		if event.GetIncidentId() != incidentID {
			continue
		}
		entries = append(entries, TimelineEntry{
			At: event.GetRecordedAt().AsTime(), EventID: event.GetEventId(), Kind: event.GetEvent(),
			Source: "lifecycle", Lifecycle: event,
		})
	}
	for index := range firings {
		record := firings[index]
		if record.GetIncidentId() != incidentID {
			continue
		}
		entries = append(entries, TimelineEntry{
			At: record.GetRecordedAt().AsTime(), EventID: record.GetEventId(), Kind: record.GetEvent(),
			Source: "firing", Firing: record,
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
		return phaseRank(entry.Lifecycle.GetEvent())
	}
	// Any firing record: between opened and responder_completed, at the dispatch rank.
	return phaseRank(PhaseDispatched)
}

package runtime

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"sdo.dev/operator/sdk"
)

// Detector firing telemetry is a durable, append-only record of whether and
// when each detector fired. It is operational telemetry, not operational
// memory: it never enters .sdo/, carries no benchmark labels or verdicts, and
// is derived only from the controller's own deterministic firing state.

const (
	FiringSchemaVersion = "sdo.detector-firing/v1"
	// DefaultFiringStreamBytes bounds each stream file; with one rotated
	// predecessor the stream never exceeds twice this size.
	DefaultFiringStreamBytes int64 = 4 << 20
	// maxTimelineEntries bounds the per-incident timeline kept in controller state.
	maxTimelineEntries = 64
)

type FiringEvent string

const (
	// FiringActivated: a finding reached its firing threshold.
	FiringActivated FiringEvent = "activated"
	// FiringCleared: an active finding reached its clear threshold.
	FiringCleared FiringEvent = "cleared"
	// FiringNotPersisted: a finding was flagged but disappeared before the
	// persistence policy's firing threshold, so it never activated.
	FiringNotPersisted FiringEvent = "not_persisted"
	// FiringBatched: an activated finding became part of a dispatched incident batch.
	FiringBatched FiringEvent = "batched"
)

type FiringRelation string

const (
	// RelationBeforeDispatch: the finding was part of the batch the responder received.
	RelationBeforeDispatch FiringRelation = "before_dispatch"
	// RelationAfterDispatch: the finding activated while an incident's responder was in flight.
	RelationAfterDispatch FiringRelation = "after_dispatch"
	// RelationNoIncident: no incident was open, or the finding never joined one.
	RelationNoIncident FiringRelation = "no_incident"
)

// FiringRecord is one line of the detector firing stream.
type FiringRecord struct {
	SchemaVersion       string                   `json:"schema_version"`
	EventID             string                   `json:"event_id"`
	Event               FiringEvent              `json:"event"`
	RecordedAt          time.Time                `json:"recorded_at"`
	EvaluationIteration int                      `json:"evaluation_iteration"`
	Application         string                   `json:"application"`
	Namespace           string                   `json:"namespace"`
	DetectorID          string                   `json:"detector_id"`
	DetectorClass       string                   `json:"detector_class"`
	Owner               string                   `json:"owner"`
	RuleID              string                   `json:"rule_id"`
	Fingerprint         string                   `json:"fingerprint"`
	Severity            string                   `json:"severity"`
	ParameterBindings   map[string]sdk.ObjectRef `json:"parameter_bindings"`
	SurfacedPlaybooks   []string                 `json:"surfaced_playbooks"`
	FiringCount         int                      `json:"firing_count"`
	FiringThreshold     int                      `json:"firing_threshold"`
	ClearCount          int                      `json:"clear_count"`
	ClearThreshold      int                      `json:"clear_threshold"`
	IncidentID          string                   `json:"incident_id"`
	DispatchRelation    FiringRelation           `json:"dispatch_relation"`
}

// FiringSink receives firing records. Record must be idempotent in
// FiringRecord.EventID: a controller that crashes between emitting a record and
// persisting its state replays the same transition with the same id.
type FiringSink interface {
	Record(FiringRecord) error
}

// firingEventID derives a content-addressed firing event id from a transition's
// durable content.
func firingEventID(parts ...string) string {
	return contentAddressedID(parts...)
}

// FileFiringSink is the detector firing stream's durable sink. It is a thin
// typed wrapper over the shared durableEventStream, so it inherits that stream's
// bounded rotation, torn-line repair, content-addressed deduplication, and
// StartFresh archival.
type FileFiringSink struct {
	stream *durableEventStream
}

func NewFileFiringSink(path string, maxBytes int64) (*FileFiringSink, error) {
	stream, err := openDurableEventStream(path, maxBytes)
	if err != nil {
		return nil, err
	}
	return &FileFiringSink{stream: stream}, nil
}

func (s *FileFiringSink) Record(record FiringRecord) error {
	if record.EventID == "" {
		return fmt.Errorf("firing record has no event id")
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode firing record: %w", err)
	}
	return s.stream.append(record.EventID, payload)
}

// StartFresh archives a stream left by an earlier controller lifecycle.
func (s *FileFiringSink) StartFresh() error {
	return s.stream.StartFresh()
}

// DetectorTimelineEntry summarizes one finding's firing history within the
// current incident window. It is carried in the incident closure.
type DetectorTimelineEntry struct {
	DetectorID          string                   `json:"detector_id"`
	DetectorClass       string                   `json:"detector_class"`
	Owner               string                   `json:"owner"`
	RuleID              string                   `json:"rule_id"`
	Fingerprint         string                   `json:"fingerprint"`
	ParameterBindings   map[string]sdk.ObjectRef `json:"parameter_bindings"`
	SurfacedPlaybookIDs []string                 `json:"surfaced_playbook_ids"`
	FirstActivatedAt    time.Time                `json:"first_activated_at"`
	LastSeenAt          time.Time                `json:"last_seen_at"`
	ClearedAt           *time.Time               `json:"cleared_at,omitempty"`
	Relation            FiringRelation           `json:"relation"`
	Activations         int                      `json:"activations"`
}

func (entry DetectorTimelineEntry) key() string {
	return FindingStateKey(entry.DetectorID, entry.Fingerprint)
}

// timelineSummary derives the three analysis booleans from a timeline.
func timelineSummary(timeline []DetectorTimelineEntry) (before bool, after bool, onlyHealth bool) {
	incident := false
	for _, entry := range timeline {
		if entry.DetectorClass == string(sdk.DetectorClassHealth) {
			continue
		}
		incident = true
		switch entry.Relation {
		case RelationBeforeDispatch:
			before = true
		case RelationAfterDispatch:
			after = true
		}
	}
	return before, after, len(timeline) > 0 && !incident
}

func sortTimeline(timeline []DetectorTimelineEntry) {
	sort.SliceStable(timeline, func(left int, right int) bool {
		if !timeline[left].FirstActivatedAt.Equal(timeline[right].FirstActivatedAt) {
			return timeline[left].FirstActivatedAt.Before(timeline[right].FirstActivatedAt)
		}
		return timeline[left].key() < timeline[right].key()
	})
}

func cloneTimeline(timeline []DetectorTimelineEntry) []DetectorTimelineEntry {
	if len(timeline) == 0 {
		return nil
	}
	result := make([]DetectorTimelineEntry, len(timeline))
	for index, entry := range timeline {
		entry.ParameterBindings = cloneBindings(entry.ParameterBindings)
		entry.SurfacedPlaybookIDs = append([]string{}, entry.SurfacedPlaybookIDs...)
		if entry.ClearedAt != nil {
			clearedAt := *entry.ClearedAt
			entry.ClearedAt = &clearedAt
		}
		result[index] = entry
	}
	return result
}

func cloneBindings(bindings map[string]sdk.ObjectRef) map[string]sdk.ObjectRef {
	result := make(map[string]sdk.ObjectRef, len(bindings))
	for role, resource := range bindings {
		result[role] = resource
	}
	return result
}

// SetFiringSink attaches the detector firing telemetry sink.
func (c *Controller) SetFiringSink(sink FiringSink) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.firingSink = sink
}

// RestoredFromState reports whether AttachStateStore restored durable state, as
// opposed to starting a new controller lifecycle.
func (c *Controller) RestoredFromState() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stateRestored
}

// recordFiring emits telemetry for the transitions one detector's evaluation
// produced and maintains the incident timeline. It runs after batching
// decisions so a finding that joined an open incident is classified correctly.
func (c *Controller) recordFiring(spec sdk.DetectorSpec, now time.Time, findings []sdk.Finding, changes FindingChanges) {
	c.mu.Lock()
	defer c.mu.Unlock()
	at := now.UTC()
	for _, finding := range findings {
		if finding.Status != sdk.FindingActive {
			continue
		}
		if index := c.timelineIndexLocked(spec.ID, FindingFingerprint(finding)); index >= 0 &&
			c.timeline[index].ClearedAt == nil {
			c.timeline[index].LastSeenAt = at
		}
	}
	for _, transition := range changes.Transitions {
		fingerprint := FindingFingerprint(transition.Finding)
		incidentID, relation := c.classifyFindingLocked(spec.ID, transition.Finding)
		index := c.timelineIndexLocked(spec.ID, fingerprint)
		switch transition.Event {
		case FiringActivated:
			if index >= 0 {
				entry := &c.timeline[index]
				entry.ClearedAt = nil
				entry.LastSeenAt = at
				entry.Activations++
				entry.ParameterBindings = cloneBindings(transition.Finding.ParameterBindings)
				if entry.Relation != RelationBeforeDispatch {
					entry.Relation = relation
				}
				relation = entry.Relation
			} else {
				c.addTimelineEntryLocked(spec, transition.Finding, at, relation)
			}
		case FiringCleared:
			if index >= 0 {
				clearedAt := at
				c.timeline[index].ClearedAt = &clearedAt
				relation = c.timeline[index].Relation
			}
		}
		c.emitFiringLocked(transition.Event, spec.ID, transition.Finding, at, incidentID, relation,
			transition.FiringCount, transition.ClearCount,
			firingEventID(spec.ID, fingerprint, string(transition.Event), fmt.Sprint(transition.Sequence)))
	}
}

// classifyFindingLocked relates a finding to the open incident, if any.
func (c *Controller) classifyFindingLocked(detectorID string, finding sdk.Finding) (string, FiringRelation) {
	if !c.incidentOpen || c.currentIncidentRequest == nil {
		return "", RelationNoIncident
	}
	incidentID := c.currentIncidentRequest.IncidentID
	key := FindingStateKey(detectorID, FindingFingerprint(finding))
	for _, existing := range c.currentIncidentRequest.Findings {
		if FindingStateKey(existing.DetectorID, FindingFingerprint(existing)) == key {
			return incidentID, RelationBeforeDispatch
		}
	}
	if !c.incidentDispatchedAt.IsZero() {
		return incidentID, RelationAfterDispatch
	}
	return incidentID, RelationNoIncident
}

// noteBatchedLocked marks findings that became part of an incident's batch.
// Activation records carry the relation known at activation time, usually
// no_incident because the batcher debounces; this settles it.
func (c *Controller) noteBatchedLocked(incidentID string, findings []sdk.Finding) {
	for _, finding := range findings {
		fingerprint := FindingFingerprint(finding)
		spec := c.detectorSpecs[finding.DetectorID]
		if index := c.timelineIndexLocked(finding.DetectorID, fingerprint); index >= 0 {
			c.timeline[index].Relation = RelationBeforeDispatch
		}
		firing := 0
		if state, ok := c.tracker.states[FindingStateKey(finding.DetectorID, fingerprint)]; ok {
			firing = state.FiringCount
		}
		c.emitFiringLocked(FiringBatched, spec.ID, finding, c.evaluationAt, incidentID, RelationBeforeDispatch,
			firing, 0, firingEventID(incidentID, finding.DetectorID, fingerprint, string(FiringBatched)))
	}
}

// pruneTimelineLocked drops entries that cleared before this incident opened
// and did not join its batch; they are unrelated blips.
func (c *Controller) pruneTimelineLocked(batch []sdk.Finding) {
	inBatch := make(map[string]struct{}, len(batch))
	for _, finding := range batch {
		inBatch[FindingStateKey(finding.DetectorID, FindingFingerprint(finding))] = struct{}{}
	}
	kept := c.timeline[:0]
	for _, entry := range c.timeline {
		if _, ok := inBatch[entry.key()]; ok || entry.ClearedAt == nil {
			kept = append(kept, entry)
		}
	}
	c.timeline = kept
}

func (c *Controller) timelineIndexLocked(detectorID string, fingerprint string) int {
	for index := range c.timeline {
		if c.timeline[index].DetectorID == detectorID && c.timeline[index].Fingerprint == fingerprint {
			return index
		}
	}
	return -1
}

func (c *Controller) addTimelineEntryLocked(
	spec sdk.DetectorSpec, finding sdk.Finding, at time.Time, relation FiringRelation,
) {
	if len(c.timeline) >= maxTimelineEntries {
		// Prefer evicting the oldest cleared entry over an active one.
		victim := 0
		for index, entry := range c.timeline {
			if entry.ClearedAt != nil {
				victim = index
				break
			}
		}
		c.timeline = append(c.timeline[:victim], c.timeline[victim+1:]...)
	}
	c.timeline = append(c.timeline, DetectorTimelineEntry{
		DetectorID: spec.ID, DetectorClass: string(spec.Class), Owner: string(spec.Owner),
		RuleID: finding.RuleID, Fingerprint: FindingFingerprint(finding),
		ParameterBindings:   cloneBindings(finding.ParameterBindings),
		SurfacedPlaybookIDs: append([]string{}, finding.Playbooks...),
		FirstActivatedAt:    at, LastSeenAt: at, Relation: relation, Activations: 1,
	})
}

func (c *Controller) emitFiringLocked(
	event FiringEvent, detectorID string, finding sdk.Finding, at time.Time, incidentID string,
	relation FiringRelation, firingCount int, clearCount int, eventID string,
) {
	if c.firingSink == nil {
		return
	}
	spec := c.detectorSpecs[detectorID]
	policy := c.tracker.policy(detectorID)
	record := FiringRecord{
		SchemaVersion: FiringSchemaVersion, EventID: eventID, Event: event, RecordedAt: at,
		EvaluationIteration: c.evaluationIteration,
		Application:         c.config.Application, Namespace: c.config.Namespace,
		DetectorID: detectorID, DetectorClass: string(spec.Class), Owner: string(spec.Owner),
		RuleID: finding.RuleID, Fingerprint: FindingFingerprint(finding), Severity: string(finding.Severity),
		ParameterBindings: cloneBindings(finding.ParameterBindings),
		SurfacedPlaybooks: append([]string{}, finding.Playbooks...),
		FiringCount:       firingCount, FiringThreshold: policy.Firing,
		ClearCount: clearCount, ClearThreshold: policy.Clearing,
		IncidentID: incidentID, DispatchRelation: relation,
	}
	if err := c.firingSink.Record(record); err != nil && c.OnError != nil {
		c.OnError(fmt.Errorf("record detector firing: %w", err))
	}
}

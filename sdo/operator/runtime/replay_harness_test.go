// Package runtime deterministic record-and-replay harness.
//
// These tests attack a different bug class than the chaos suite
// (chaos_harness_test.go): not "a timing interleaving nobody tried", but "a
// recorded incident that needs a live cluster to reproduce". A real incident's
// durable artifacts -- the detector-firing telemetry stream, the
// IncidentClosure the controller produced, and the RuntimeState it persisted --
// are frozen on disk as a golden fixture under testdata/replay/<name>/ and
// driven back through the controller's existing deterministic artifact paths
// with no cluster. The invariant is that every value the controller re-derives
// from a recorded artifact matches what the record captured, byte-for-byte, so
// a once-live incident becomes a permanent offline regression test.
//
// Determinism contract: each replay dimension is a pure function of the frozen
// artifacts. Same fixture in, same result out, on any machine, with no clock,
// no RNG, and no Kubernetes API server (the state round-trip runs against a
// fake clientset, like the chaos restart dimension). A fixture that ever
// diverges names itself in the failing subtest.
//
// This file is test-only. It adds no production control flow and reuses the
// in-package production funcs it is meant to pin (NewFileFiringSink, the
// content-addressed firingEventID, sortTimeline/timelineSummary, the
// ConfigMapStateStore) plus the in-package fakes (readStream, ...). It is
// cause-blind: it inspects only durable fields, never a benchmark verdict, and
// a deterministic Go detector never runs here at all.
package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// replayFixtureRoot is the directory tree of frozen incidents, relative to the
// runtime package. Go excludes testdata/ from builds, so the fixtures never
// ship in a binary.
const replayFixtureRoot = "testdata/replay"

// replayFiringsFile, replayClosureFile, and replayStateFile are the durable
// artifact names inside one fixture directory. The firing stream is required;
// the closure and state are optional so a fixture can freeze whatever the
// source run durably recorded.
const (
	replayFiringsFile   = "detector-firings.jsonl"
	replayLifecycleFile = "lifecycle-events.jsonl"
	replayClosureFile   = "closure.json"
	replayStateFile     = "state.json"
	replayMetaFile      = "meta.json"
)

// replayMeta is a fixture's provenance and scrub record. It documents where the
// incident came from and what was rewritten to make it environment-neutral, so
// a reviewer can audit that no hostnames, cluster names, or benchmark verdicts
// leaked into the committed golden.
type replayMeta struct {
	Source      string   `json:"source"`
	Description string   `json:"description"`
	Scrubbed    []string `json:"scrubbed"`
}

// replayFixture is one recorded incident frozen on disk.
type replayFixture struct {
	name      string
	dir       string
	firings   []FiringRecord
	lifecycle []LifecycleEvent
	closure   *IncidentClosure
	state     *RuntimeState
	meta      replayMeta
}

// loadReplayFixtures scans the fixture root and loads every incident directory.
// A missing root yields no fixtures (the suite then skips), so the harness can
// land before any fixture is frozen.
func loadReplayFixtures(t *testing.T) []replayFixture {
	t.Helper()
	entries, err := os.ReadDir(replayFixtureRoot)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatalf("read replay fixture root: %v", err)
	}
	var fixtures []replayFixture
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		fixtures = append(fixtures, loadReplayFixture(t, filepath.Join(replayFixtureRoot, entry.Name()), entry.Name()))
	}
	sort.Slice(fixtures, func(i, j int) bool { return fixtures[i].name < fixtures[j].name })
	return fixtures
}

// loadReplayFixture reads one incident directory. The firing stream is
// mandatory; the closure, state, and meta are read when present.
func loadReplayFixture(t *testing.T, dir string, name string) replayFixture {
	t.Helper()
	firings := readStream(t, filepath.Join(dir, replayFiringsFile))
	if len(firings) == 0 {
		t.Fatalf("fixture %s: %s is missing or empty", name, replayFiringsFile)
	}
	fixture := replayFixture{name: name, dir: dir, firings: firings}
	fixture.lifecycle = readLifecycleStream(t, filepath.Join(dir, replayLifecycleFile))
	if raw, ok := readReplayFile(t, filepath.Join(dir, replayClosureFile)); ok {
		document := &contracts.IncidentClosure{}
		if err := parseProtoJSON(raw, document); err != nil {
			t.Fatalf("decode %s closure: %v", name, err)
		}
		closure := incidentClosureState(document)
		fixture.closure = &closure
	}
	if raw, ok := readReplayFile(t, filepath.Join(dir, replayStateFile)); ok {
		state, err := decodeControllerState(raw)
		if err != nil {
			t.Fatalf("decode %s state: %v", name, err)
		}
		fixture.state = &state
	}
	if raw, ok := readReplayFile(t, filepath.Join(dir, replayMetaFile)); ok {
		if err := json.Unmarshal(raw, &fixture.meta); err != nil {
			t.Fatalf("decode %s meta: %v", name, err)
		}
	}
	return fixture
}

// readReplayFile reads an optional artifact, reporting whether it was present.
// A present-but-unreadable artifact fails loudly rather than being skipped.
func readReplayFile(t *testing.T, path string) ([]byte, bool) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, false
	}
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return raw, true
}

// TestReplayRecordedIncidents is the suite entrypoint: every frozen fixture is
// replayed through every dimension that applies to it. With no fixtures yet it
// skips, so the harness is green from its first commit.
func TestReplayRecordedIncidents(t *testing.T) {
	fixtures := loadReplayFixtures(t)
	if len(fixtures) == 0 {
		t.Skipf("no replay fixtures under %s", replayFixtureRoot)
	}
	for _, fixture := range fixtures {
		fixture := fixture
		t.Run(fixture.name, func(t *testing.T) {
			replayIncident(t, fixture)
		})
	}
}

// replayIncident drives one fixture back through the controller's deterministic
// artifact paths. Each dimension is a subtest so a failure localizes to the
// exact derivation that diverged from the record.
func replayIncident(t *testing.T, fx replayFixture) {
	t.Helper()
	t.Run("firing-stream-durable", func(t *testing.T) { replayFiringStreamDurable(t, fx) })
	t.Run("event-id-attribution", func(t *testing.T) { replayEventIDAttribution(t, fx) })
	if len(fx.lifecycle) > 0 {
		t.Run("lifecycle-stream-durable", func(t *testing.T) { replayLifecycleStreamDurable(t, fx) })
		t.Run("lifecycle-event-id-attribution", func(t *testing.T) { replayLifecycleEventIDAttribution(t, fx) })
		t.Run("incident-timeline-reconstruction", func(t *testing.T) { replayIncidentTimeline(t, fx) })
	}
	if fx.closure != nil {
		t.Run("closure-analysis-booleans", func(t *testing.T) { replayClosureAnalysis(t, fx) })
	}
	if fx.state != nil {
		t.Run("state-round-trip", func(t *testing.T) { replayStateRoundTrip(t, fx) })
	}
}

// replayFiringStreamDurable re-emits the recorded stream through the production
// FileFiringSink and asserts the durable result is the record, then that a
// restart over the same file replays every record idempotently (every id is
// already seen) and leaves the stream byte-for-byte unchanged. This pins the
// crash-replay contract the sink documents: Record is idempotent in EventID.
func replayFiringStreamDurable(t *testing.T, fx replayFixture) {
	t.Helper()
	path := filepath.Join(t.TempDir(), replayFiringsFile)
	sink, err := NewFileFiringSink(path, DefaultFiringStreamBytes)
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	for _, record := range fx.firings {
		if err := sink.Record(record); err != nil {
			t.Fatalf("record %s: %v", record.EventID, err)
		}
	}
	got := readStream(t, path)
	assertFiringRecordsEqual(t, fx.firings, got)

	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read durable stream: %v", err)
	}
	// Restart: a fresh sink reopens the same file, so every recorded id is
	// already in `seen` and re-recording it is a no-op.
	restart, err := NewFileFiringSink(path, DefaultFiringStreamBytes)
	if err != nil {
		t.Fatalf("restart sink: %v", err)
	}
	for _, record := range fx.firings {
		if err := restart.Record(record); err != nil {
			t.Fatalf("replay record %s after restart: %v", record.EventID, err)
		}
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read durable stream after restart: %v", err)
	}
	if string(before) != string(after) {
		t.Fatalf("replay after restart mutated the durable stream (%d -> %d bytes)", len(before), len(after))
	}
}

// replayEventIDAttribution re-derives every record's event id from the record's
// own durable content and the transition ordinal reconstructed from the stream,
// and asserts it equals the recorded id. The id is content-addressed
// (firingEventID over detector, fingerprint, event kind, and per-finding
// sequence; for a batched record, over incident, detector, fingerprint), so a
// replay re-derives the identical id -- that is exactly what lets a crashed
// controller dedup a re-emitted transition. The derivation is also asserted
// stable (recomputing it yields the same id) and collision-free across the
// stream.
func replayEventIDAttribution(t *testing.T, fx replayFixture) {
	t.Helper()
	activated := map[string]int{}
	cleared := map[string]int{}
	notPersisted := map[string]int{}
	seen := map[string]FiringRecord{}

	for _, record := range fx.firings {
		key := FindingStateKey(record.DetectorID, record.Fingerprint)
		var want string
		switch record.Event {
		case FiringActivated:
			activated[key]++
			want = firingEventID(record.DetectorID, record.Fingerprint, string(record.Event), strconv.Itoa(activated[key]))
		case FiringCleared:
			cleared[key]++
			want = firingEventID(record.DetectorID, record.Fingerprint, string(record.Event), strconv.Itoa(cleared[key]))
		case FiringNotPersisted:
			notPersisted[key]++
			want = firingEventID(record.DetectorID, record.Fingerprint, string(record.Event), strconv.Itoa(notPersisted[key]))
		case FiringBatched:
			want = firingEventID(record.IncidentID, record.DetectorID, record.Fingerprint, string(FiringBatched))
		default:
			t.Fatalf("record %s has unknown event %q", record.EventID, record.Event)
		}
		if want != record.EventID {
			t.Fatalf("event id for %s/%s %s mismatch: recorded %s, re-derived %s",
				record.DetectorID, record.Fingerprint, record.Event, record.EventID, want)
		}
		if again := want; again != record.EventID {
			t.Fatalf("event id derivation is not stable for %s", record.EventID)
		}
		if prior, dup := seen[record.EventID]; dup {
			t.Fatalf("event id %s is not unique: %s/%s %s and %s/%s %s",
				record.EventID, prior.DetectorID, prior.Fingerprint, prior.Event,
				record.DetectorID, record.Fingerprint, record.Event)
		}
		seen[record.EventID] = record
	}
}

// replayClosureAnalysis re-derives the closure's three analysis booleans from
// its own frozen DetectorTimeline through the production sort and summary, and
// asserts they match the booleans the controller recorded. It also asserts the
// sort is idempotent, so the derivation is byte-stable regardless of the
// timeline's stored order.
func replayClosureAnalysis(t *testing.T, fx replayFixture) {
	t.Helper()
	timeline := cloneTimeline(fx.closure.DetectorTimeline)
	sortTimeline(timeline)
	firstOrder := cloneTimeline(timeline)
	sortTimeline(timeline)
	if !reflect.DeepEqual(firstOrder, timeline) {
		t.Fatalf("timeline sort is not idempotent")
	}
	before, after, onlyHealth := timelineSummary(timeline)
	if before != fx.closure.IncidentDetectorFiredBeforeDispatch {
		t.Fatalf("before-dispatch boolean mismatch: recorded %v, re-derived %v",
			fx.closure.IncidentDetectorFiredBeforeDispatch, before)
	}
	if after != fx.closure.IncidentDetectorFiredAfterDispatch {
		t.Fatalf("after-dispatch boolean mismatch: recorded %v, re-derived %v",
			fx.closure.IncidentDetectorFiredAfterDispatch, after)
	}
	if onlyHealth != fx.closure.NoIncidentDetectorFired {
		t.Fatalf("no-incident-detector boolean mismatch: recorded %v, re-derived %v",
			fx.closure.NoIncidentDetectorFired, onlyHealth)
	}
}

// replayStateRoundTrip loads the frozen RuntimeState into the production
// ConfigMapStateStore backed by a fake clientset (no API server), then reads it
// back, and asserts the round-trip is faithful and byte-stable: the loaded
// state marshals to the same bytes as the frozen one, and a second Save at the
// loaded revision is the store's digest no-op (same revision, no write). This
// is the restart-resume path the chaos suite exercises live, pinned here to a
// recorded state.
func replayStateRoundTrip(t *testing.T, fx replayFixture) {
	t.Helper()
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	store := NewConfigMapStateStore(client, "demo", "sdo-controller-state")

	revision, err := store.Save(ctx, *fx.state, "")
	if err != nil {
		t.Fatalf("save recorded state: %v", err)
	}
	loaded, loadedRevision, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load recorded state: %v", err)
	}
	if loadedRevision != revision {
		t.Fatalf("revision drifted on load: saved %s, loaded %s", revision, loadedRevision)
	}
	assertStateBytesEqual(t, *fx.state, loaded)

	// A second save of the same state at the loaded revision is the store's
	// digest no-op: the revision must not advance.
	again, err := store.Save(ctx, loaded, loadedRevision)
	if err != nil {
		t.Fatalf("re-save loaded state: %v", err)
	}
	if again != loadedRevision {
		t.Fatalf("re-saving an unchanged state advanced the revision: %s -> %s", loadedRevision, again)
	}
}

// TestReplayStateRoundTripContract exercises the state-round-trip dimension
// directly. The stored runs expose no per-incident RuntimeState dump (the
// controller's state ConfigMap is not exported), so no golden fixture lights
// this dimension up today; this self-test pins the dimension against a
// representative valid state so the shipped path is covered and the
// ConfigMapStateStore round-trip contract stays guarded. When a real
// RuntimeState is ever captured, dropping it in as state.json replays here
// unchanged.
func TestReplayStateRoundTripContract(t *testing.T) {
	now := time.Unix(1_790_931_114, 0).UTC()
	state := RuntimeState{
		Version:            RuntimeStateVersion,
		SchedulerDeadlines: map[string]time.Time{"health-objective": now},
		FindingStates: map[string]FindingState{
			"health-objective/deployment-unavailable/hotel-reservation/geo": {
				DetectorID: "health-objective", FiringCount: 2, Active: true,
				FirstSeenAt: now, Activations: 1,
			},
		},
		PendingBatch:        BatcherState{Findings: []sdk.Finding{}},
		EvaluationIteration: 42,
		DetectorTimeline: []DetectorTimelineEntry{{
			DetectorID: "health-objective", DetectorClass: "health", Owner: "health_judge",
			RuleID: "deployment-unavailable", Fingerprint: "health-objective/deployment-unavailable/hotel-reservation/geo",
			FirstActivatedAt: now, LastSeenAt: now, Relation: RelationBeforeDispatch, Activations: 1,
		}},
	}
	if err := state.Validate(); err != nil {
		t.Fatalf("representative state must be valid: %v", err)
	}
	replayStateRoundTrip(t, replayFixture{name: "state-round-trip-contract", state: &state})
}

// replayLifecycleStreamDurable re-emits the recorded lifecycle stream through
// the production FileLifecycleSink and asserts the durable result is the record,
// then that a restart over the same file replays every event idempotently and
// leaves the stream byte-for-byte unchanged. This pins the crash-replay contract
// the sink documents: RecordLifecycle is idempotent in EventID.
func replayLifecycleStreamDurable(t *testing.T, fx replayFixture) {
	t.Helper()
	path := filepath.Join(t.TempDir(), replayLifecycleFile)
	sink, err := NewFileLifecycleSink(path, DefaultLifecycleStreamBytes)
	if err != nil {
		t.Fatalf("new lifecycle sink: %v", err)
	}
	for _, event := range fx.lifecycle {
		if err := sink.RecordLifecycle(event); err != nil {
			t.Fatalf("record %s: %v", event.EventID, err)
		}
	}
	got := readLifecycleStream(t, path)
	assertLifecycleEventsEqual(t, fx.lifecycle, got)

	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read durable lifecycle stream: %v", err)
	}
	restart, err := NewFileLifecycleSink(path, DefaultLifecycleStreamBytes)
	if err != nil {
		t.Fatalf("restart lifecycle sink: %v", err)
	}
	for _, event := range fx.lifecycle {
		if err := restart.RecordLifecycle(event); err != nil {
			t.Fatalf("replay event %s after restart: %v", event.EventID, err)
		}
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read durable lifecycle stream after restart: %v", err)
	}
	if string(before) != string(after) {
		t.Fatalf("replay after restart mutated the lifecycle stream (%d -> %d bytes)", len(before), len(after))
	}
}

// replayLifecycleEventIDAttribution re-derives every lifecycle event's id from
// its own durable content -- the incident id, the phase, and (for a supersede)
// the superseding child -- and asserts it equals the recorded id and is unique
// across the stream. The id is content-addressed, so a crashed controller
// re-emitting a transition reproduces the identical id and the sink drops it.
func replayLifecycleEventIDAttribution(t *testing.T, fx replayFixture) {
	t.Helper()
	seen := map[string]LifecycleEvent{}
	for _, event := range fx.lifecycle {
		want := lifecycleEventID(event.IncidentID, string(event.Event))
		if event.Event == PhaseSuperseded {
			want = lifecycleEventID(event.IncidentID, string(event.Event), event.SupersededBy)
		}
		if want != event.EventID {
			t.Fatalf("lifecycle id for %s/%s mismatch: recorded %s, re-derived %s",
				event.IncidentID, event.Event, event.EventID, want)
		}
		if prior, dup := seen[event.EventID]; dup {
			t.Fatalf("lifecycle id %s is not unique: %s/%s and %s/%s",
				event.EventID, prior.IncidentID, prior.Event, event.IncidentID, event.Event)
		}
		seen[event.EventID] = event
	}
}

// replayIncidentTimeline reconstructs each incident's timeline from the frozen
// lifecycle and firing streams and asserts the reconstruction is deterministic
// (stable under a re-run), scoped to the incident, and non-decreasing in time.
func replayIncidentTimeline(t *testing.T, fx replayFixture) {
	t.Helper()
	incidentIDs := map[string]struct{}{}
	for _, event := range fx.lifecycle {
		if event.IncidentID != "" {
			incidentIDs[event.IncidentID] = struct{}{}
		}
	}
	for incidentID := range incidentIDs {
		timeline := ReconstructIncidentTimeline(incidentID, fx.lifecycle, fx.firings)
		if len(timeline) == 0 {
			t.Fatalf("incident %s reconstructed to an empty timeline", incidentID)
		}
		again := ReconstructIncidentTimeline(incidentID, fx.lifecycle, fx.firings)
		if len(again) != len(timeline) {
			t.Fatalf("incident %s reconstruction is not deterministic", incidentID)
		}
		for index := range timeline {
			if timeline[index].EventID != again[index].EventID {
				t.Fatalf("incident %s reconstruction order is not stable at %d", incidentID, index)
			}
			if index > 0 && timeline[index].At.Before(timeline[index-1].At) {
				t.Fatalf("incident %s timeline goes backwards in time at %d", incidentID, index)
			}
			switch {
			case timeline[index].Lifecycle != nil && timeline[index].Lifecycle.IncidentID != incidentID:
				t.Fatalf("incident %s timeline leaked lifecycle event for %s", incidentID, timeline[index].Lifecycle.IncidentID)
			case timeline[index].Firing != nil && timeline[index].Firing.IncidentID != incidentID:
				t.Fatalf("incident %s timeline leaked firing for %s", incidentID, timeline[index].Firing.IncidentID)
			}
		}
	}
}

// assertLifecycleEventsEqual asserts two lifecycle streams are equal
// event-by-event in order, reporting the first divergence.
func assertLifecycleEventsEqual(t *testing.T, want []LifecycleEvent, got []LifecycleEvent) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("lifecycle stream length mismatch: recorded %d, durable %d", len(want), len(got))
	}
	for index := range want {
		if !reflect.DeepEqual(want[index], got[index]) {
			t.Fatalf("lifecycle event %d diverged after a durable round-trip:\n recorded %+v\n durable  %+v",
				index, want[index], got[index])
		}
	}
}

// TestReplayLifecycleContract exercises the lifecycle replay dimensions
// directly. No stored run exposes a frozen lifecycle stream yet, so no golden
// fixture lights these dimensions up today; this self-test pins them against a
// representative incident (open through acknowledgment, plus a superseded
// parent) so the shipped path is covered. When a real lifecycle stream is ever
// captured, dropping it in as lifecycle-events.jsonl replays here unchanged.
func TestReplayLifecycleContract(t *testing.T) {
	at := func(sec int64) time.Time { return time.Unix(1_790_931_114+sec, 0).UTC() }
	lifecycle := []LifecycleEvent{
		{SchemaVersion: LifecycleSchemaVersion, EventID: lifecycleEventID("demo-1", string(PhaseOpened)),
			Event: PhaseOpened, IncidentID: "demo-1", Application: "demo", Namespace: "demo", RecordedAt: at(0)},
		{SchemaVersion: LifecycleSchemaVersion, EventID: lifecycleEventID("demo-1", string(PhaseSuperseded), "demo-1-f1"),
			Event: PhaseSuperseded, IncidentID: "demo-1", Application: "demo", Namespace: "demo",
			SupersededBy: "demo-1-f1", Reason: "follow_up", RecordedAt: at(1)},
		{SchemaVersion: LifecycleSchemaVersion, EventID: lifecycleEventID("demo-1", string(PhaseWorktreeReleased)),
			Event: PhaseWorktreeReleased, IncidentID: "demo-1", Application: "demo", Namespace: "demo", RecordedAt: at(2)},
		{SchemaVersion: LifecycleSchemaVersion, EventID: lifecycleEventID("demo-1-f1", string(PhaseOpened)),
			Event: PhaseOpened, IncidentID: "demo-1-f1", Application: "demo", Namespace: "demo",
			ParentIncidentID: "demo-1", FollowUpAttempt: 1, RecordedAt: at(1)},
		{SchemaVersion: LifecycleSchemaVersion, EventID: lifecycleEventID("demo-1-f1", string(PhaseAcknowledged)),
			Event: PhaseAcknowledged, IncidentID: "demo-1-f1", Application: "demo", Namespace: "demo",
			OutcomeCommit: "outcome-sha", RecordedAt: at(3)},
	}
	firings := []FiringRecord{
		{SchemaVersion: FiringSchemaVersion, EventID: firingEventID("demo-1-f1", "health", "fp", string(FiringBatched)),
			Event: FiringBatched, IncidentID: "demo-1-f1", RecordedAt: at(1)},
	}
	fx := replayFixture{name: "lifecycle-contract", lifecycle: lifecycle, firings: firings}
	replayLifecycleStreamDurable(t, fx)
	replayLifecycleEventIDAttribution(t, fx)
	replayIncidentTimeline(t, fx)
}

// assertFiringRecordsEqual asserts two firing streams are equal record-by-record
// in order, reporting the first divergence.
func assertFiringRecordsEqual(t *testing.T, want []FiringRecord, got []FiringRecord) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("stream length mismatch: recorded %d, durable %d", len(want), len(got))
	}
	for index := range want {
		if !reflect.DeepEqual(want[index], got[index]) {
			t.Fatalf("record %d diverged after a durable round-trip:\n recorded %+v\n durable  %+v",
				index, want[index], got[index])
		}
	}
}

// assertStateBytesEqual asserts two runtime states serialize identically, the
// strongest faithfulness check for a durable round-trip.
func assertStateBytesEqual(t *testing.T, want RuntimeState, got RuntimeState) {
	t.Helper()
	wantBytes, err := encodeControllerState(want)
	if err != nil {
		t.Fatalf("encode recorded state: %v", err)
	}
	gotBytes, err := encodeControllerState(got)
	if err != nil {
		t.Fatalf("encode loaded state: %v", err)
	}
	if string(wantBytes) != string(gotBytes) {
		t.Fatalf("state round-trip is not byte-stable:\n recorded %s\n loaded   %s", wantBytes, gotBytes)
	}
}

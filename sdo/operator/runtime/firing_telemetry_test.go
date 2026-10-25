package runtime

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"sdo.dev/operator/runtime/internal/lifecycleio"

	"k8s.io/client-go/kubernetes/fake"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

type memorySink struct{ records []*contracts.FiringRecord }

func (s *memorySink) Record(record *contracts.FiringRecord) error {
	s.records = append(s.records, record)
	return nil
}

func (s *memorySink) events(event FiringEvent, detectorID string) []*contracts.FiringRecord {
	var result []*contracts.FiringRecord
	for _, record := range s.records {
		if record.GetEvent() == string(event) && record.GetDetectorId() == detectorID {
			result = append(result, record)
		}
	}
	return result
}

// testFiringRecord is the smallest record the FiringRecord contract accepts.
func testFiringRecord(eventID string) *contracts.FiringRecord {
	return &contracts.FiringRecord{
		SchemaVersion: FiringSchemaVersion, EventId: eventID, Event: string(FiringActivated),
		RecordedAt: contractTimestamp(time.Unix(1, 0).UTC()), DetectorId: "d", DispatchRelation: string(RelationNoIncident),
	}
}

func readStream(t *testing.T, path string) []*contracts.FiringRecord {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatal(err)
	}
	defer file.Close()
	var records []*contracts.FiringRecord
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		record := &contracts.FiringRecord{}
		if wire.DecodeLenientUnvalidated(scanner.Bytes(), record) == nil {
			records = append(records, record)
		}
	}
	return records
}

func TestFindingStateReportsTransitionsWithCounts(t *testing.T) {
	tracker := NewFindingStateTracker(2, 2)
	finding := stateFinding("fault")
	if changes := tracker.Observe(stateNow, "d", []sdk.Finding{finding}); len(changes.Transitions) != 0 {
		t.Fatalf("below threshold must not transition: %#v", changes.Transitions)
	}
	changes := tracker.Observe(stateNow, "d", []sdk.Finding{finding})
	if len(changes.Transitions) != 1 || changes.Transitions[0].Event != FiringActivated ||
		changes.Transitions[0].FiringCount != 2 || changes.Transitions[0].Sequence != 1 {
		t.Fatalf("activation transition: %#v", changes.Transitions)
	}
	tracker.Observe(stateNow, "d", nil)
	changes = tracker.Observe(stateNow, "d", nil)
	if len(changes.Transitions) != 1 || changes.Transitions[0].Event != FiringCleared ||
		changes.Transitions[0].ClearCount != 2 || changes.Transitions[0].Sequence != 1 {
		t.Fatalf("clear transition: %#v", changes.Transitions)
	}
}

func TestFindingStateReportsFindingsThatNeverReachedTheFiringThreshold(t *testing.T) {
	tracker := NewFindingStateTracker(3, 1)
	finding := stateFinding("blip")
	tracker.Observe(stateNow, "d", []sdk.Finding{finding})
	tracker.Observe(stateNow, "d", []sdk.Finding{finding})
	changes := tracker.Observe(stateNow, "d", nil)
	if len(changes.Transitions) != 1 || changes.Transitions[0].Event != FiringNotPersisted ||
		changes.Transitions[0].FiringCount != 2 {
		t.Fatalf("not_persisted transition: %#v", changes.Transitions)
	}
	if again := tracker.Observe(stateNow, "d", nil); len(again.Transitions) != 0 {
		t.Fatalf("not_persisted must be reported once: %#v", again.Transitions)
	}
}

func TestFileFiringSinkAppendsDeduplicatesAndSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "telemetry", "detector-firings.jsonl")
	sink, err := NewFileFiringSink(path, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	record := testFiringRecord("a")
	for _, id := range []string{"a", "a", "b"} {
		record.EventId = id
		if err := sink.Record(record); err != nil {
			t.Fatal(err)
		}
	}
	reopened, err := NewFileFiringSink(path, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	record.EventId = "b"
	if err := reopened.Record(record); err != nil {
		t.Fatal(err)
	}
	record.EventId = "c"
	if err := reopened.Record(record); err != nil {
		t.Fatal(err)
	}
	ids := []string{}
	for _, stored := range readStream(t, path) {
		ids = append(ids, stored.EventId)
	}
	if !equalStrings(ids, []string{"a", "b", "c"}) {
		t.Fatalf("stream ids = %v", ids)
	}
}

func TestFileFiringSinkRepairsATornLastLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f.jsonl")
	if err := os.WriteFile(path, []byte(`{"event_id":"a"}`+"\n"+`{"event_id":"tor`), 0o644); err != nil {
		t.Fatal(err)
	}
	sink, err := NewFileFiringSink(path, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Record(testFiringRecord("b")); err != nil {
		t.Fatal(err)
	}
	records := readStream(t, path)
	if len(records) != 2 || records[0].EventId != "a" || records[1].EventId != "b" {
		t.Fatalf("records after torn line: %#v", records)
	}
}

func TestFileFiringSinkStaysWithinItsSizeBound(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f.jsonl")
	const bound = 2048
	sink, err := NewFileFiringSink(path, bound)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 200; index++ {
		record := testFiringRecord(firingEventID("e", string(rune('a'+index%26)), time.Duration(index).String()))
		record.Fingerprint = "padding-padding-padding"
		if err := sink.Record(record); err != nil {
			t.Fatal(err)
		}
	}
	total := int64(0)
	for _, candidate := range []string{path, path + ".1"} {
		info, statErr := os.Stat(candidate)
		if statErr != nil {
			t.Fatalf("expected %s: %v", candidate, statErr)
		}
		if info.Size() > bound {
			t.Fatalf("%s is %d bytes, bound %d", candidate, info.Size(), bound)
		}
		total += info.Size()
	}
	if _, err := os.Stat(path + ".2"); err == nil {
		t.Fatal("rotation must keep a single predecessor")
	}
	if len(readStream(t, path)) == 0 {
		t.Fatal("live stream is empty after rotation")
	}
}

func TestFileFiringSinkStartFreshArchivesAnOrphanedStream(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f.jsonl")
	sink, _ := NewFileFiringSink(path, 1<<20)
	if err := sink.Record(testFiringRecord("a")); err != nil {
		t.Fatal(err)
	}
	if err := sink.StartFresh(); err != nil {
		t.Fatal(err)
	}
	if err := sink.Record(testFiringRecord("a")); err != nil {
		t.Fatal(err)
	}
	if len(readStream(t, path)) != 1 {
		t.Fatal("a fresh lifecycle must not be deduplicated against the archived stream")
	}
	if _, err := os.Stat(path + ".prev"); err != nil {
		t.Fatalf("archive missing: %v", err)
	}
}

func telemetryController(t *testing.T, sink FiringSink, dispatcher Dispatcher, detectors ...sdk.Detector) *Controller {
	t.Helper()
	controller, err := NewController(
		testControllerConfig(), detectors, staticProvider{snapshot: sdktest.Snapshot{}}, dispatcher, time.Unix(0, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	controller.SetFiringSink(sink)
	return controller
}

func incidentDetector(id string, samples ...sdk.Finding) *sequenceDetector {
	detector := controllerDetector(id, time.Second, samples...)
	detector.spec.Class = sdk.DetectorClassIncident
	detector.spec.Owner = sdk.DetectorOwnerResponder
	detector.spec.Persistence = sdk.PersistencePolicy{Firing: 2, Clearing: 2}
	detector.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning}
	detector.spec.OriginatingIncident = "incident-0"
	detector.spec.OriginatingCommit = "commit-0"
	return detector
}

func healthDetector(id string, samples ...sdk.Finding) *sequenceDetector {
	detector := controllerDetector(id, time.Second, samples...)
	detector.spec.Class = sdk.DetectorClassHealth
	detector.spec.Owner = sdk.DetectorOwnerHealthJudge
	detector.spec.Persistence = sdk.PersistencePolicy{Firing: 2, Clearing: 2}
	detector.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityCritical}
	detector.spec.OriginatingCommit = "health-objective"
	return detector
}

func step(t *testing.T, controller *Controller, second int) {
	t.Helper()
	if err := controller.Step(context.Background(), time.Unix(int64(second), 0), nil); err != nil {
		t.Fatalf("step %d: %v", second, err)
	}
}

func TestControllerRecordsActivationBatchingAndClearingWithDispatchRelation(t *testing.T) {
	cause := incidentDetector("cause", stateFinding("cause"), stateFinding("cause"), sdk.Finding{}, sdk.Finding{}, sdk.Finding{}, sdk.Finding{})
	health := healthDetector("health", stateFinding("health"), stateFinding("health"), stateFinding("health"),
		stateFinding("health"), sdk.Finding{}, sdk.Finding{})
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	sink := &memorySink{}
	controller := telemetryController(t, sink, dispatcher, cause, health)
	closed := make(chan IncidentClosure, 1)
	controller.OnIncidentClosed = func(closure IncidentClosure) { closed <- closure }

	step(t, controller, 0)
	step(t, controller, 1)
	executePendingEffect(t, controller)
	request := awaitRequest(t, dispatcher.requests)
	awaitDispatchCompletionQueued(t, controller)
	for second := 2; second < 6; second++ {
		step(t, controller, second)
	}

	activated := sink.events(FiringActivated, "cause")
	if len(activated) != 1 {
		t.Fatalf("cause activations: %#v", sink.records)
	}
	first := activated[0]
	if first.DetectorClass != "incident" || first.Owner != "responder" || first.RuleId != "cause" ||
		first.Fingerprint != "cause" || first.FiringCount != 2 || first.FiringThreshold != 2 ||
		first.ClearThreshold != 2 || first.EvaluationIteration != 2 || first.DispatchRelation != string(RelationNoIncident) ||
		first.IncidentId != "" || !first.RecordedAt.AsTime().Equal(time.Unix(1, 0).UTC()) {
		t.Fatalf("activation record: %#v", first)
	}
	batched := sink.events(FiringBatched, "cause")
	if len(batched) != 1 || batched[0].IncidentId != request.IncidentID || batched[0].DispatchRelation != string(RelationBeforeDispatch) {
		t.Fatalf("batched record: %#v", batched)
	}
	if len(sink.events(FiringBatched, "health")) != 0 {
		t.Fatal("a finding below the batching severity must not be marked batched")
	}
	cleared := sink.events(FiringCleared, "cause")
	if len(cleared) != 1 || cleared[0].ClearCount != 2 || cleared[0].DispatchRelation != string(RelationBeforeDispatch) {
		t.Fatalf("clear record: %#v", cleared)
	}
	if len(sink.events(FiringCleared, "health")) != 1 {
		t.Fatalf("health clear missing: %#v", sink.records)
	}

	var closure IncidentClosure
	select {
	case closure = <-closed:
	case <-time.After(time.Second):
		t.Fatal("no closure")
	}
	if len(closure.DetectorTimeline) != 2 {
		t.Fatalf("timeline: %#v", closure.DetectorTimeline)
	}
	byDetector := map[string]DetectorTimelineEntry{}
	for _, entry := range closure.DetectorTimeline {
		byDetector[entry.DetectorID] = entry
	}
	cause1 := byDetector["cause"]
	if cause1.Relation != RelationBeforeDispatch || cause1.ClearedAt == nil || cause1.DetectorClass != "incident" ||
		!cause1.FirstActivatedAt.Equal(time.Unix(1, 0).UTC()) || cause1.LastSeenAt.After(*cause1.ClearedAt) {
		t.Fatalf("cause timeline entry: %#v", cause1)
	}
	if byDetector["health"].Relation != RelationNoIncident || byDetector["health"].DetectorClass != "health" {
		t.Fatalf("health timeline entry: %#v", byDetector["health"])
	}
	if !closure.IncidentDetectorFiredBeforeDispatch || closure.IncidentDetectorFiredAfterDispatch ||
		closure.NoIncidentDetectorFired {
		t.Fatalf("closure booleans: %#v", closure)
	}
	if len(exportState(t, controller).DetectorTimeline) != 0 {
		t.Fatal("the timeline must reset once its closure is cut")
	}
}

func TestControllerClassifiesAnIncidentDetectorThatActivatesWhileTheResponderRuns(t *testing.T) {
	cause := incidentDetector("cause", stateFinding("cause"))
	late := incidentDetector("late", sdk.Finding{}, sdk.Finding{}, stateFinding("late"))
	dispatcher := &blockingDispatcher{requests: make(chan IncidentRequest, 1), release: make(chan struct{})}
	defer close(dispatcher.release)
	sink := &memorySink{}
	controller := telemetryController(t, sink, dispatcher, cause, late)
	step(t, controller, 0)
	step(t, controller, 1)
	executePendingEffect(t, controller)
	awaitRequest(t, dispatcher.requests)
	step(t, controller, 2)
	step(t, controller, 3)

	activated := sink.events(FiringActivated, "late")
	if len(activated) != 1 || activated[0].DispatchRelation != string(RelationAfterDispatch) || activated[0].IncidentId == "" {
		t.Fatalf("late activation: %#v", sink.records)
	}
	before, after, onlyHealth := timelineSummary(detectorTimelineState(exportState(t, controller).DetectorTimeline))
	if !before || !after || onlyHealth {
		t.Fatalf("summary before=%v after=%v onlyHealth=%v", before, after, onlyHealth)
	}
}

func TestTimelineSummaryFlagsHealthOnlyFiring(t *testing.T) {
	health := []DetectorTimelineEntry{{DetectorClass: "health", Relation: RelationNoIncident}}
	before, after, onlyHealth := timelineSummary(health)
	if before || after || !onlyHealth {
		t.Fatalf("health-only: %v %v %v", before, after, onlyHealth)
	}
	if _, _, empty := timelineSummary(nil); empty {
		t.Fatal("an empty timeline is not health-only firing")
	}
}

func TestControllerRecordsFindingsThatNeverPersisted(t *testing.T) {
	blip := incidentDetector("blip", stateFinding("blip"), sdk.Finding{}, sdk.Finding{})
	sink := &memorySink{}
	controller := telemetryController(t, sink, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, blip)
	step(t, controller, 0)
	step(t, controller, 1)
	records := sink.events(FiringNotPersisted, "blip")
	if len(records) != 1 || records[0].FiringCount != 1 || records[0].FiringThreshold != 2 {
		t.Fatalf("not_persisted: %#v", sink.records)
	}
	if len(sink.events(FiringActivated, "blip")) != 0 || len(exportState(t, controller).DetectorTimeline) != 0 {
		t.Fatal("a finding that never persisted must not enter the timeline")
	}
}

func TestFiringTelemetryDoesNotDuplicateAfterRestartOrReplay(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "detector-firings.jsonl")
	client := fake.NewSimpleClientset()
	build := func() (*Controller, *FileFiringSink) {
		sink, err := NewFileFiringSink(path, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		controller := telemetryController(t, sink, &recordingDispatcher{requests: make(chan IncidentRequest, 1)},
			incidentDetector("cause", stateFinding("cause")))
		if err := controller.AttachStateStore(ctx, lifecycleio.NewConfigMapStateStore(client, "demo", "state")); err != nil {
			t.Fatal(err)
		}
		return controller, sink
	}

	first, _ := build()
	step(t, first, 0)
	step(t, first, 1)
	if err := first.PersistState(ctx); err != nil {
		t.Fatal(err)
	}
	// Restart after a persisted state: the active finding must not re-activate.
	second, _ := build()
	if !second.RestoredFromState() || exportState(t, second).EvaluationIteration != 2 {
		t.Fatalf("state not restored: %#v", exportState(t, second))
	}
	step(t, second, 2)
	step(t, second, 3)
	if count := len(readStream(t, path)); count != 2 {
		t.Fatalf("restart re-emitted records: %d (want activated + batched)", count)
	}

	// Crash after emitting but before persisting: a controller with no durable
	// state replays the same transition and must not duplicate its record.
	replayPath := filepath.Join(t.TempDir(), "replay.jsonl")
	for run := 0; run < 2; run++ {
		sink, err := NewFileFiringSink(replayPath, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		controller := telemetryController(t, sink, &recordingDispatcher{requests: make(chan IncidentRequest, 1)},
			incidentDetector("cause", stateFinding("cause")))
		step(t, controller, 0)
		step(t, controller, 1)
	}
	var activations int
	for _, record := range readStream(t, replayPath) {
		if record.Event == string(FiringActivated) {
			activations++
		}
	}
	if activations != 1 {
		t.Fatalf("replayed activation duplicated: %d", activations)
	}
}

func TestControllerWithoutASinkStillBuildsTheClosureTimeline(t *testing.T) {
	cause := incidentDetector("cause", stateFinding("cause"), stateFinding("cause"), sdk.Finding{}, sdk.Finding{}, sdk.Finding{})
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	controller := telemetryController(t, nil, dispatcher, cause)
	closed := make(chan IncidentClosure, 1)
	controller.OnIncidentClosed = func(closure IncidentClosure) { closed <- closure }
	step(t, controller, 0)
	step(t, controller, 1)
	executePendingEffect(t, controller)
	awaitRequest(t, dispatcher.requests)
	awaitDispatchCompletionQueued(t, controller)
	for second := 2; second < 5; second++ {
		step(t, controller, second)
	}
	select {
	case closure := <-closed:
		if !closure.IncidentDetectorFiredBeforeDispatch || len(closure.DetectorTimeline) != 1 {
			t.Fatalf("closure: %#v", closure)
		}
	case <-time.After(time.Second):
		t.Fatal("no closure")
	}
}

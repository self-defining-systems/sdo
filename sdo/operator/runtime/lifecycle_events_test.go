package runtime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

type memoryLifecycleSink struct{ events []LifecycleEvent }

func (s *memoryLifecycleSink) RecordLifecycle(event LifecycleEvent) error {
	s.events = append(s.events, event)
	return nil
}

func (s *memoryLifecycleSink) byPhase(phase LifecyclePhase) []LifecycleEvent {
	var result []LifecycleEvent
	for _, event := range s.events {
		if event.Event == phase {
			result = append(result, event)
		}
	}
	return result
}

func readLifecycleStream(t *testing.T, path string) []LifecycleEvent {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatal(err)
	}
	var events []LifecycleEvent
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if line == "" {
			continue
		}
		var event LifecycleEvent
		if json.Unmarshal([]byte(line), &event) == nil {
			events = append(events, event)
		}
	}
	return events
}

// lifecycleController builds a broker-backed controller with both telemetry
// sinks and a fully controlled clock, so every lifecycle event -- including the
// broker-completion events timestamped from the controller clock -- lands at a
// deterministic instant. clk is advanced by the returned stepper; the broker
// completions read the latest clk, so same-tick events order by lifecycle rank.
func lifecycleController(
	t *testing.T, detectors ...sdk.Detector,
) (*Controller, *summarizingDispatcher, *recordingIncidentBroker, *memoryLifecycleSink, *memorySink, func(int64)) {
	t.Helper()
	dispatcher := &summarizingDispatcher{requests: make(chan IncidentRequest, 4)}
	config := testControllerConfig()
	config.VerificationTimeout = 10 * time.Second
	controller, err := NewController(
		config, detectors, staticProvider{snapshot: sdktest.Snapshot{}}, dispatcher, time.Unix(0, 0),
	)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	broker := &recordingIncidentBroker{
		workspace: IncidentWorkspace{Worktree: "/worktrees/incident", BaseCommit: "base-sha"},
		receipt: ClosureReceipt{
			Worktree: "/worktrees/incident", BaseCommit: "base-sha", ProposalCommit: "repair-sha",
			OutcomeCommit: "outcome-sha", ReflectionCommit: "reflect-sha", AckToken: "ack-token",
		},
	}
	if err := controller.SetIncidentBroker(broker); err != nil {
		t.Fatalf("set broker: %v", err)
	}
	lifecycle := &memoryLifecycleSink{}
	firing := &memorySink{}
	controller.SetLifecycleSink(lifecycle)
	controller.SetFiringSink(firing)
	const base = int64(1_000_000)
	clk := time.Unix(base, 0).UTC()
	controller.now = func() time.Time { return clk }
	step := func(sec int64) {
		clk = time.Unix(base+sec, 0).UTC()
		if err := controller.Step(context.Background(), clk, nil); err != nil {
			t.Fatalf("step %d: %v", sec, err)
		}
	}
	return controller, dispatcher, broker, lifecycle, firing, step
}

func lifecycleHealthDetector(id string, samples ...bool) *sequenceDetector {
	return followUpHealthDetector(id, samples...)
}

// TestLifecycleStreamEmitsEveryTransitionOnce drives one incident through the
// whole broker-backed lifecycle and asserts each transition emitted exactly one
// well-formed event with a stable content-addressed id, and that the emitted
// streams reconstruct the incident's full timeline in order.
func TestLifecycleStreamEmitsEveryTransitionOnce(t *testing.T) {
	health := lifecycleHealthDetector("health", true, true, false, false, false, false)
	controller, dispatcher, _, lifecycle, firing, step := lifecycleController(t, health)
	closed := make(chan IncidentClosure, 1)
	controller.OnIncidentClosed = func(closure IncidentClosure) { closed <- closure }

	step(0)
	step(1) // health reaches its firing threshold and opens the incident
	drainWorkspace(t, controller)
	executePendingEffect(t, controller)
	request := awaitRequest(t, dispatcher.requests)
	awaitDispatchCompletionQueued(t, controller)
	for second := int64(2); second <= 5; second++ {
		step(second) // health clears, verification succeeds, closure is cut
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("incident never closed")
	}
	// Commit and acknowledge the closure through the broker.
	closureEffect, ok := controller.PendingClosureEffect()
	if !ok {
		t.Fatal("closure was not pending after the incident closed")
	}
	if err := controller.ExecuteClosureEffect(context.Background(), closureEffect); err != nil {
		t.Fatalf("execute closure: %v", err)
	}
	awaitBrokerResult(t, controller.closureResults)
	controller.processBrokerCompletions()
	ack, ok := controller.PendingClosureAcknowledgmentEffect()
	if !ok {
		t.Fatal("acknowledgment was not pending after the closure committed")
	}
	if err := controller.ExecuteClosureAcknowledgmentEffect(context.Background(), ack); err != nil {
		t.Fatalf("execute acknowledgment: %v", err)
	}
	awaitBrokerResult(t, controller.acknowledgmentResults)
	controller.processBrokerCompletions()

	wantPhases := []LifecyclePhase{
		PhaseOpened, PhaseWorkspacePrepared, PhaseDispatched, PhaseResponderCompleted,
		PhaseClosed, PhaseClosureCommitted, PhaseAcknowledged,
	}
	for _, phase := range wantPhases {
		got := lifecycle.byPhase(phase)
		if len(got) != 1 {
			t.Fatalf("phase %q emitted %d times, want exactly 1: %#v", phase, len(got), lifecycle.events)
		}
		event := got[0]
		if event.SchemaVersion != LifecycleSchemaVersion || event.EventID == "" ||
			event.IncidentID != request.IncidentID || event.Application != "demo" || event.Namespace != "demo" {
			t.Fatalf("phase %q is not well-formed: %#v", phase, event)
		}
		if want := lifecycleEventID(request.IncidentID, string(phase)); event.EventID != want {
			t.Fatalf("phase %q id %q is not the content-addressed id %q", phase, event.EventID, want)
		}
	}
	if committed := lifecycle.byPhase(PhaseClosureCommitted)[0]; committed.OutcomeCommit != "outcome-sha" ||
		committed.ProposalCommit != "repair-sha" || committed.ReflectionCommit != "reflect-sha" {
		t.Fatalf("closure_committed did not link the outcome commits: %#v", committed)
	}
	if prepared := lifecycle.byPhase(PhaseWorkspacePrepared)[0]; prepared.Worktree != "/worktrees/incident" ||
		prepared.BaseCommit != "base-sha" {
		t.Fatalf("workspace_prepared did not record the prepared worktree: %#v", prepared)
	}

	// Reconstruct the incident's full timeline from the emitted streams and
	// assert the lifecycle phases appear in order.
	timeline := ReconstructIncidentTimeline(request.IncidentID, lifecycle.events, firing.records)
	var phaseOrder []LifecyclePhase
	for _, entry := range timeline {
		if entry.Source == "lifecycle" {
			phaseOrder = append(phaseOrder, entry.Lifecycle.Event)
		}
	}
	if !equalPhases(phaseOrder, wantPhases) {
		t.Fatalf("reconstructed lifecycle order = %v, want %v", phaseOrder, wantPhases)
	}
	if timeline[0].Kind != string(PhaseOpened) {
		t.Fatalf("reconstructed timeline does not start at incident_opened: %#v", timeline[0])
	}
	if last := timeline[len(timeline)-1]; last.Kind != string(PhaseAcknowledged) {
		t.Fatalf("reconstructed timeline does not end at incident_acknowledged: %#v", last)
	}
	assertLifecycleTransportNeutral(t, lifecycle.events)
}

// TestLifecycleStreamRecordsSupersedeAndRelease drives a follow-up that
// supersedes an open incident in place and strands the parent worktree, and
// asserts the stranded-worktree lifecycle -- the exact class of bug (a worktree
// leak on supersede) this stream exists to make diagnosable in seconds -- is
// fully observable: the parent is superseded once, the follow-up opens once with
// its parent linked, and the stranded worktree is released once.
func TestLifecycleStreamRecordsSupersedeAndRelease(t *testing.T) {
	controller, dispatcher := newFollowUpController(t, 2, healthBPersistent())
	broker := &recordingIncidentBroker{workspace: IncidentWorkspace{Worktree: "/worktrees/incident", BaseCommit: "base"}}
	if err := controller.SetIncidentBroker(broker); err != nil {
		t.Fatalf("set broker: %v", err)
	}
	lifecycle := &memoryLifecycleSink{}
	controller.SetLifecycleSink(lifecycle)

	first := completeFirstResponderWithBroker(t, controller, dispatcher)
	stepAt(t, controller, 3) // the residual health finding supersedes the parent
	drainRelease(t, controller)

	superseded := lifecycle.byPhase(PhaseSuperseded)
	if len(superseded) != 1 || superseded[0].IncidentID != first.IncidentID || superseded[0].SupersededBy == "" {
		t.Fatalf("parent supersede not recorded exactly once: %#v", lifecycle.events)
	}
	childID := superseded[0].SupersededBy
	if superseded[0].EventID != lifecycleEventID(first.IncidentID, string(PhaseSuperseded), childID) {
		t.Fatalf("supersede id is not content-addressed over the parent and child: %#v", superseded[0])
	}
	released := lifecycle.byPhase(PhaseWorktreeReleased)
	if len(released) != 1 || released[0].IncidentID != first.IncidentID {
		t.Fatalf("stranded parent worktree release not recorded exactly once: %#v", lifecycle.events)
	}
	var childOpened *LifecycleEvent
	for index := range lifecycle.events {
		event := lifecycle.events[index]
		if event.Event == PhaseOpened && event.IncidentID == childID {
			childOpened = &lifecycle.events[index]
		}
	}
	if childOpened == nil || childOpened.ParentIncidentID != first.IncidentID || childOpened.FollowUpAttempt != 1 {
		t.Fatalf("follow-up open did not link its superseded parent: %#v", lifecycle.events)
	}
	// The parent's timeline reconstructs through its supersede; the child's is a
	// separate incident id reachable via ParentIncidentID / SupersededBy.
	parentTimeline := ReconstructIncidentTimeline(first.IncidentID, lifecycle.events, nil)
	if last := parentTimeline[len(parentTimeline)-1]; last.Kind != string(PhaseWorktreeReleased) {
		t.Fatalf("parent timeline does not end at worktree_released: %#v", last)
	}
	assertLifecycleTransportNeutral(t, lifecycle.events)
}

// TestLifecycleFileSinkIsIdempotentAcrossReopen pins the crash-replay contract:
// re-emitting every event after reopening the durable sink is a no-op, so the
// stream is byte-for-byte unchanged, exactly as a restarted controller relies on.
func TestLifecycleFileSinkIsIdempotentAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "telemetry", "lifecycle-events.jsonl")
	sink, err := NewFileLifecycleSink(path, DefaultLifecycleStreamBytes)
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	events := []LifecycleEvent{
		{SchemaVersion: LifecycleSchemaVersion, EventID: lifecycleEventID("inc", string(PhaseOpened)),
			Event: PhaseOpened, IncidentID: "inc", RecordedAt: time.Unix(1, 0).UTC()},
		{SchemaVersion: LifecycleSchemaVersion, EventID: lifecycleEventID("inc", string(PhaseDispatched)),
			Event: PhaseDispatched, IncidentID: "inc", RecordedAt: time.Unix(2, 0).UTC()},
		{SchemaVersion: LifecycleSchemaVersion, EventID: lifecycleEventID("inc", string(PhaseAcknowledged)),
			Event: PhaseAcknowledged, IncidentID: "inc", RecordedAt: time.Unix(3, 0).UTC()},
	}
	for _, event := range events {
		if err := sink.RecordLifecycle(event); err != nil {
			t.Fatalf("record: %v", err)
		}
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read durable stream: %v", err)
	}
	reopened, err := NewFileLifecycleSink(path, DefaultLifecycleStreamBytes)
	if err != nil {
		t.Fatalf("reopen sink: %v", err)
	}
	for _, event := range events {
		if err := reopened.RecordLifecycle(event); err != nil {
			t.Fatalf("re-record after reopen: %v", err)
		}
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read durable stream after reopen: %v", err)
	}
	if string(before) != string(after) {
		t.Fatalf("re-emitting after reopen mutated the durable stream (%d -> %d bytes)", len(before), len(after))
	}
	if got := readLifecycleStream(t, path); len(got) != len(events) {
		t.Fatalf("durable stream has %d events, want %d", len(got), len(events))
	}
}

// TestReconstructIncidentTimelineOrdersBothStreams is the deterministic unit
// test of the offline reconstruction: it joins a lifecycle stream and a firing
// stream for one incident id, orders by recorded-at with a lifecycle-rank
// tie-break, drops other incidents, and interleaves firings between opened and
// closed.
func TestReconstructIncidentTimelineOrdersBothStreams(t *testing.T) {
	at := func(sec int64) time.Time { return time.Unix(sec, 0).UTC() }
	lifecycle := []LifecycleEvent{
		{Event: PhaseAcknowledged, IncidentID: "inc", RecordedAt: at(3)},
		{Event: PhaseOpened, IncidentID: "inc", RecordedAt: at(1)},
		{Event: PhaseClosed, IncidentID: "inc", RecordedAt: at(3)},
		{Event: PhaseDispatched, IncidentID: "inc", RecordedAt: at(1)}, // same tick as opened
		{Event: PhaseOpened, IncidentID: "other", RecordedAt: at(1)},   // different incident, dropped
	}
	firings := []FiringRecord{
		{Event: FiringBatched, IncidentID: "inc", RecordedAt: at(2)},
		{Event: FiringActivated, IncidentID: "other", RecordedAt: at(2)}, // dropped
	}
	timeline := ReconstructIncidentTimeline("inc", lifecycle, firings)
	var kinds []string
	for _, entry := range timeline {
		if entry.Lifecycle != nil && entry.Lifecycle.IncidentID != "inc" {
			t.Fatalf("reconstruction leaked another incident: %#v", entry.Lifecycle)
		}
		if entry.Firing != nil && entry.Firing.IncidentID != "inc" {
			t.Fatalf("reconstruction leaked another incident's firing: %#v", entry.Firing)
		}
		kinds = append(kinds, entry.Kind)
	}
	want := []string{
		string(PhaseOpened), string(PhaseDispatched), string(FiringBatched),
		string(PhaseClosed), string(PhaseAcknowledged),
	}
	if strings.Join(kinds, ",") != strings.Join(want, ",") {
		t.Fatalf("reconstructed order = %v, want %v", kinds, want)
	}
}

func equalPhases(got []LifecyclePhase, want []LifecyclePhase) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range got {
		if got[index] != want[index] {
			return false
		}
	}
	return true
}

// assertLifecycleTransportNeutral fails if any emitted event carries a benchmark
// or submission term, guarding sdo/operator/runtime's transport neutrality: the
// stream is derived only from the controller's own lifecycle state.
func assertLifecycleTransportNeutral(t *testing.T, events []LifecycleEvent) {
	t.Helper()
	forbidden := []string{"verdict", "benchmark", "sregym", "resolution", "oracle", "submission", "receipt"}
	for _, event := range events {
		payload, err := json.Marshal(event)
		if err != nil {
			t.Fatalf("marshal lifecycle event: %v", err)
		}
		lower := strings.ToLower(string(payload))
		for _, term := range forbidden {
			if strings.Contains(lower, term) {
				t.Fatalf("lifecycle event carries a non-neutral term %q: %s", term, payload)
			}
		}
	}
}

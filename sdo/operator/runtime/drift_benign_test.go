package runtime

import (
	"context"
	"encoding/json"
	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
	"time"
)

func benignFixture(t *testing.T) (sdk.DriftEvent, sdk.Finding) {
	t.Helper()
	f := unexplainedFinding()
	event := driftEventsForFindings(nil, []sdk.Finding{f})[0]
	event.Manager = "operator"
	event.Fields = []sdk.DriftField{{Path: "spec.ports[0].targetPort", Old: "8080", Source: "8080", New: "8081"}}
	event.InvestigationStatus = "completed"
	event.InvestigationVerdict = "benign"
	event.InvestigationReason = "Verified maintenance listener serves the accepted clients"
	event.InvestigationEvidence = []sdk.DriftEvidence{{Kind: "observed_impact", Observation: "live connectivity succeeds on both the accepted and maintenance listener"}}
	f.Metadata = map[string]any{"drift_event": event}
	return event, f
}
func benignDiff(event sdk.DriftEvent) program.Diff {
	accepted, _ := json.Marshal(map[string]any{"spec": map[string]any{"ports": []any{map[string]any{"targetPort": 8080}}}})
	observed, _ := json.Marshal(map[string]any{"spec": map[string]any{"ports": []any{map[string]any{"targetPort": 8081}}}})
	return program.Diff{Changed: true, Text: "accepted: " + string(accepted) + "\nobserved: " + string(observed)}
}

func TestHermeticBenignDriftAcrossFastloopResetAndNextIncident(t *testing.T) {
	ctx := context.Background()
	start := time.Unix(100, 0).UTC()
	root := t.TempDir()
	event, _ := benignFixture(t)
	// Use the real outcome store, then construct fresh controllers as reset does.
	// No controller-state or verdict ConfigMap carries the label across reset.
	store := memory.NewJSONStore(root)
	record := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "benign-prior", Classification: "cleared_without_action", DriftEvents: driftEventsContract([]sdk.DriftEvent{event})}
	if _, err := store.Put(ctx, record); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name       string
		change     func(*sdk.DriftEvent)
		suppressed bool
	}{
		{"exact", func(*sdk.DriftEvent) {}, true},
		{"timestamp old and unrelated commit", func(e *sdk.DriftEvent) { e.ObservedAt = start.Add(time.Hour); e.Fields[0].Old = "1234" }, true},
		{"changed current", func(e *sdk.DriftEvent) { e.Fields[0].New = "9999" }, false},
		{"changed source", func(e *sdk.DriftEvent) { e.Fields[0].Source = "8082" }, false},
		{"changed manager", func(e *sdk.DriftEvent) { e.Manager = "someone-else" }, false},
		{"new key", func(e *sdk.DriftEvent) { e.Ref.Name = "peer" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observed := driftEventsState(driftEventsContract([]sdk.DriftEvent{event}))[0]
			tc.change(&observed)
			observed.InvestigationVerdict = ""
			observed.InvestigationStatus = ""
			config := testControllerConfig()
			config.RepositoryWorktree = root
			config.SourceCommit = "unrelated-outcome-commit"
			c, err := NewController(config, nil, staticProvider{snapshot: sdktest.Snapshot{}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
			if err != nil {
				t.Fatal(err)
			}
			c.ConfigureDrift(DriftControllerConfig{ProgramPersistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, BatchWindow: time.Nanosecond, View: func(sdk.DetectionContext) program.ReadView {
				return driftReadView{ReplayReadView: &sdktest.ReplayReadView{}, diff: benignDiff(observed)}
			}})
			c.queueUnexplainedFinding(start, observed)
			if err := c.StepEvents(ctx, start.Add(time.Second), nil); err != nil {
				t.Fatal(err)
			}
			if c.IncidentOpen() == tc.suppressed {
				t.Fatalf("exact benign suppression=%v state=%+v", tc.suppressed, exportState(t, c))
			}
		})
	}
	// A fault/unsure label, or rollback to a seed without outcomes, never suppresses.
	for _, verdict := range []string{"bug", "unsure", "seed-rollback"} {
		t.Run(verdict, func(t *testing.T) {
			repository := t.TempDir()
			copy := proto.Clone(record).(*contracts.OutcomeRecord)
			copy.DriftEvents[0].InvestigationVerdict = verdict
			if verdict != "seed-rollback" {
				if _, err := memory.NewJSONStore(repository).Put(ctx, copy); err != nil {
					t.Fatal(err)
				}
			}
			config := testControllerConfig()
			config.RepositoryWorktree = repository
			c, err := NewController(config, nil, staticProvider{snapshot: sdktest.Snapshot{}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
			if err != nil {
				t.Fatal(err)
			}
			c.ConfigureDrift(DriftControllerConfig{ProgramPersistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, BatchWindow: time.Nanosecond, View: func(sdk.DetectionContext) program.ReadView {
				return driftReadView{ReplayReadView: &sdktest.ReplayReadView{}, diff: benignDiff(event)}
			}})
			c.queueUnexplainedFinding(start, event)
			if err := c.StepEvents(ctx, start.Add(time.Second), nil); err != nil {
				t.Fatal(err)
			}
			if !c.IncidentOpen() {
				t.Fatal("non-benign or discarded label suppressed a fault")
			}
		})
	}
}

func TestCheckedBenignNoopClosesWithoutMitigationCredit(t *testing.T) {
	ctx := context.Background()
	start := time.Unix(100, 0).UTC()
	event, _ := benignFixture(t)
	config := testControllerConfig()
	config.FiringThreshold = 1
	config.ClearThreshold = 1
	c, err := NewController(config, nil, staticProvider{snapshot: sdktest.Snapshot{}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
	if err != nil {
		t.Fatal(err)
	}
	c.ConfigureDrift(DriftControllerConfig{ProgramPersistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, BatchWindow: time.Nanosecond, View: func(sdk.DetectionContext) program.ReadView {
		return driftReadView{ReplayReadView: &sdktest.ReplayReadView{}, diff: benignDiff(event)}
	}})
	c.queueUnexplainedFinding(start, event)
	if err := c.StepEvents(ctx, start.Add(time.Second), nil); err != nil {
		t.Fatal(err)
	}
	request := exportState(t, c).IncidentRequest
	if request == nil {
		t.Fatal("missing ordinary drift incident")
	}
	c.dispatcher = &fixedResultDispatcher{requests: make(chan IncidentRequest, 1), result: func(r IncidentRequest) IncidentResult {
		result := completedResult(r.IncidentID)
		result.DriftEvents = []sdk.DriftEvent{event}
		return result
	}}
	executePendingEffect(t, c)
	awaitDispatchCompletionQueued(t, c)
	if err := c.StepEvents(ctx, start.Add(3*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	closure, ok := c.PendingIncidentClosure()
	if !ok {
		t.Fatal("checked benign noop did not close")
	}
	derived, err := (ProtoOutcomeDeriver{}).Derive(ctx, closure, "", "test", "test")
	if err != nil || derived.Record.GetClassification() != "cleared_without_action" || derived.RequiresReflection {
		t.Fatalf("benign no-op was credited: %+v %v", derived, err)
	}
}

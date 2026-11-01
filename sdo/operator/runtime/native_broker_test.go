package runtime

import (
	"context"
	"errors"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"testing"
	"time"
)

func testDerivedOutcome(id, classification string, reflect bool) DerivedOutcome {
	return DerivedOutcome{Record: &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: id, Classification: classification}, RequiresReflection: reflect}
}

type rejectingLatePull struct{}

func (rejectingLatePull) VerifyPull(context.Context, string, uint32, string, []string, []string) error {
	return errors.New("stale report digest")
}

func nativeClosure(workspace IncidentWorkspace) IncidentClosure {
	now := time.Unix(1700000000, 0).UTC()
	return IncidentClosure{
		Request: IncidentRequest{
			SchemaVersion: ProtocolSchemaVersion, Application: "app", Namespace: "default", IncidentID: workspace.IncidentID,
			Findings:        []sdk.Finding{stateFinding("health")},
			DetectorHistory: []DetectorEvaluation{{DetectorID: "health", EvaluatedAt: now, Status: DetectorEvaluationFiring}}, SourceCommit: workspace.BaseCommit, DeployedCommit: workspace.BaseCommit,
			RepositoryWorktree: workspace.Worktree, RepositoryBaseCommit: workspace.BaseCommit,
			ResponseDeadline: now.Add(time.Minute), CancellationToken: "token", RepairPolicy: "commit",
		},
		Result: &IncidentResult{
			SchemaVersion: ProtocolSchemaVersion, IncidentID: workspace.IncidentID, Status: IncidentCompleted,
			Timing: TimingMetrics{StartedAt: now, CompletedAt: now.Add(time.Second)},
		},
		DetectedAt: now, DispatchedAt: now, ResponderCompletedAt: now.Add(time.Second), VerifiedAt: now.Add(time.Second),
	}
}

type priorOutcomeReader struct{ outcomes []memory.Outcome }

func (r priorOutcomeReader) List(context.Context) ([]memory.Outcome, error) { return r.outcomes, nil }

func (r priorOutcomeReader) Get(context.Context, string) (memory.Outcome, error) {
	return memory.Outcome{}, memory.ErrNotFound
}

func TestProtoOutcomeDeriverAcceptsRuntimeClosure(t *testing.T) {
	closure := nativeClosure(IncidentWorkspace{IncidentID: "inc-proto", Worktree: "/proposal", BaseCommit: "base"})
	derived, err := (ProtoOutcomeDeriver{}).Derive(context.Background(), closure, "", "test", "test")
	if err != nil {
		t.Fatal(err)
	}
	if derived.Record.GetIncidentId() != "inc-proto" {
		t.Fatalf("wrong outcome: %+v", derived.Record)
	}
}

func nativeDetectorManifest(class, id, incident string) string {
	owner, origin := "health_judge", ""
	if class == "incident" {
		owner, origin = "responder", "originatingIncident: "+incident+"\n"
	}
	return "apiVersion: sdo.dev/v1alpha1\nkind: DetectorManifest\nsdkVersion: v0.1\nid: " + id + "\nconstructor: New\nclass: " + class + "\nowner: " + owner + "\nwatches:\n  - apiVersion: v1\n    kind: Pod\ninterval: 30s\npersistence:\n  firing: 1\n  clearing: 1\nbatching:\n  severity: critical\n  debounce: 500ms\npossiblePlaybooks:\n  - base\n" + origin + "originatingCommit: initial\n"
}

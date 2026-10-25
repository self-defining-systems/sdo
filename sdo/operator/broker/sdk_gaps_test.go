package broker

import (
	"context"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"testing"
)

func TestRecordSDKGapsIsAppendOnlyAcrossBrokerRestart(t *testing.T) {
	ctx := context.Background()
	target := repository(t)
	workspace := filepath.Join(t.TempDir(), "broker")
	b, err := New(target, workspace, nil)
	if err != nil {
		t.Fatal(err)
	}
	original := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "incident", Classification: "success", SourceCommit: "source"}
	if _, err = b.RecordOutcome(ctx, "incident", original); err != nil {
		t.Fatal(err)
	}
	peer := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "peer", Classification: "failed"}
	if _, err = b.RecordOutcome(ctx, "peer", peer); err != nil {
		t.Fatal(err)
	}
	gap := &contracts.SdkGap{FaultClass: "db", Primitive: "operation", Reason: "SDK gap", ScratchScript: "fixture operation"}
	first, err := b.RecordSDKGaps(ctx, "incident", []*contracts.SdkGap{gap})
	if err != nil {
		t.Fatal(err)
	}
	b, err = New(target, workspace, nil)
	if err != nil {
		t.Fatal(err)
	}
	again, err := b.RecordSDKGaps(ctx, "incident", []*contracts.SdkGap{gap})
	if err != nil || again.Commit != first.Commit {
		t.Fatalf("retry created another annotation: %+v %v", again, err)
	}
	if _, err = b.RecordOutcome(ctx, "incident", original); err != nil {
		t.Fatal(err)
	}
	if head := git(t, target, "rev-parse", "HEAD"); head != first.Commit {
		t.Fatal("retry changed accepted HEAD")
	}
	got, err := memory.OpenReader(target).Get(ctx, "incident")
	if err != nil || got.Record.GetSourceCommit() != "source" || len(got.Record.GetSdkGaps()) != 1 {
		t.Fatal("incident evidence changed")
	}
	other, err := memory.OpenReader(target).Get(ctx, "peer")
	if err != nil || len(other.Record.GetSdkGaps()) != 0 {
		t.Fatal("annotation changed peer outcome")
	}
}

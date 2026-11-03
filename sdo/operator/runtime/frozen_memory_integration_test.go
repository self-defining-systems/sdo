//go:build integration

package runtime

import (
	"context"
	"path/filepath"
	"testing"

	"sdo.dev/operator/broker"
)

func TestFrozenMemoryClosureKeepsEvidencePrivateAcrossRestart(t *testing.T) {
	for _, classification := range []string{"success", "failed"} {
		t.Run(classification, func(t *testing.T) {
			ctx := context.Background()
			root := nativeRepository(t)
			repository, err := broker.New(root, filepath.Join(t.TempDir(), "proposals"), nil)
			if err != nil {
				t.Fatal(err)
			}
			native := NativeIncidentBroker{Repository: repository, LedgerRoot: t.TempDir(), ResponderBackend: "scripted", ResponderModel: "none"}
			native.FrozenMemory = true
			derivations := 0
			native.Deriver = OutcomeDeriverFunc(func(context.Context, IncidentClosure, string, string, string) (DerivedOutcome, error) {
				derivations++
				return testDerivedOutcome("frozen", classification, true), nil
			})
			workspace, err := native.PrepareIncident(ctx, "frozen")
			if err != nil {
				t.Fatal(err)
			}
			closure := nativeClosure(workspace)
			closure.Request.RepairPolicy = "recorded-actions"
			before := nativeGit(t, root, "rev-parse", "HEAD")
			receipt, err := native.ProcessClosure(ctx, closure)
			if err != nil {
				t.Fatal(err)
			}
			if err := native.AcknowledgeClosure(ctx, receipt); err != nil {
				t.Fatal(err)
			}
			// A new broker instance reads the durable policy and evidence.
			restarted := native
			again, err := restarted.ProcessClosure(ctx, closure)
			if err != nil || receipt != again {
				t.Fatalf("restart: %v %v", again, err)
			}
			if derivations != 1 {
				t.Fatalf("derived %d times", derivations)
			}
			if after := nativeGit(t, root, "rev-parse", "HEAD"); after != before {
				t.Fatalf("published repeat evidence: %s -> %s", before, after)
			}
			ledger, err := restarted.readLedger("frozen")
			if err != nil {
				t.Fatal(err)
			}
			if ledger.OutcomeDocument.GetClassification() != classification || ledger.RequiresReflection || ledger.ReflectionQueued || ledger.ReflectionCommit != "" {
				t.Fatalf("evidence missing or learning enabled: %v", ledger)
			}
			if ok, err := restarted.committedOutcomeClosure(ctx, closure, &receipt); err != nil || !ok {
				t.Fatalf("private recovery: %v %v", ok, err)
			}
			restarted.FrozenMemory = false
			if _, err := restarted.ProcessClosure(ctx, closure); err == nil {
				t.Fatal("restart changed frozen policy")
			}
		})
	}
}

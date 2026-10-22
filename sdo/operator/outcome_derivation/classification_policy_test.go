package outcome_derivation

import (
	"context"
	"testing"

	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestDerivedClassificationsHaveSharedReceiptAndLearningRules(t *testing.T) {
	tests := []struct {
		name, classification string
		change               func(*contractsv1alpha1.IncidentClosure)
		repairCommit         string
	}{
		{"success", "success", func(*contractsv1alpha1.IncidentClosure) {}, "repair"},
		{"verification_timeout", "failed", func(c *contractsv1alpha1.IncidentClosure) { c.FinalDetectorStates = nil }, "repair"},
		{"cleared_without_action", "cleared_without_action", func(c *contractsv1alpha1.IncidentClosure) {
			c.Result.RepairActions = nil
		}, ""},
		{"failed", "failed", func(c *contractsv1alpha1.IncidentClosure) { c.Result.Status = "failed" }, "repair"},
		{"cancelled_with_repair", "success", func(c *contractsv1alpha1.IncidentClosure) { c.Result.Status = "cancelled" }, "repair"},
		{"cancelled_without_action", "cleared_without_action", func(c *contractsv1alpha1.IncidentClosure) {
			c.Result.Status = "cancelled"
			c.Result.RepairActions = nil
		}, ""},
		{"unattributed_repair_still_succeeds", "success", func(c *contractsv1alpha1.IncidentClosure) {
			c.Result.RepairActions[0].Resources[0].Name = "other"
		}, "repair"},
		{"no_root_cause_still_succeeds", "success", func(c *contractsv1alpha1.IncidentClosure) {
			c.Result.ConfirmedRootCauses = nil
		}, "repair"},
		{"action_without_commit_succeeds", "success", func(*contractsv1alpha1.IncidentClosure) {}, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			closure := fixtureClosure()
			tc.change(closure)
			result, err := Derive(context.Background(), closure, tc.repairCommit, "codex", "model")
			if err != nil {
				t.Fatal(err)
			}
			classification := result.Record.GetClassification()
			if classification != tc.classification || result.RequiresReflection != (classification == "success") {
				t.Fatalf("classification and learning disagreed: %+v", result)
			}
		})
	}
}

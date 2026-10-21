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
		{"partial", "partial", func(c *contractsv1alpha1.IncidentClosure) { c.FinalDetectorStates[0].Status = "firing" }, "repair"},
		{"external_recovery", "external_recovery", func(c *contractsv1alpha1.IncidentClosure) {
			c.Result.RepairActions[0].Resources[0].Name = "other"
		}, "repair"},
		{"cleared_without_action", "cleared_without_action", func(c *contractsv1alpha1.IncidentClosure) {
			c.Result.RepairActions = nil
		}, ""},
		{"false_positive", "false_positive", func(c *contractsv1alpha1.IncidentClosure) {
			c.Result.ConfirmedRootCauses = nil
		}, "repair"},
		{"failed", "failed", func(c *contractsv1alpha1.IncidentClosure) { c.Result.Status = "failed" }, "repair"},
		{"cancelled", "cancelled", func(c *contractsv1alpha1.IncidentClosure) { c.Result.Status = "cancelled" }, "repair"},
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
			rule, ok := classificationPolicy[classification]
			if classification != tc.classification || !ok || rule.Learnable != result.RequiresReflection {
				t.Fatalf("classification %q has no matching policy: %+v", classification, result)
			}
			if rule.ReceiptAccepted != (classification == "success" || classification == "external_recovery" || classification == "cleared_without_action" || classification == "false_positive") {
				t.Fatalf("receipt policy disagrees for %q: %+v", classification, rule)
			}
		})
	}
}

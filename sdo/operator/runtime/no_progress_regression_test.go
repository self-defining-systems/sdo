package runtime

import (
	"encoding/json"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
)

func noProgressInventory(t *testing.T) sdk.Finding {
	t.Helper()
	object := inventoryObject("v1", "ServiceAccount", "app", "cleanup")
	return NewSourceInventory("app", "accepted", nil).Findings([]*unstructured.Unstructured{object}, time.Unix(100, 0))[0]
}

func noProgressFailure(finding sdk.Finding) continuationRepair {
	return continuationRepair{Kind: "recorded-action", ActionID: "failed", Receipt: continuationReceipt{ActionID: "failed", Resources: []sdk.ObjectRef{finding.PrimaryResource}, FailureReason: "execution_failed", StartedAt: time.Unix(99, 0).UTC(), CompletedAt: time.Unix(100, 0).UTC()}}
}

func TestResponderNoProgressContinuesOnChangedEvidence(t *testing.T) {
	for _, mode := range []string{"health", "inventory"} {
		t.Run(mode, func(t *testing.T) {
			finding := noProgressInventory(t)
			if mode == "health" {
				finding.DetectorID = "health"
				finding.Metadata = nil
				finding.Evidence = "ready=0/3"
			}
			repair := noProgressFailure(finding)
			previous, err := continuationFeedback(1, ToolResponse{Findings: []sdk.Finding{finding}}, repair)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "health" {
				finding.Evidence = "ready=2/3"
			} else {
				finding.Evidence += "; independent evidence changed"
			}
			if responderNoProgress(string(previous), ToolResponse{Findings: []sdk.Finding{finding}}, []continuationRepair{repair}) {
				t.Fatal("changing evidence under a stable fingerprint stopped response")
			}
		})
	}
}

func TestResponderNoProgressContinuesPendingOrUncertainWrite(t *testing.T) {
	for _, reason := range []string{"outcome_unknown", "delete_not_confirmed", "pending", "", "success"} {
		t.Run(reason, func(t *testing.T) {
			finding := noProgressInventory(t)
			repair := noProgressFailure(finding)
			repair.Receipt.FailureReason = reason
			if reason == "pending" {
				repair.Receipt.CompletedAt = time.Time{}
			}
			if reason == "success" {
				repair.Receipt.Success = true
				repair.Receipt.FailureReason = ""
			}
			verification := ToolResponse{Findings: []sdk.Finding{finding}}
			previous, err := continuationFeedback(1, verification, repair)
			if err != nil {
				t.Fatal(err)
			}
			if responderNoProgress(string(previous), verification, []continuationRepair{repair}) {
				t.Fatal("pending, uncertain or successful mutation stopped response")
			}
		})
	}
}

func TestResponderNoProgressRequiresStableInventoryAndFailedReceipts(t *testing.T) {
	for _, mode := range []string{"unchanged", "fresh-sample", "health", "deleting", "owned", "ttl-job", "changed-receipt", "new-attempt", "no-attempt"} {
		t.Run(mode, func(t *testing.T) {
			object := inventoryObject("v1", "ServiceAccount", "app", "cleanup")
			if mode == "deleting" {
				stamp := metav1.NewTime(time.Unix(90, 0))
				object.SetDeletionTimestamp(&stamp)
			}
			if mode == "owned" {
				object.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: "unknown", UID: "owner"}})
			}
			if mode == "ttl-job" {
				object.SetAPIVersion("batch/v1")
				object.SetKind("Job")
				object.Object["spec"] = map[string]any{"ttlSecondsAfterFinished": int64(30)}
			}
			inventory := NewSourceInventory("app", "accepted", nil)
			finding := inventory.Findings([]*unstructured.Unstructured{object}, time.Unix(100, 0))[0]
			if mode == "health" {
				finding.DetectorID = "health"
				finding.Metadata = nil
			}
			repairs := []continuationRepair{noProgressFailure(finding)}
			if mode == "no-attempt" {
				repairs = nil
			}
			previous, err := continuationFeedback(1, ToolResponse{Findings: []sdk.Finding{finding}}, repairs...)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "fresh-sample" {
				finding = inventory.Findings([]*unstructured.Unstructured{object}, time.Unix(101, 0))[0]
			}
			if mode == "changed-receipt" {
				repairs[0].Receipt.Details = "new failure evidence"
			}
			if mode == "new-attempt" {
				next := repairs[0]
				next.ActionID = "second"
				repairs = append(repairs, next)
			}
			got := responderNoProgress(string(previous), ToolResponse{Findings: []sdk.Finding{finding}}, repairs)
			if got != (mode == "unchanged" || mode == "fresh-sample") {
				encoded, _ := json.Marshal(finding)
				t.Fatalf("mode=%s stopped=%v finding=%s", mode, got, encoded)
			}
		})
	}
}

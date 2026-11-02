package runtime

import (
	"sdo.dev/operator/sdk"
	"testing"
)

func TestResponderNoProgressRequiresUnchangedFindingsAndSuccesses(t *testing.T) {
	finding := noProgressInventory(t)
	verification := ToolResponse{Findings: []sdk.Finding{finding}}
	failed := noProgressFailure(finding)
	previous, err := continuationFeedback(1, verification, failed)
	if err != nil {
		t.Fatal(err)
	}
	if !responderNoProgress(string(previous), verification, []continuationRepair{failed}) {
		t.Fatal("unchanged known failures prevented terminal no progress")
	}
	newSuccess := failed
	newSuccess.Receipt.Success = true
	newSuccess.ActionID = "second"
	if responderNoProgress(string(previous), verification, []continuationRepair{failed, newSuccess}) {
		t.Fatal("new successful mutation was ignored")
	}
	changed := ToolResponse{Findings: []sdk.Finding{{Fingerprint: "changed"}}}
	if responderNoProgress(string(previous), changed, nil) {
		t.Fatal("changed still-broken set considered unchanged")
	}
	if responderNoProgress("", verification, nil) || responderNoProgress(string(previous), ToolResponse{}, nil) {
		t.Fatal("first response or pending health observation ended early")
	}
	verification.Error = "inventory read unavailable"
	if responderNoProgress(string(previous), verification, nil) {
		t.Fatal("unavailable verification ended incident")
	}
}


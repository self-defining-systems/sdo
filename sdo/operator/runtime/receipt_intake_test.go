package runtime

import "testing"

func TestWorkerResultKeepsOnlyReceiptsGoIssued(t *testing.T) {
	got := goIssuedReceipts([]RepairActionReceipt{
		{ActionID: "sdo-api-abc", Kind: "api-patch"},
		{ActionID: "sdo-program-app-1", Kind: "program-plan"},
		{ActionID: "sdo-act-1", Kind: "kubectl-patch"},
		{ActionID: "source-1", Kind: "kubectl-patch"},
		{ActionID: "sdo-api-", Kind: "api-patch"},
		{ActionID: "", Kind: "api-patch"},
	})
	if len(got) != 2 || got[0].ActionID != "sdo-api-abc" || got[1].ActionID != "sdo-program-app-1" {
		t.Fatalf("kept %+v, want only the Go-issued receipts", got)
	}
}

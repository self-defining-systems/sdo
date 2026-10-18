package broker

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNativeLedgerEdgeValidatesIncidentAndPath(t *testing.T) {
	path, err := NativeLedgerPath("/workspace/worktrees", "incident-1")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(path, "/workspace/worktrees/native-ledger/") || !strings.HasSuffix(path, ".json") {
		t.Fatalf("wrong ledger edge path: %q", path)
	}
	valid := []byte(`{"incident_id":"incident-1","acknowledged":true}`)
	if _, err := ValidateNativeLedger(valid, "incident-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateNativeLedger(valid, "incident-2"); err == nil {
		t.Fatal("cross-incident ledger accepted")
	}
	if _, err := ValidateNativeLedger([]byte(`{"incident_id":"incident-1","closure":{"request":{"incident_id":"other"}}}`), "incident-1"); err == nil {
		t.Fatal("cross-incident closure accepted")
	}
	var decoded map[string]any
	if err := json.Unmarshal(valid, &decoded); err != nil {
		t.Fatal(err)
	}
}

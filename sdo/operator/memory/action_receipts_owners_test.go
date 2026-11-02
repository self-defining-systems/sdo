package memory

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// The action journal lives in agent-writable scratch, so an owner chain
// appended there never reaches SDO (REV12 F1).
func TestActionJournalDropsAgentOwnerChains(t *testing.T) {
	root := t.TempDir()
	relative, err := PrepareIncidentSession(root, "", "incident-1")
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 10, 26, 15, 18, 0, 0, time.UTC)
	pod := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Pod", Namespace: "app", Name: "unrelated-7c9d"}
	receipt, err := protojson.Marshal(&contracts.RepairActionReceipt{
		ActionId: "sdo-act-1", Kind: "kubectl-delete", Target: "Pod/unrelated-7c9d", Resources: []*contracts.ObjectRef{pod}, Summary: "delete", Details: "delete", Success: true,
		StartedAt: timestamppb.New(at), CompletedAt: timestamppb.New(at.Add(time.Second)),
		Owners: []*contracts.ResourceOwners{{Resource: pod, OwnerChain: []*contracts.ObjectRef{{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "cause"}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	line, err := json.Marshal(map[string]any{"state": "finished", "receipt": json.RawMessage(receipt)})
	if err != nil {
		t.Fatal(err)
	}
	scratch := filepath.Join(root, relative, "scratch")
	if err := os.MkdirAll(scratch, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(scratch, "repair-actions.jsonl"), append(line, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	rows, _, _, err := IncidentActionReceipts(root, "incident-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || len(rows[0].GetOwners()) != 0 {
		t.Fatalf("agent-written owner chain survived the journal read: %v", rows)
	}
}

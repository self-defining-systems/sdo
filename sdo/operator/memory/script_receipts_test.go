package memory

import (
	"encoding/json"
	"google.golang.org/protobuf/types/known/timestamppb"
	"os"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"strings"
	"testing"
	"time"
)

func TestScriptOriginReceiptsAreControllerOwnedTimedAndIdempotent(t *testing.T) {
	root := t.TempDir()
	const incident = "script-origin"
	if err := CaptureIncidentSnapshot(root, incident, "open@detect", sdktest.Snapshot{NamespaceName: "app"}, "app"); err != nil {
		t.Fatal(err)
	}
	at := time.Unix(100, 0)
	outcome := &contracts.OutcomeRecord{IncidentId: incident, RepairActions: []*contracts.RepairActionReceipt{{ActionId: "manual-exec", Success: true, StartedAt: timestamppb.New(at), CompletedAt: timestamppb.New(at.Add(time.Second)), Details: "must-not-copy-command-or-credential", Resources: []*contracts.ObjectRef{{ApiVersion: "v1", Kind: "Pod", Namespace: "app", Name: "database"}}}, {ActionId: "failed-exec", Success: false, StartedAt: timestamppb.New(at), CompletedAt: timestamppb.New(at.Add(time.Second))}}}
	for i := 0; i < 2; i++ {
		if err := CaptureRepairReceipts(root, incident, outcome); err != nil {
			t.Fatal(err)
		}
	}
	dir, err := IncidentCorpusDir(root, incident)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "open@detect-recordings.json"))
	if err != nil {
		t.Fatal(err)
	}
	var calls []sdktest.PrimitiveCall
	if err = json.Unmarshal(raw, &calls); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 1 || calls[0].Method != "RepairReceipt" || strings.Contains(string(raw), "must-not-copy") || strings.Contains(string(raw), "failed-exec") {
		t.Fatalf("invalid origin capture: %s", raw)
	}
	var origin program.ScriptOriginReceipt
	if err = json.Unmarshal(calls[0].Result, &origin); err != nil {
		t.Fatal(err)
	}
	if origin.ID != "manual-exec" || origin.IncidentID != incident || !origin.Success || origin.CompletedAt.Sub(origin.StartedAt) != time.Second {
		t.Fatalf("origin identity lost: %+v", origin)
	}
	if err = CaptureRepairReceipts(root, "another-incident", outcome); err == nil {
		t.Fatal("cross-incident origin accepted")
	}
}

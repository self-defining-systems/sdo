package operatorcli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/latefindings"
)

func TestLateFindingsPullChecksIncidentAndRecordsLogicalReceipt(t *testing.T) {
	root := t.TempDir()
	reportPath := filepath.Join(root, "report.json")
	receipts := filepath.Join(root, "pulls.jsonl")
	report := latefindings.EmptyReport("incident-1")
	report.LateFindings = []latefindings.Finding{{DetectorID: "detector", Fingerprint: "rule/x", PlaybookIDs: []string{"repair"}, StillActive: true}}
	data, _ := json.Marshal(report)
	if err := os.WriteFile(reportPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := RunLateFindings(context.Background(), LateFindingsConfig{IncidentID: "incident-1", ReportPath: reportPath, ReceiptsPath: receipts}, &output); err != nil {
		t.Fatal(err)
	}
	var got latefindings.Report
	if err := json.Unmarshal(output.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.LateFindings) != 1 || got.LateFindings[0].PlaybookIDs[0] != "repair" {
		t.Fatalf("bad logical report: %+v", got)
	}
	pull, err := os.ReadFile(receipts)
	if err != nil {
		t.Fatal(err)
	}
	var receipt map[string]any
	if err := json.Unmarshal(pull, &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt["incident_id"] != "incident-1" || len(receipt["report_digest"].(string)) != 64 || receipt["playbook_ids"].([]any)[0] != "repair" {
		t.Fatalf("bad pull receipt: %v", receipt)
	}
	if err := RunLateFindings(context.Background(), LateFindingsConfig{IncidentID: "incident-2", ReportPath: reportPath, ReceiptsPath: receipts}, &output); err == nil {
		t.Fatal("cross-incident report accepted")
	}
}

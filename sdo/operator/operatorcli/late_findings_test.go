package operatorcli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/latefindings"
)

func TestLateFindingsPullChecksIncidentAndRecordsLogicalReceipt(t *testing.T) {
	root := t.TempDir()
	reportPath := filepath.Join(root, "report.json")
	receipts := filepath.Join(root, "pulls.jsonl")
	report := latefindings.EmptyReport("incident-1")
	report.LateFindings = []*contracts.LateFinding{{
		DetectorId: "detector", Fingerprint: "rule/x", PlaybookIds: []string{"repair"}, StillActive: true,
		ActivatedAt: timestamppb.New(time.Unix(1700000000, 0)),
	}}
	data, err := wire.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(reportPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := RunLateFindings(context.Background(), LateFindingsConfig{IncidentID: "incident-1", ReportPath: reportPath, ReceiptsPath: receipts}, &output); err != nil {
		t.Fatal(err)
	}
	got := &contracts.LateFindingsReport{}
	if err := wire.Unmarshal(output.Bytes(), got); err != nil {
		t.Fatal(err)
	}
	if len(got.GetLateFindings()) != 1 || got.GetLateFindings()[0].GetPlaybookIds()[0] != "repair" {
		t.Fatalf("bad logical report: %v", got)
	}
	pull, err := os.ReadFile(receipts)
	if err != nil {
		t.Fatal(err)
	}
	receipt := &contracts.LateFindingsReceipt{}
	if err := wire.Unmarshal(bytes.TrimSpace(pull), receipt); err != nil {
		t.Fatal(err)
	}
	wantDigest, err := latefindings.Digest(report)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.GetIncidentId() != "incident-1" || receipt.GetReportDigest() != wantDigest || receipt.GetPlaybookIds()[0] != "repair" {
		t.Fatalf("bad pull receipt: %v", receipt)
	}
	if err := RunLateFindings(context.Background(), LateFindingsConfig{IncidentID: "incident-2", ReportPath: reportPath, ReceiptsPath: receipts}, &output); err == nil {
		t.Fatal("cross-incident report accepted")
	}
}

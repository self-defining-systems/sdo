package validate

import (
	"bytes"
	"google.golang.org/protobuf/encoding/protojson"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/sdktest"
	"strings"
	"testing"
)

func TestMitigationReplayNotesAreInformational(t *testing.T) {
	var output bytes.Buffer
	note := sdktest.MitigationReplayResult{ProgramID: "restore", CaseID: "incident", Status: sdktest.MitigationReplaySkippedUnreplayableEffect, Effects: []string{"program.ReconcileFromSource"}}
	if err := WriteReport(&output, nil, note); err != nil {
		t.Fatal(err)
	}
	var report contracts.MemoryCheckReport
	if err := protojson.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(output.String(), ReportPrefix))), &report); err != nil {
		t.Fatal(err)
	}
	if len(report.MitigationReplayNotes) != 1 || report.MitigationReplayNotes[0].Status != contracts.MitigationReplayNote_STATUS_SKIPPED_UNREPLAYABLE_EFFECT {
		t.Fatalf("lost typed note: %+v", &report)
	}
	diagnostics, err := ParseReport(output.String())
	if err != nil || Err(diagnostics) != nil {
		t.Fatalf("note became rejection: %+v %v", diagnostics, err)
	}
}

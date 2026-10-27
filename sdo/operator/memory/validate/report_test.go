package validate

import (
	"bytes"
	"reflect"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"sdo.dev/operator/builder"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/sdktest"
	"strings"
	"testing"
)

func TestMitigationReplayNotesAreInformational(t *testing.T) {
	var output bytes.Buffer
	note := sdktest.MitigationReplayResult{ProgramID: "restore", CaseID: "incident", Status: sdktest.MitigationReplaySkippedUnreplayableEffect, Effects: []string{"program.ReconcileFromSource"}}
	if err := WriteReport(&output, nil, nil, note); err != nil {
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

func TestStageTimingsRoundTripThroughRunnerOutput(t *testing.T) {
	var out bytes.Buffer
	want := []builder.StageTiming{{Stage: "build ./...", Duration: 4200 * time.Millisecond}, {Stage: "test ./...", Duration: 31 * time.Second}}
	if err := WriteStageTimings(&out, want); err != nil {
		t.Fatal(err)
	}
	if got := ParseStageTimings("noise\n" + out.String()); !reflect.DeepEqual(got, want) {
		t.Fatalf("stage timings = %+v, want %+v", got, want)
	}
}

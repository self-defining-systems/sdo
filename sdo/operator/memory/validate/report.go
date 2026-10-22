package validate

import (
	"fmt"
	"io"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/sdktest"
)

// ReportPrefix frames the trusted runner report separately from tool output.
const ReportPrefix = "SDO_MEMORY_REPORT:"
const MaxReportBytes = 1024 * 1024

func WriteReport(out io.Writer, diagnostics []Diagnostic, notes ...sdktest.MitigationReplayResult) error {
	data, err := protojson.Marshal(&contracts.MemoryCheckReport{Diagnostics: ProtoDiagnostics(diagnostics), MitigationReplayNotes: ProtoMitigationReplayNotes(notes)})
	if err != nil {
		return err
	}
	if len(data) > MaxReportBytes {
		return fmt.Errorf("validator report exceeds %d bytes", MaxReportBytes)
	}
	_, err = fmt.Fprintf(out, "%s%s\n", ReportPrefix, data)
	return err
}

// ParseReport accepts only a complete framed report from the runner. Missing
// or truncated reports remain infrastructure errors and never imply acceptance.
func ParseReport(logs string) ([]Diagnostic, error) {
	for _, line := range strings.Split(logs, "\n") {
		if !strings.HasPrefix(line, ReportPrefix) {
			continue
		}
		report := &contracts.MemoryCheckReport{}
		if err := protojson.Unmarshal([]byte(strings.TrimPrefix(line, ReportPrefix)), report); err != nil {
			return nil, err
		}
		var ds []Diagnostic
		for _, d := range report.GetDiagnostics() {
			if d.Rule == "" || d.Message == "" {
				return nil, fmt.Errorf("incomplete validator diagnostic")
			}
			ds = append(ds, Diagnostic{Rule: d.Rule, Path: d.Path, Line: int(d.Line), Message: d.Message, Hint: d.Hint})
		}
		return ds, nil
	}
	return nil, fmt.Errorf("validator omitted structured memory report")
}

// ProtoMitigationReplayNotes keeps successful policy notes separate from failures.
func ProtoMitigationReplayNotes(notes []sdktest.MitigationReplayResult) []*contracts.MitigationReplayNote {
	result := make([]*contracts.MitigationReplayNote, 0, len(notes))
	for _, note := range notes {
		status := contracts.MitigationReplayNote_STATUS_REPLAYED
		if note.Status == sdktest.MitigationReplaySkippedUnreplayableEffect {
			status = contracts.MitigationReplayNote_STATUS_SKIPPED_UNREPLAYABLE_EFFECT
		}
		result = append(result, &contracts.MitigationReplayNote{ProgramId: note.ProgramID, CaseId: note.CaseID, Status: status, Effects: append([]string(nil), note.Effects...)})
	}
	return result
}

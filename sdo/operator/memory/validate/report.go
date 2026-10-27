package validate

import (
	"fmt"
	"io"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"sdo.dev/operator/builder"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/sdktest"
)

// ReportPrefix frames the trusted runner report separately from tool output.
const ReportPrefix = "SDO_MEMORY_REPORT:"
const MaxReportBytes = 1024 * 1024

// StageReportPrefix frames one Go toolchain stage duration in the runner's output.
const StageReportPrefix = "SDO_VALIDATOR_STAGE:"

// WriteStageTimings reports how long each compile-layer stage took so the
// broker can log where validator wall time goes.
func WriteStageTimings(out io.Writer, timings []builder.StageTiming) error {
	for _, timing := range timings {
		if _, err := fmt.Fprintf(out, "%s%s\t%d\n", StageReportPrefix, timing.Stage, timing.Duration.Milliseconds()); err != nil {
			return err
		}
	}
	return nil
}

// ParseStageTimings reads the stage lines of a runner's output. Malformed
// lines are skipped: timings are diagnostics and never affect acceptance.
func ParseStageTimings(logs string) []builder.StageTiming {
	var timings []builder.StageTiming
	for _, line := range strings.Split(logs, "\n") {
		stage, millis, ok := strings.Cut(strings.TrimPrefix(line, StageReportPrefix), "\t")
		if !strings.HasPrefix(line, StageReportPrefix) || !ok {
			continue
		}
		var ms int64
		if _, err := fmt.Sscanf(millis, "%d", &ms); err != nil {
			continue
		}
		timings = append(timings, builder.StageTiming{Stage: stage, Duration: time.Duration(ms) * time.Millisecond})
	}
	return timings
}

// WriteReport frames the runner's verdict. Coverage gaps are non-blocking
// notes the broker records beside the accepted commit.
func WriteReport(out io.Writer, diagnostics, gaps []Diagnostic, notes ...sdktest.MitigationReplayResult) error {
	data, err := protojson.Marshal(&contracts.MemoryCheckReport{Diagnostics: ProtoDiagnostics(diagnostics), CoverageGaps: ProtoDiagnostics(gaps), MitigationReplayNotes: ProtoMitigationReplayNotes(notes)})
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
		return reportDiagnostics(report.GetDiagnostics())
	}
	return nil, fmt.Errorf("validator omitted structured memory report")
}

// ParseReportGaps reads the coverage gaps of a runner's report. Gaps never
// affect acceptance, so a missing or malformed report yields none.
func ParseReportGaps(logs string) []Diagnostic {
	for _, line := range strings.Split(logs, "\n") {
		if !strings.HasPrefix(line, ReportPrefix) {
			continue
		}
		report := &contracts.MemoryCheckReport{}
		if protojson.Unmarshal([]byte(strings.TrimPrefix(line, ReportPrefix)), report) != nil {
			return nil
		}
		gaps, _ := reportDiagnostics(report.GetCoverageGaps())
		return gaps
	}
	return nil
}

func reportDiagnostics(items []*contracts.MemoryDiagnostic) ([]Diagnostic, error) {
	var ds []Diagnostic
	for _, d := range items {
		if d.Rule == "" || d.Message == "" {
			return nil, fmt.Errorf("incomplete validator diagnostic")
		}
		ds = append(ds, Diagnostic{Rule: d.Rule, Path: d.Path, Line: int(d.Line), Message: d.Message, Hint: d.Hint})
	}
	return ds, nil
}

// ProtoMitigationReplayNotes keeps successful policy notes separate from failures.
func ProtoMitigationReplayNotes(notes []sdktest.MitigationReplayResult) []*contracts.MitigationReplayNote {
	result := make([]*contracts.MitigationReplayNote, 0, len(notes))
	for _, note := range notes {
		status := contracts.MitigationReplayNote_STATUS_REPLAYED
		if note.Status == sdktest.MitigationReplaySkippedUnreplayableEffect {
			status = contracts.MitigationReplayNote_STATUS_SKIPPED_UNREPLAYABLE_EFFECT
		}
		if note.Status == sdktest.MitigationReplayObservedTransition {
			status = contracts.MitigationReplayNote_STATUS_OBSERVED_TRANSITION
		}
		result = append(result, &contracts.MitigationReplayNote{ProgramId: note.ProgramID, CaseId: note.CaseID, Status: status, Effects: append([]string(nil), note.Effects...)})
	}
	return result
}

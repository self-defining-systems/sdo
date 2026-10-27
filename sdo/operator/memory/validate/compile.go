package validate

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"sdo.dev/operator/builder"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
)

const maxCompileOutput = 4096

// Go test timings and cache hits differ on repeated runs; they are log detail,
// not part of a diagnostic's identity.
var goTestDuration = regexp.MustCompile(`(?m)(?:\t(?:[0-9]+\.[0-9]+s|\(cached\))| \([0-9]+\.[0-9]+s\))$`)

// goLocation matches a compiler or vet position inside a detector workspace,
// whose layout mirrors the diagnostics directory. With -trimpath, cached vet
// output can retain a relative or absolute path to an earlier isolated build.
var goLocation = regexp.MustCompile(`(?m)^(?:vet: )?(?:\./)?(?:(?:[^\r\n:]*/)?sdo-operator-build-[^/\s:]+/)?((?:detectors|traffic)/[^\s:]+\.go):(\d+)(?::\d+)?: (.+)$`)

// compile builds the detectors against the SDK in an isolated workspace, then
// vets and tests them. It executes detector code: the broker invokes it in
// its network-isolated Job; agent self-checks invoke the same runner locally.
func (s *run) compile(ctx context.Context) error {
	diagnostics, err := memory.CandidateDiagnostics(s.req.Root)
	if errors.Is(err, memory.ErrDiagnosticsNotBootstrapped) {
		return nil
	}
	if err != nil {
		return err
	}
	workspace, err := builder.BuildWorkspace(ctx, builder.Config{
		ApplicationRoot: s.req.Root, DiagnosticsRoot: diagnostics, OperatorRoot: s.req.OperatorRoot, HealthyBaseline: s.req.HealthyBaseline,
	})
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s.add(Compile, Diagnostic{Rule: RuleDetectorCompile, Path: layout.DiagnosticsDir, Message: err.Error()})
		return nil
	}
	defer workspace.Close()
	err = workspace.Check(ctx)
	if s.req.StageTimings != nil {
		*s.req.StageTimings = append(*s.req.StageTimings, workspace.StageTimings...)
	}
	if err == nil {
		notes, noteErr := workspace.MitigationReplayResults()
		if noteErr != nil {
			s.req.noteGaps(Diagnostic{Rule: RuleReplayObservation, Message: "replay unverified: " + noteErr.Error()})
		}
		if s.req.MitigationReplayNotes != nil {
			*s.req.MitigationReplayNotes = append(*s.req.MitigationReplayNotes, notes...)
		}
		gaps, gapErr := workspace.CoverageGaps()
		if gapErr != nil {
			s.req.noteGaps(Diagnostic{Rule: RuleReplayObservation, Message: "replay unverified: " + gapErr.Error()})
		}
		for _, gap := range gaps {
			s.req.noteGaps(Diagnostic{Rule: RuleReplayObservation, Path: layout.PlaybooksDir, Message: gap})
		}
		for _, note := range notes {
			s.req.noteGaps(Diagnostic{Rule: RuleReplayObservation, Path: layout.PlaybookDir(note.ProgramID), Message: fmt.Sprintf("program=%s case=%s replay=%s: %s", note.ProgramID, note.CaseID, note.Status, strings.Join(note.Effects, "; "))})
		}
		return nil
	}
	var stage *builder.StageError
	if ctx.Err() != nil || !errors.As(err, &stage) {
		return err
	}
	rule := RuleDetectorCompile
	switch {
	case strings.HasPrefix(stage.Stage, "vet"):
		rule = RuleDetectorVet
	case strings.HasPrefix(stage.Stage, "test"):
		rule = RuleDetectorTest
	}
	if strings.Contains(stage.Output, RuleDetectorHealthyBaseline) {
		s.add(Compile, Diagnostic{Rule: RuleDetectorHealthyBaseline, Path: layout.DiagnosticsDir, Message: goTestDuration.ReplaceAllString(stage.Output, ""), Hint: "incident detectors must stay quiet on the controller-captured healthy state"})
		return nil
	}
	if strings.Contains(stage.Output, RuleDetectorControllerStart) {
		message := goTestDuration.ReplaceAllString(stage.Output, "")
		if len(message) > maxCompileOutput {
			message = message[:maxCompileOutput]
		}
		s.add(Compile, Diagnostic{Rule: RuleDetectorControllerStart, Path: layout.DiagnosticsDir, Message: message, Hint: "accepted detectors must load in the controller"})
		return nil
	}
	located := goLocation.FindAllStringSubmatch(stage.Output, -1)
	reported := map[string]bool{}
	for _, m := range located {
		if key := m[1] + ":" + m[2] + ":" + m[3]; reported[key] {
			continue
		} else {
			reported[key] = true
		}
		line, _ := strconv.Atoi(m[2])
		path := layout.DiagnosticsDir + "/" + m[1]
		if strings.HasPrefix(m[1], "generated/") {
			path = m[1]
		}
		s.add(Compile, Diagnostic{Rule: rule, Path: path, Line: line, Message: strings.TrimSpace(m[3]), Hint: "go " + stage.Stage + " failed"})
	}
	if len(located) == 0 {
		output := goTestDuration.ReplaceAllString(stage.Output, "")
		if len(output) > maxCompileOutput {
			output = "..." + output[len(output)-maxCompileOutput:]
		}
		s.add(Compile, Diagnostic{Rule: rule, Path: layout.DiagnosticsDir, Message: "go " + stage.Stage + " failed: " + output})
	}
	return nil
}

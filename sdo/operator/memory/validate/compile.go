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

// goLocation matches a compiler or vet position inside the complete memory
// workspace. With -trimpath, cached vet
// output can retain a relative or absolute path to an earlier isolated build.
var goLocation = regexp.MustCompile(`(?m)^(?:vet: )?(?:\./)?(?:(?:[^\r\n:]*/)?sdo-operator-build-[^/\s:]+/)?((?:detectors|traffic|playbooks|lib|generated)/[^\s:]+\.go):(\d+)(?::\d+)?: (.+)$`)

// telemetryBindingViolation is one generated query-to-source binding failure.
var telemetryBindingViolation = regexp.MustCompile(`(?m)(telemetry\.(?:unbound|kind))(?:\[[a-z0-9][a-z0-9_.-]*\])?: .+?$`)

// healthyBaselineViolation is one generated healthy-baseline test failure:
// the incident detector ID and the violation.
var healthyBaselineViolation = regexp.MustCompile(`(?m)detector\.healthy-baseline\[([a-z0-9][a-z0-9_.-]*)\]: (.+?)\s*$`)

// compile builds the detectors against the SDK in an isolated workspace, then
// vets and tests them. It executes detector code: the broker invokes it in
// its bounded subprocess; agent self-checks invoke the same runner locally.
func (s *run) compile(ctx context.Context) error {
	diagnostics, err := memory.CandidateDiagnostics(s.req.Root)
	if errors.Is(err, memory.ErrDiagnosticsNotBootstrapped) {
		return nil
	}
	if err != nil {
		return err
	}
	sources, err := EffectiveTelemetrySources(s.req.Root, s.req.Telemetry, s.req.TelemetryRegistry)
	if err != nil {
		s.add(Compile, Diagnostic{Rule: RuleTelemetrySchema, Path: layout.TelemetryDir, Message: err.Error(), Hint: "fix the telemetry bindings so detector queries can be checked against them"})
		return nil
	}
	workspace, err := builder.BuildWorkspace(ctx, builder.Config{
		Replay: s.req.Replay, ApplicationRoot: s.req.Root, DiagnosticsRoot: diagnostics, OperatorRoot: s.req.OperatorRoot, HealthyBaseline: s.req.HealthyBaseline,
		Telemetry: &builder.TelemetryBindings{Sources: sources},
	})
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s.add(Compile, Diagnostic{Rule: RuleDetectorCompile, Path: layout.DiagnosticsDir, Message: err.Error()})
		return nil
	}
	defer workspace.Close()
	if s.req.AcceptanceOnly {
		err = workspace.CheckAcceptance(ctx)
	} else {
		err = workspace.Check(ctx)
	}
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
		edges, edgeErr := workspace.TrafficCoverageGaps()
		if edgeErr != nil {
			s.req.noteGaps(Diagnostic{Rule: RuleTrafficUndrivenEdge, Path: layout.TrafficWorkloadsDir, Message: "traffic coverage unverified: " + edgeErr.Error()})
		}
		for _, edge := range edges {
			s.req.noteGaps(Diagnostic{Rule: RuleTrafficUndrivenEdge, Path: layout.TrafficWorkloadsDir, Message: edge})
		}
		bounds, boundErr := workspace.TrafficLatencyGaps()
		if boundErr != nil {
			s.req.noteGaps(Diagnostic{Rule: RuleTrafficDefaultLatency, Path: layout.TrafficWorkloadsDir, Message: "traffic latency bounds unverified: " + boundErr.Error()})
		}
		for _, bound := range bounds {
			s.req.noteGaps(Diagnostic{Rule: RuleTrafficDefaultLatency, Path: layout.TrafficWorkloadsDir, Message: bound})
		}
		mapFindings, mapErr := workspace.ApplicationMapFindings()
		if mapErr != nil {
			s.req.noteGaps(Diagnostic{Rule: RuleMapSchema, Path: layout.ArchitectureFile, Message: "application map scenario check unverified: " + mapErr.Error()})
		}
		for _, finding := range mapFindings {
			s.mapFinding(Compile, finding, 0)
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
	if builder.IsHealthyBaselineStage(stage) {
		const hint = "incident detectors must stay quiet on the controller-captured healthy state"
		// The replay runs alone, after the registration and controller-start
		// contracts passed, so each violation's detector ID names exactly one
		// detector directory and Check can compare it with the base.
		matches := healthyBaselineViolation.FindAllStringSubmatch(stage.Output, -1)
		seen := map[string]bool{}
		for _, match := range matches {
			if key := match[1] + "\x00" + match[2]; !seen[key] {
				seen[key] = true
				s.add(Compile, Diagnostic{Rule: RuleDetectorHealthyBaseline, Path: layout.DetectorDir(layout.ClassIncident, match[1]), Message: match[2], Hint: hint})
			}
		}
		if len(matches) == 0 {
			s.add(Compile, Diagnostic{Rule: RuleDetectorHealthyBaseline, Path: layout.DiagnosticsDir, Message: goTestDuration.ReplaceAllString(stage.Output, ""), Hint: hint})
		}
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
	if bindings := telemetryBindingViolation.FindAllStringSubmatch(stage.Output, -1); len(bindings) > 0 {
		const hint = "every detector query names a bound source (or none, when a default source of its kind exists); bind the source in .sdo/diagnostics/telemetry/sources.yaml or name one that is bound"
		seen := map[string]bool{}
		for _, match := range bindings {
			if !seen[match[0]] {
				seen[match[0]] = true
				s.add(Compile, Diagnostic{Rule: match[1], Path: layout.DiagnosticsDir, Message: strings.TrimSpace(match[0]), Hint: hint})
			}
		}
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
		switch {
		case strings.HasPrefix(m[1], "playbooks/"), strings.HasPrefix(m[1], "lib/"):
			path = layout.Dir + "/" + m[1]
		case strings.HasPrefix(m[1], "generated/"):
			path = m[1]
		}
		s.add(Compile, Diagnostic{Rule: rule, Path: path, Line: line, Message: strings.TrimSpace(m[3]), Hint: "go " + stage.Stage + " failed"})
	}
	if len(located) == 0 && rule == RuleDetectorTest {
		if message, path := summarizeGoTest(goTestDuration.ReplaceAllString(stage.Output, "")); message != "" {
			s.add(Compile, Diagnostic{Rule: rule, Path: path, Message: message, Hint: "go " + stage.Stage + " failed; fix the failing test or the code it exercises, the useful lines are the failing tests, the panic and the first frame in your source"})
			return nil
		}
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

// goTestSourceFrame is a stack frame inside the isolated workspace's authored
// source (not the Go runtime, testing package or SDK).
var goTestSourceFrame = regexp.MustCompile(`sdo-operator-build-[^/\s]+/((?:detectors|traffic|playbooks|lib|generated)/[^\s:]+\.go):(\d+)`)

// goTestLogLine is a failing test's t.Error/t.Fatal output, `file_test.go:16: msg`.
var goTestLogLine = regexp.MustCompile(`^\s+[^\s:/]+\.go:\d+: `)

// summarizeGoTest reduces raw `go test` output to what a repair needs: failing
// test names, their log lines, the panic message and the first stack frame in
// authored source (returned as the diagnostic path). Passing packages, the
// goroutine dump and runtime frames are dropped. It returns "" when the output
// has none of these, so the caller keeps the raw tail.
func summarizeGoTest(output string) (message string, path string) {
	var kept []string
	frame := ""
	for _, line := range strings.Split(output, "\n") {
		switch {
		case strings.HasPrefix(line, "--- FAIL"), strings.HasPrefix(line, "panic: "), strings.HasPrefix(line, "FAIL\t"):
			kept = append(kept, strings.TrimRight(line, " \t\r"))
		case goTestLogLine.MatchString(line):
			kept = append(kept, strings.TrimRight(line, " \t\r"))
		case frame == "" && goTestSourceFrame.MatchString(line) && strings.HasPrefix(line, "\t"):
			m := goTestSourceFrame.FindStringSubmatch(line)
			frame = m[1] + ":" + m[2]
			kept = append(kept, "first frame in your source: "+frame)
		}
	}
	if len(kept) == 0 {
		return "", layout.DiagnosticsDir
	}
	path = layout.DiagnosticsDir
	if frame != "" {
		path = layout.DiagnosticsDir + "/" + frame[:strings.LastIndex(frame, ":")]
	}
	message = strings.Join(kept, "\n")
	if len(message) > maxCompileOutput {
		message = message[:maxCompileOutput]
	}
	return message, path
}

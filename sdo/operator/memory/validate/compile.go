package validate

import (
	"context"
	"errors"
	"regexp"
	"strconv"
	"strings"

	"sdo.dev/operator/builder"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
)

const maxCompileOutput = 4096

// goLocation matches a compiler or vet position inside a detector workspace,
// whose layout mirrors the diagnostics directory.
var goLocation = regexp.MustCompile(`(?m)^(?:vet: )?(?:\./)?((?:detectors|traffic)/[^\s:]+\.go):(\d+)(?::\d+)?: (.+)$`)

// compile builds the detectors against the SDK in an isolated workspace, then
// vets and tests them. It executes detector code: call it only inside the
// network-isolated validation Job, never in the broker process.
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
	if err == nil {
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
		output := stage.Output
		if len(output) > maxCompileOutput {
			output = "..." + output[len(output)-maxCompileOutput:]
		}
		s.add(Compile, Diagnostic{Rule: rule, Path: layout.DiagnosticsDir, Message: "go " + stage.Stage + " failed: " + output})
	}
	return nil
}

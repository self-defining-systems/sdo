package broker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sdo.dev/operator/broker/internal/processio"
	"sdo.dev/operator/sdk/sdktest"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/memory/validate"
)

type ValidationInfrastructureError = processio.ValidationInfrastructureError

// SubprocessValidator uses the running controller executable and SDK. The
// broker already owns a fresh staged clone; compiler scratch is private too.
type SubprocessValidator struct {
	Binary, OperatorRoot, HealthyBaseline string
	Timeout                               time.Duration
	MemoryBytes                           int64
}

func (v SubprocessValidator) Validator() Validator {
	return func(ctx context.Context, candidate string, paths []string) error {
		if len(paths) > 0 && !layout.RequiresValidatorJob(paths) {
			slog.Info("validation_subprocess_skipped", "reason", "no executable or corpus change", "changed_paths", len(paths))
			return nil
		}
		binary := v.Binary
		if binary == "" {
			var err error
			binary, err = os.Executable()
			if err != nil {
				return err
			}
		}
		root := v.OperatorRoot
		if root == "" {
			root = processio.OperatorRoot()
			if root == "" {
				root = "/opt/sdo/sdo/operator"
			}
		}
		root, err := filepath.Abs(root)
		if err != nil {
			return err
		}
		args := []string{"validation-child", candidate, root}
		needed, err := validate.BaselineGateRequired(candidate, paths)
		if err != nil {
			return err
		}
		baseline := ""
		if needed {
			baseline = v.HealthyBaseline
			if override := compileBaseline(ctx); override != "" {
				baseline = override
			}
		}
		replay, _ := ctx.Value(replayKey{}).(string)
		args = append(args, baseline, replay)
		scratch, err := os.MkdirTemp("", "sdo-validation-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(scratch)
		timeout := v.Timeout
		if timeout <= 0 {
			timeout = 10 * time.Minute
		}
		memory := v.MemoryBytes
		if memory <= 0 {
			memory = 3 << 30
		}
		output, err := processio.RunValidationProcess(ctx, binary, args, scratch, timeout, memory)
		if err != nil {
			return err
		}
		if collected, ok := ctx.Value(coverageGapsKey{}).(*[]validate.Diagnostic); ok {
			*collected = append(*collected, output.Gaps...)
		}
		if collected, ok := ctx.Value(replayNotesKey{}).(*[]*contracts.MitigationReplayNote); ok {
			*collected = append(*collected, output.ReplayNotes...)
		}
		return nil
	}
}

// RunValidationChild is called before normal launch by both the operator CLI
// and synthesized controllers, so the child always runs the parent's binary.
func RunValidationChild() {
	if len(os.Args) < 2 || os.Args[1] != "validation-child" {
		return
	}
	if len(os.Args) != 6 {
		fmt.Fprintln(os.Stderr, "invalid validation child arguments")
		os.Exit(2)
	}
	var notes []sdktest.MitigationReplayResult
	var gaps []validate.Diagnostic
	ds, err := validate.CompileCandidate(context.Background(), validate.Request{MitigationReplayNotes: &notes, CoverageGaps: &gaps, Replay: os.Args[5], Root: os.Args[2], OperatorRoot: os.Args[3], HealthyBaseline: os.Args[4], Layers: validate.Compile})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err = validate.WriteReport(os.Stdout, ds, gaps, notes...); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if len(ds) > 0 {
		os.Exit(1)
	}
	os.Exit(0)
}

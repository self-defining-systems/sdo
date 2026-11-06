package broker

import (
	"context"
	"encoding/json"
	"errors"
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
	"sdo.dev/operator/telemetry"
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
		policy, err := childTelemetryArgument(ctx)
		if err != nil {
			return err
		}
		args := childArguments(candidate, root, baseline, replay, policy)
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

// childArguments is the validation child's argv after the program name.
func childArguments(candidate, operatorRoot, baseline, replay, policy string) []string {
	return []string{"validation-child", candidate, operatorRoot, baseline, replay, policy}
}

// ChildRequest is the compile request the validation subprocess would run for
// the check in ctx, built through the same argument encoding and decoding the
// subprocess uses. A caller that stubs the compile step (a test of the
// onboarding broker's telemetry) resolves telemetry from it exactly as the
// child would, without building detectors.
func ChildRequest(ctx context.Context, candidate string, registry *telemetry.Registry) (validate.Request, error) {
	policy, err := childTelemetryArgument(ctx)
	if err != nil {
		return validate.Request{}, err
	}
	return validationChildRequest(append([]string{"sdo-operator"}, childArguments(candidate, "/operator", "", "", policy)...), registry)
}

// childTelemetryArgument is the check's telemetry policy as the validation
// child reads it. A compile request without one is a wiring error: the child
// would resolve detector queries against no operator-provided source.
func childTelemetryArgument(ctx context.Context) (string, error) {
	policy, ok := ValidationTelemetryPolicy(ctx)
	if !ok {
		return "", errors.New("validation subprocess requires the check's telemetry policy")
	}
	data, err := json.Marshal(policy)
	if err != nil {
		return "", fmt.Errorf("encode telemetry policy: %w", err)
	}
	return string(data), nil
}

// validationChildRequest is the compile request the child runs for the
// arguments SubprocessValidator passes: the candidate, operator sources,
// baseline, replay mode and the parent check's telemetry policy, checked
// against the child's adapter registry.
func validationChildRequest(args []string, registry *telemetry.Registry) (validate.Request, error) {
	if len(args) != 7 || args[1] != "validation-child" {
		return validate.Request{}, errors.New("invalid validation child arguments")
	}
	if registry == nil {
		return validate.Request{}, errors.New("validation child requires the telemetry adapter registry")
	}
	var policy telemetry.Policy
	if err := json.Unmarshal([]byte(args[6]), &policy); err != nil {
		return validate.Request{}, fmt.Errorf("validation child telemetry policy: %w", err)
	}
	if err := policy.Validate(); err != nil {
		return validate.Request{}, fmt.Errorf("validation child telemetry policy: %w", err)
	}
	return validate.Request{Replay: args[5], Root: args[2], OperatorRoot: args[3], HealthyBaseline: args[4], Layers: validate.Compile, Telemetry: policy, TelemetryRegistry: registry}, nil
}

// RunValidationChild is called before normal launch by both the operator CLI
// and synthesized controllers, so the child always runs the parent's binary.
// registry is the composition root's telemetry adapter registry, the one the
// parent broker was built with.
func RunValidationChild(registry *telemetry.Registry) {
	if len(os.Args) < 2 || os.Args[1] != "validation-child" {
		return
	}
	request, err := validationChildRequest(os.Args, registry)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	var notes []sdktest.MitigationReplayResult
	var gaps []validate.Diagnostic
	request.MitigationReplayNotes, request.CoverageGaps = &notes, &gaps
	ds, err := validate.CompileCandidate(context.Background(), request)
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

package validate

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// CompileRunner executes the compile layer locally or in the broker's isolated
// Job. Both transports run CompileCandidate; a missing runner is an error.
type CompileRunner func(context.Context, Request) ([]Diagnostic, error)

// Check is the complete memory acceptance pipeline. Callers cannot select
// partial layers. Static failures block execution of candidate Go code.
func Check(ctx context.Context, request Request, compile CompileRunner) ([]Diagnostic, error) {
	if request.Base == "" || request.Actor == "" || request.Phase == "" {
		return nil, errors.New("memory check requires base, actor, and phase (SDO_BASE_COMMIT, SDO_ACTOR, SDO_PHASE)")
	}
	if compile == nil {
		return nil, errors.New("memory check requires a compile runner")
	}
	request.Layers = Static
	diagnostics, err := Validate(ctx, request)
	if err != nil || len(diagnostics) > 0 {
		return diagnostics, err
	}
	request.Layers = Compile
	return compile(ctx, request)
}

// CompileCandidate is the runner body shared by local memory checks and the
// broker's validation Job. It builds, vets, and tests using the same builder.
func CompileCandidate(ctx context.Context, request Request) ([]Diagnostic, error) {
	if request.OperatorRoot == "" {
		return nil, errors.New("memory check requires operator sources (SDO_OPERATOR_ROOT)")
	}
	for _, name := range []string{"go.mod", "sdk/go.mod", "core/go.mod"} {
		if _, err := os.Stat(filepath.Join(request.OperatorRoot, name)); err != nil {
			return nil, fmt.Errorf("memory check operator sources: %w", err)
		}
	}
	if _, err := exec.LookPath("go"); err != nil {
		return nil, fmt.Errorf("memory check Go toolchain: %w", err)
	}
	if request.HealthyBaseline != "" {
		if info, err := os.Stat(request.HealthyBaseline); err != nil {
			return nil, fmt.Errorf("memory check healthy baseline: %w", err)
		} else if !info.IsDir() {
			return nil, errors.New("memory check healthy baseline must be a directory")
		}
	}
	request.Layers = Compile
	return Validate(ctx, request)
}

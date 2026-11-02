package validate

import (
	"context"
	"errors"
)

// CheckAccepted audits a complete saved tree using the acceptance validator's
// structural, semantic and compile gates. Publication ownership and incident
// gates apply to proposals, not to already accepted memory. Callers must pin and
// isolate the accepted revision before calling; this function never publishes it.
func CheckAccepted(ctx context.Context, request Request, compile CompileRunner) ([]Diagnostic, error) {
	if compile == nil {
		return nil, errors.New("accepted audit requires a compile runner")
	}
	request.Base, request.Actor, request.Phase, request.Publication = "", "", "", nil
	request.Layers = Structural | Semantic
	diagnostics, err := Validate(ctx, request)
	if err != nil || len(diagnostics) != 0 {
		return diagnostics, err
	}
	return compile(ctx, request)
}

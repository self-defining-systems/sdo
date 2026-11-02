//go:build integration

package runtime

import (
	"context"
	"errors"
	"testing"

	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/runtime/sim"
)

// checkReflectionInProcess runs the complete memory check (static, semantic and
// publication layers) the way `sdo-operator memory check` does for a responder
// reflection, but in process and with the compile layer simulated. Use it when
// the subject is a validation rule or a lifecycle decision. A test whose subject
// is the CLI, or whether learned Go compiles and replays, keeps the real binary.
func checkReflectionInProcess(t *testing.T, ctx context.Context, checkout, base, capture, incidentID string, compiler *sim.Compiler) []validate.Diagnostic {
	t.Helper()
	publication, err := validate.LoadPublicationContext(ctx, checkout)
	if err != nil {
		t.Fatal(err)
	}
	if publication == nil {
		publication = &validate.PublicationContext{BaseCommit: base, IncidentID: incidentID}
	}
	decision, err := validate.LoadCheckedReflectionDecision(ctx, checkout, "")
	if err != nil {
		var failure *validate.Failure
		if errors.As(err, &failure) {
			return failure.Diagnostics
		}
		t.Fatal(err)
	}
	paths, err := validate.ChangedPaths(ctx, checkout, base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := validate.NormalizeAuthoredWhitespace(checkout, paths); err != nil {
		t.Fatal(err)
	}
	if _, err := validate.NormalizeAuthoredPlaybooks(checkout, paths); err != nil {
		t.Fatal(err)
	}
	diagnostics, err := validate.Check(ctx, validate.Request{
		Root: checkout, Base: base, CaptureDirectory: capture, IncidentID: incidentID,
		Actor: validate.Responder, Phase: validate.ReflectionPhase, Publication: publication, Decision: decision,
	}, func(ctx context.Context, request validate.Request) ([]validate.Diagnostic, error) {
		changed, err := validate.ChangedPaths(ctx, request.Root, request.Base)
		if err != nil {
			return nil, err
		}
		return nil, compiler.Compile(ctx, request.Root, changed)
	})
	if err != nil {
		t.Fatal(err)
	}
	return diagnostics
}

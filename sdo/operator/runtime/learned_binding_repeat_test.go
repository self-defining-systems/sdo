package runtime

import (
	"context"

	"sdo.dev/operator/sdk/program"
)

// Synthetic learned source repairs share the live evidence shape: one required
// resource role, with no authored link from the generic opening health detector.
type learnedRepeatProgram struct {
	program.Diagnoser
	program.Mitigator
	spec program.ProgramSpec
	role string
}

func (p learnedRepeatProgram) Spec() program.ProgramSpec { return p.spec }

func (p learnedRepeatProgram) original(b program.Bindings) program.Bindings {
	return program.Bindings{Objects: program.ObjectBindings{p.role: b.Objects["resource"]}}
}

func (p learnedRepeatProgram) Diagnose(ctx context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	return p.Diagnoser.Diagnose(ctx, v, p.original(b))
}

func (p learnedRepeatProgram) Mitigate(ctx context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	return p.Mitigator.Mitigate(ctx, v, p.original(b))
}


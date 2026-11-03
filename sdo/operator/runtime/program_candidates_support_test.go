package runtime

import (
	"context"
	"sdo.dev/operator/sdk/program"
)

type candidateProgram struct {
	spec program.ProgramSpec
	run  func(context.Context, program.Bindings) program.Verdict
}

func (p candidateProgram) Spec() program.ProgramSpec { return p.spec }

func (p candidateProgram) Diagnose(ctx context.Context, _ program.ReadView, b program.Bindings) (program.Verdict, error) {
	return p.run(ctx, b), nil
}

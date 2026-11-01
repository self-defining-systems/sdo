package runtime

import (
	"context"
	"sdo.dev/operator/sdk/program"
)

type learningReplayDiagnoser struct{ fixed program.Result }

func (p learningReplayDiagnoser) Spec() program.ProgramSpec { return (fastloopPortProgram{}).Spec() }

func (p learningReplayDiagnoser) Diagnose(_ context.Context, view program.ReadView, bindings program.Bindings) (program.Verdict, error) {
	for _, service := range view.Services() {
		if service.Name == bindings["service"].Name && service.Spec.Ports[0].TargetPort.IntVal == 9999 {
			return program.Verdict{Result: program.Match}, nil
		}
	}
	return program.Verdict{Result: p.fixed}, nil
}

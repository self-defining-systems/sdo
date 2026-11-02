package runtime

import (
	"context"

	"sdo.dev/operator/sdk/program"
)

// An already accepted/imported registry entry needs no incident corpus to
// derive authority. Its application identity is the deployed chart's name.
type adoptedFlagProgram struct{}

func (adoptedFlagProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "synthetic-adopted-high-cpu", Params: []program.ParamSpec{{Name: "config", Kind: "ConfigMap", Required: true}}}
}

func (adoptedFlagProgram) Diagnose(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {
	return program.Verdict{Result: program.Match}, nil
}

func (adoptedFlagProgram) Mitigate(_ context.Context, _ program.ReadView, b program.Bindings) (program.Plan, error) {
	ref := b.Objects["config"]
	return program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "data[flags.json]", DocumentFormat: "json", DocumentPath: []string{"flags", "highCpu", "defaultVariant"}, Value: program.FromSource(ref, "data[flags.json]")}}}, nil
}


package mongoauth

import (
	"context"
	"fixture-diagnostics/lib/mongorelation"
	"sdo.dev/operator/sdk/program"
)

type Program struct{}

func (Program) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "mongo-auth-bootstrap", Params: []program.ParamSpec{{Name: "pod", Kind: "Pod", Required: true}}}
}
func (Program) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	return mongorelation.Diagnose(v, b.Objects["pod"])
}

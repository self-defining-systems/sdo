// Package driftbound is a synthetic source-absent RBAC repair fixture.
package driftbound

import (
	"context"
	"sdo.dev/operator/sdk/program"
)

type Program struct{}

func (Program) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "source-absent-binding", Params: []program.ParamSpec{{Name: "binding", Kind: "ClusterRoleBinding", Required: true}}}
}
func (Program) Diagnose(_ context.Context, view program.ReadView, bindings program.Bindings) (program.Verdict, error) {
	diff, err := view.DiffAgainstSource(bindings.Objects["binding"])
	if err != nil {
		return program.Verdict{}, err
	}
	result := program.NoMatch
	if diff.PresentButNotInSource() {
		result = program.Match
	}
	return program.Verdict{Result: result}, nil
}
func (Program) Mitigate(_ context.Context, _ program.ReadView, bindings program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.DeleteObject{Ref: bindings.Objects["binding"], Reason: "remove source-absent authority"}}}, nil
}

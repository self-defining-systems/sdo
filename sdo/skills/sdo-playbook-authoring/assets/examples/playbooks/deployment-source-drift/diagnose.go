// Package deploymentsourcedrift restores a Deployment that drifted from accepted source.
// The same shape covers a ConfigMap or Service: change the role Kind.
package deploymentsourcedrift

import (
	"context"

	"sdo.dev/operator/sdk/program"
)

type Program struct{}

func (Program) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "deployment-source-drift", Params: []program.ParamSpec{{Name: "deployment", Kind: "Deployment", Required: true}}}
}

func (Program) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	ref := b.Objects["deployment"]
	diff, err := v.DiffAgainstSource(ref)
	if err != nil {
		return program.Verdict{}, err // unavailable evidence is Unknown, never NoMatch
	}
	if !diff.Changed || diff.SourceAbsent {
		return program.Verdict{Result: program.NoMatch}, nil
	}
	return program.Verdict{Result: program.Match, Evidence: []program.Evidence{{Resource: ref, Fact: "live object differs from accepted source", Observed: diff.Text, Expected: "accepted source"}}}, nil
}

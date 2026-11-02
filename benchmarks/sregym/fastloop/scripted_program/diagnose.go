package service_targetport_mismatch

import (
	"context"

	"sdo.dev/operator/sdk/program"
)

type Program struct{}

func (Program) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "service-targetport-mismatch", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}
}

// Diagnose compares the Service with its accepted source, as a learned program
// does: the memory host answers the comparison from the diff the controller froze.
func (Program) Diagnose(_ context.Context, view program.ReadView, bindings program.Bindings) (program.Verdict, error) {
	ref := bindings.Objects["service"]
	diff, err := view.DiffAgainstSource(ref)
	if err != nil {
		return program.Verdict{Result: program.Unknown}, err
	}
	if diff.SourceAbsent {
		return program.Verdict{Result: program.Unknown}, nil
	}
	if !diff.Changed {
		return program.Verdict{Result: program.NoMatch}, nil
	}
	return program.Verdict{Result: program.Match, Evidence: []program.Evidence{{Resource: ref, Fact: "live object differs from accepted source", Observed: diff.Text, Expected: "accepted source"}}}, nil
}

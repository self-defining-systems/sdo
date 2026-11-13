// Package templatedrift sets a Deployment's container image back to what accepted source declares.
package templatedrift

import (
	"context"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type Program struct{}

// Spec declares no object role: the program is tried on every incident and finds its subject itself.
func (Program) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "template-drift"} }

type drift struct {
	deployment sdk.ObjectRef
	container  string
	path       string
}

// drifted finds the cause: a live pod template whose image differs from accepted source. It reads no Pods or logs.
func drifted(v program.ReadView) ([]drift, error) {
	var found []drift
	for _, d := range v.Deployments() {
		ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: d.Namespace, Name: d.Name}
		diff, err := v.DiffAgainstSource(ref)
		if err != nil {
			return nil, err // unavailable evidence is Unknown, never NoMatch
		}
		if !diff.Changed {
			continue
		}
		changes, err := diff.ContainerChanges()
		if err != nil {
			return nil, err
		}
		for _, c := range changes {
			if c.Changed("image") {
				found = append(found, drift{ref, c.Name, "spec.template.spec.containers[name=" + c.Name + "].image"})
			}
		}
	}
	return found, nil
}

func (Program) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	found, err := drifted(v)
	if err != nil {
		return program.Verdict{}, err
	}
	if len(found) == 0 {
		return program.Verdict{Result: program.NoMatch}, nil // no subject: not this fault
	}
	return program.Verdict{Result: program.Match, Evidence: []program.Evidence{{Resource: found[0].deployment, Fact: "template image differs from accepted source", Observed: found[0].container, Expected: "the accepted image"}}}, nil
}

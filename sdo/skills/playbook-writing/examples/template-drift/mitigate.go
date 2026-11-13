package templatedrift

import (
	"context"

	"sdo.dev/operator/sdk/program"
)

// Mitigate is the change that fixed it: the accepted image written back, as the responder ran it.
func (Program) Mitigate(_ context.Context, v program.ReadView, _ program.Bindings) (program.Plan, error) {
	found, err := drifted(v)
	if err != nil {
		return program.Plan{}, err
	}
	var actions []program.Action
	for _, d := range found {
		actions = append(actions, program.SetField{Ref: d.deployment, Path: d.path, Value: program.FromSource(d.deployment, d.path), Reason: "the image drifted from accepted source"})
	}
	return program.Plan{Actions: actions}, nil
}

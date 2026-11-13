package stuckpods

import (
	"context"

	"sdo.dev/operator/sdk/program"
)

// Mitigate deletes each crashing Pod, as the responder did, so the controller replaces it.
func (Program) Mitigate(_ context.Context, v program.ReadView, _ program.Bindings) (program.Plan, error) {
	var actions []program.Action
	for _, c := range crashing(v) {
		actions = append(actions, program.DeletePod{Ref: c.pod})
	}
	return program.Plan{Actions: actions}, nil
}

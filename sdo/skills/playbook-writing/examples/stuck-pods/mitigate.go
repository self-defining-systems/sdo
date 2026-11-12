package stuckpods

import (
	"context"

	"sdo.dev/operator/sdk/program"
)

// Mitigate deletes each crashing Pod, as the responder did, so the controller replaces it.
func (Program) Mitigate(_ context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	pods, err := crashing(v, b)
	if err != nil {
		return program.Plan{}, err
	}
	var actions []program.Action
	for _, pod := range pods {
		actions = append(actions, program.DeletePod{Ref: pod})
	}
	return program.Plan{Actions: actions}, nil
}

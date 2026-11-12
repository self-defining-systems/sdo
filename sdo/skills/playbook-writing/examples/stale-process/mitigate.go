package staleprocess

import (
	"context"

	"sdo.dev/operator/sdk/program"
)

// Mitigate is the change that fixed it: a rolling restart of the component.
func (Program) Mitigate(_ context.Context, _ program.ReadView, b program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.RestartRollout{Ref: b.Objects["component"], Reason: "the process held stale state"}}}, nil
}

package deploymentsourcedrift

import (
	"context"

	"sdo.dev/operator/sdk/program"
)

// Mitigate restores the whole object; use SetField with FromSource to restore one field.
// StalledRolloutRelease frees a rollout whose new Pods cannot be placed while old ones hold the place.
func (Program) Mitigate(_ context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	ref := b.Objects["deployment"]
	actions := []program.Action{program.ReconcileFromSource{Ref: ref}}
	actions = append(actions, program.StalledRolloutRelease(v, ref)...)
	return program.Plan{Actions: actions, Rationale: "restore accepted source"}, nil
}

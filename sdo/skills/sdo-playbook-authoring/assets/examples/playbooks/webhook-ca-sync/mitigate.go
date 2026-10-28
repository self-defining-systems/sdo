package webhookcasync

import (
	"context"

	"sdo.dev/operator/sdk/program"
)

func (Program) Mitigate(_ context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	stale, err := staleCA(v, b)
	if err != nil {
		return program.Plan{}, err
	}
	var actions []program.Action
	for _, target := range stale {
		actions = append(actions, program.SyncWebhookCA{Target: target, Reason: "caBundle differs from the serving Secret"})
	}
	return program.Plan{Actions: actions}, nil
}

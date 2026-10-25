package mongoauth

import (
	"context"
	"fixture-diagnostics/lib/mongorelation"
	"sdo.dev/operator/sdk/program"
)

func (Program) Mitigate(_ context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	config, err := mongorelation.Configuration(v, b.Objects["pod"])
	if err != nil {
		return program.Plan{}, err
	}
	return program.Plan{Actions: []program.Action{program.RunScript{Script: `kubectl exec --namespace "$NAMESPACE" "$POD" --container "$CONTAINER" -- bash "$MOUNT/$ITEM"`, Env: config.Inputs, Reason: "restore the account using its current mounted initialization routine"}}}, nil
}

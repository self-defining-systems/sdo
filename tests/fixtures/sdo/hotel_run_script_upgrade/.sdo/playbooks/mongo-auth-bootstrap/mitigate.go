package mongoauth

import (
	"context"
	"fixture-diagnostics/lib/mongorelation"
	"sdo.dev/operator/sdk/program"
)

func (p Program) Mitigate(ctx context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	verdict, err := p.Diagnose(ctx, v, b)
	if err != nil || verdict.Result != program.Match {
		return program.Plan{}, err
	}
	config, err := mongorelation.Configuration(v, b.Objects["pod"])
	if err != nil {
		return program.Plan{}, err
	}
	return program.Plan{Actions: []program.Action{program.RunScript{Script: `kubectl exec --namespace "$NAMESPACE" "$POD" --container "$CONTAINER" -- bash "$MOUNT/$ITEM"`, Env: config.Inputs, Reason: "restore the account using its current mounted initialization routine"}}}, nil
}

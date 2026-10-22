package routingport

import (
	"context"
	"sdo.dev/operator/sdk/program"
)

func (p Program) Mitigate(ctx context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	verdict, err := p.Diagnose(ctx, v, b)
	if err != nil || verdict.Result != program.Match {
		return program.Plan{}, err
	}
	_, _, port, ok := replacement(v, b)
	if !ok {
		return program.Plan{}, nil
	}
	return program.Plan{Actions: []program.Action{program.SetField{Ref: b["service"], Path: "spec.ports[0].targetPort", Value: port, Reason: "use observed selected-Pod listener"}}}, nil
}

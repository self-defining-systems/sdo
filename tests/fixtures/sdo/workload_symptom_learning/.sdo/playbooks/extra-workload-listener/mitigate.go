package repair

import (
	"context"
	"sdo.dev/operator/sdk/program"
)

func (Program) Mitigate(_ context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.PatchObject{Ref: b.Objects["deployment"], Edits: []program.ListEdit{{Path: "/spec/template/spec/containers", Match: map[string]any{"name": "sidecar"}, Op: "remove"}}, Reason: "remove conflicting extra listener"}}}, nil
}

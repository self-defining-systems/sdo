package runtime

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type terminalShapeProgram struct {
	pipelinePortProgram
	shape    string
	calls    *atomic.Int32
	compiled program.Mitigator
}

func (p terminalShapeProgram) Mitigate(ctx context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	p.calls.Add(1)
	if p.compiled != nil {
		return p.compiled.Mitigate(ctx, v, b)
	}
	if p.shape == "empty" {
		return program.Plan{}, nil
	}
	return program.Plan{Actions: []program.Action{program.PatchObject{Ref: b.Objects["service"], Type: "merge", Patch: []byte(`{}`), Edits: []program.ListEdit{{Path: "/spec/ports", Match: map[string]any{"name": "http"}, Op: "remove"}}, Reason: "synthetic invalid shape"}}}, nil
}

type bindingShapeProgram struct {
	pipelinePortProgram
	empty string
	calls *atomic.Int32
}

func (p bindingShapeProgram) Mitigate(ctx context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	p.calls.Add(1)
	if b.Objects["service"].Name == p.empty {
		return program.Plan{}, nil
	}
	return p.pipelinePortProgram.Mitigate(ctx, v, b)
}

type scalarShapeProgram struct{ pipelinePortProgram }

func (scalarShapeProgram) Spec() program.ProgramSpec {
	spec := (pipelinePortProgram{}).Spec()
	spec.Params = append(spec.Params, program.ParamSpec{Name: "executable", ValueKind: program.BoolValue, Default: false})
	return spec
}

func (p scalarShapeProgram) Mitigate(ctx context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	if b.Values["executable"] != true {
		return program.Plan{}, nil
	}
	return p.pipelinePortProgram.Mitigate(ctx, v, b)
}

type transientPlanProgram struct {
	pipelinePortProgram
	calls *atomic.Int32
}

func (p transientPlanProgram) Mitigate(ctx context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	if p.calls.Add(1) == 1 {
		return program.Plan{}, fmt.Errorf("live observation unavailable")
	}
	return p.pipelinePortProgram.Mitigate(ctx, v, b)
}

type transientCompiledReadView struct {
	program.ReadView
	reads atomic.Int32
}

func (v *transientCompiledReadView) Logs(sdk.ObjectRef, string, time.Duration) (string, error) {
	if v.reads.Add(1) == 1 {
		return "", fmt.Errorf("synthetic observation unavailable")
	}
	return "synthetic observation", nil
}

type mutatingFailureProgram struct {
	pipelinePortProgram
	calls *atomic.Int32
}

func (p mutatingFailureProgram) Mitigate(ctx context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	p.calls.Add(1)
	if b.Objects["service"].Name == "api" {
		b.Objects["service"] = sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "peer"}
		return program.Plan{}, nil
	}
	return p.pipelinePortProgram.Mitigate(ctx, v, b)
}


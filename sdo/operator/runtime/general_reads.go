package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"sdo.dev/operator/sdk/program"
	"time"
)

func (v LiveReadView) Observe(o program.ExternalOperation) (int32, error) {
	if err := o.Validate(); err != nil {
		return 0, err
	}
	captured, err := v.Object(o.Pod())
	if err != nil || captured == nil || captured.GetUID() == "" {
		return 0, fmt.Errorf("command Pod absent from snapshot")
	}
	ctx, cancel := context.WithTimeout(v.context(), 5*time.Second)
	defer cancel()
	client := TypedPlanClient{Client: v.Client, ExecConfig: v.DatabaseExecConfig, ProtectedNamespace: v.ProtectedNamespace}
	before, err := client.Get(ctx, o.Pod())
	if err != nil || before == nil || before.GetUID() != captured.GetUID() {
		return 0, fmt.Errorf("command Pod identity changed")
	}
	code, err := client.RunOperation(ctx, o)
	if err != nil {
		return 0, err
	}
	after, err := client.Get(ctx, o.Pod())
	if err != nil || after == nil || after.GetUID() != captured.GetUID() {
		return 0, fmt.Errorf("command Pod identity changed")
	}
	return code, nil
}
func (v LiveReadView) KubernetesRead(r program.KubernetesRequest) (json.RawMessage, error) {
	if err := program.ValidateKubernetesRead(r); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(v.context(), 5*time.Second)
	defer cancel()
	return (TypedPlanClient{Client: v.Client, ExecConfig: v.DatabaseExecConfig, ProtectedNamespace: v.ProtectedNamespace}).KubernetesRequest(ctx, r)
}
func (v *cachedReadView) Observe(o program.ExternalOperation) (int32, error) {
	var r int32
	err := v.read("Observe", []any{o}, &r, func() (any, error) { return v.ReadView.Observe(o) })
	return r, err
}
func (v *cachedReadView) KubernetesRead(q program.KubernetesRequest) (json.RawMessage, error) {
	var r json.RawMessage
	err := v.read("KubernetesRead", []any{q}, &r, func() (any, error) { return v.ReadView.KubernetesRead(q) })
	return r, err
}

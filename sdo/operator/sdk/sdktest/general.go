package sdktest

import (
	"encoding/json"
	"fmt"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"strings"
)

func (v *RecordingReadView) Observe(o program.ExternalOperation) (int32, error) {
	if err := o.Validate(); err != nil {
		return 0, err
	}
	r, err := v.ReadView.Observe(o)
	if err != nil {
		err = fmt.Errorf("external observation unavailable")
	}
	v.record("Observe", []any{o}, r, err)
	return r, err
}
func (v *ReplayReadView) Observe(o program.ExternalOperation) (int32, error) {
	if err := o.Validate(); err != nil {
		return 0, err
	}
	var r int32
	err := v.replay("Observe", []any{o}, &r)
	return r, err
}
func (s Snapshot) Observe(o program.ExternalOperation) (int32, error) {
	return (&ReplayReadView{Snapshot: s, Calls: s.Readings}).Observe(o)
}
func (v *RecordingReadView) KubernetesRead(q program.KubernetesRequest) (json.RawMessage, error) {
	if err := program.ValidateKubernetesRead(q); err != nil {
		return nil, err
	}
	r, err := v.ReadView.KubernetesRead(q)
	if err == nil {
		projected, projectionErr := sdk.ProjectWebhookCAJSON(r)
		if projectionErr == nil {
			r = projected
		} else if strings.Contains(q.Path, "webhookconfigurations") {
			err = projectionErr
		}
	}
	if err != nil {
		err = fmt.Errorf("Kubernetes API observation unavailable")
	}
	v.record("KubernetesRead", []any{q}, r, err)
	return r, err
}
func (v *ReplayReadView) KubernetesRead(q program.KubernetesRequest) (json.RawMessage, error) {
	if err := program.ValidateKubernetesRead(q); err != nil {
		return nil, err
	}
	var r json.RawMessage
	err := v.replay("KubernetesRead", []any{q}, &r)
	return r, err
}
func (s Snapshot) KubernetesRead(q program.KubernetesRequest) (json.RawMessage, error) {
	return (&ReplayReadView{Snapshot: s, Calls: s.Readings}).KubernetesRead(q)
}

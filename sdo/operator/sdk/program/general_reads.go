package program

import (
	"encoding/json"
	"fmt"
	"strings"
)

func (v snapshotReadView) Observe(o ExternalOperation) (int32, error) {
	if err := o.Validate(); err != nil {
		return 0, err
	}
	if s, ok := v.DetectionContext.(interface {
		Observe(ExternalOperation) (int32, error)
	}); ok {
		return s.Observe(o)
	}
	return 0, fmt.Errorf("external observation unavailable")
}
func (v snapshotReadView) KubernetesRead(r KubernetesRequest) (json.RawMessage, error) {
	if err := ValidateKubernetesRead(r); err != nil {
		return nil, err
	}
	if s, ok := v.DetectionContext.(interface {
		KubernetesRead(KubernetesRequest) (json.RawMessage, error)
	}); ok {
		return s.KubernetesRead(r)
	}
	return nil, fmt.Errorf("Kubernetes API observation unavailable")
}
func ValidateKubernetesRead(r KubernetesRequest) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if r.Method != "GET" && r.Method != "HEAD" {
		return fmt.Errorf("read view requires GET or HEAD")
	}
	parts := strings.Split(strings.TrimPrefix(r.Path, "/"), "/")
	index := 2
	if parts[0] == "apis" {
		index = 3
	}
	if len(parts) > index && parts[index] == "watch" {
		index++
	}
	if len(parts) > index+2 && parts[index] == "namespaces" {
		index += 2
	}
	if len(parts) > index+2 && parts[index] == "pods" {
		switch parts[index+2] {
		case "exec", "attach", "portforward":
			return fmt.Errorf("connection subresources require a recorded action")
		}
	}
	return nil
}
func (v *checkedReadView) Observe(o ExternalOperation) (int32, error) {
	r, err := v.ReadView.Observe(o)
	v.check(err)
	return r, err
}
func (v *checkedReadView) KubernetesRead(q KubernetesRequest) (json.RawMessage, error) {
	r, err := v.ReadView.KubernetesRead(q)
	v.check(err)
	return r, err
}

package service_targetport_mismatch

import (
	"context"
	"strconv"

	"sdo.dev/operator/sdk/program"
)

// Mitigate restores the Service's ports from accepted source.
func (Program) Mitigate(_ context.Context, view program.ReadView, bindings program.Bindings) (program.Plan, error) {
	ref := bindings.Objects["service"]
	for _, service := range view.Services() {
		if service.Namespace != ref.Namespace || service.Name != ref.Name {
			continue
		}
		var actions []program.Action
		for index := range service.Spec.Ports {
			path := "spec.ports[" + strconv.Itoa(index) + "].targetPort"
			actions = append(actions, program.SetField{Ref: ref, Path: path, Value: program.FromSource(ref, path), Reason: "restore accepted targetPort"})
		}
		return program.Plan{Actions: actions}, nil
	}
	return program.Plan{}, nil
}

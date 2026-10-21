package runtime

import (
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/worker_registry"
	"sdo.dev/operator/worker_registry/registrytest"
)

func builtInReflectorRegistration(image string) *contracts.AgentRegistration {
	return testRegistration("sdo-reflector", "outcome-reflection-direct-v1", image)
}
func builtInResponderRegistration(image string, command []string) *contracts.AgentRegistration {
	r := testRegistration("sdo-responder", "incident-response", image)
	r.GetKubernetesJob().Command = command
	return r
}
func testRegistration(id, task, image string) *contracts.AgentRegistration {
	r, err := worker_registry.LoadBuiltIn(registrytest.Directory(), image)
	if err != nil {
		panic(err)
	}
	registration, err := r.Resolve(id, task)
	if err != nil {
		panic(err)
	}
	return registration
}
func decodeIncidentAgentResult(data []byte, task *contracts.AgentTask, registry *worker_registry.Registry) (IncidentResult, error) {
	result := new(contracts.AgentResult)
	if err := wire.Unmarshal(data, result); err != nil {
		return IncidentResult{}, err
	}
	return incidentResultFromAgent(result, task, registry)
}

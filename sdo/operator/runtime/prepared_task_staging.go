package runtime

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/worker_registry"
)

// inlinePreparedTaskBytes is the largest prepared task a state document holds itself. Larger tasks carry the
// logical memory snapshot, which grows with the repository, so the state keeps only a digest reference.
const inlinePreparedTaskBytes = 16 << 10

// PreparedTaskStager keeps the frozen canonical task outside the controller state ConfigMap. A dispatcher that
// implements it stages the bytes before the state that references them is saved.
type PreparedTaskStager interface {
	StagePreparedTask(incidentID string, payload []byte) (reference string, err error)
	ResolvePreparedTask(incidentID, reference string) ([]byte, error)
	ReleasePreparedTask(incidentID string)
}

// StagePreparedTask implements PreparedTaskStager on the repository volume. Without a volume the task stays inline.
func (d KubernetesJobDispatcher) StagePreparedTask(incidentID string, payload []byte) (string, error) {
	if d.RepositoryMountPath == "" {
		return string(payload), nil
	}
	return worker_registry.PreparedTaskStore{MountPath: d.RepositoryMountPath}.Stage(worker_registry.JobName(incidentID), payload)
}

func (d KubernetesJobDispatcher) ResolvePreparedTask(incidentID, reference string) ([]byte, error) {
	return worker_registry.PreparedTaskStore{MountPath: d.RepositoryMountPath}.Resolve(worker_registry.JobName(incidentID), reference)
}

func (d KubernetesJobDispatcher) ReleasePreparedTask(incidentID string) {
	if d.RepositoryMountPath != "" {
		worker_registry.PreparedTaskStore{MountPath: d.RepositoryMountPath}.Release(worker_registry.JobName(incidentID))
	}
}

// externalizePreparedTask returns the state to save: a large prepared task is staged first and replaced by its
// reference. The controller keeps the full bytes in memory. The second result is the incident whose task the
// returned state references, or "".
func (c *Controller) externalizePreparedTask(state *contracts.ControllerState) (*contracts.ControllerState, string, error) {
	stager, ok := c.dispatcher.(PreparedTaskStager)
	request := state.GetIncidentRequest()
	if !ok || len(request.GetPreparedAgentTask()) <= inlinePreparedTaskBytes || worker_registry.IsPreparedTaskReference(request.GetPreparedAgentTask()) {
		return state, "", nil
	}
	reference, err := stager.StagePreparedTask(request.GetIncidentId(), []byte(request.GetPreparedAgentTask()))
	if err != nil {
		return nil, "", fmt.Errorf("stage prepared agent task: %w", err)
	}
	if !worker_registry.IsPreparedTaskReference(reference) {
		return state, "", nil
	}
	saved := proto.Clone(state).(*contracts.ControllerState)
	saved.IncidentRequest.PreparedAgentTask = reference
	return saved, request.GetIncidentId(), nil
}

// resolvePreparedTask restores the bytes a loaded state references, verifying their digest.
func (c *Controller) resolvePreparedTask(state *contracts.ControllerState) (*contracts.ControllerState, error) {
	request := state.GetIncidentRequest()
	if !worker_registry.IsPreparedTaskReference(request.GetPreparedAgentTask()) {
		return state, nil
	}
	stager, ok := c.dispatcher.(PreparedTaskStager)
	if !ok {
		return nil, fmt.Errorf("controller state references a staged agent task but the dispatcher cannot resolve it")
	}
	payload, err := stager.ResolvePreparedTask(request.GetIncidentId(), request.GetPreparedAgentTask())
	if err != nil {
		return nil, err
	}
	resolved := proto.Clone(state).(*contracts.ControllerState)
	resolved.IncidentRequest.PreparedAgentTask = string(payload)
	c.stagedTaskIncident = request.GetIncidentId()
	return resolved, nil
}

// releaseStaleStagedTask drops the staged bytes of an earlier incident once a saved state no longer references them.
func (c *Controller) releaseStaleStagedTask(current string) {
	if stager, ok := c.dispatcher.(PreparedTaskStager); ok && c.stagedTaskIncident != "" && c.stagedTaskIncident != current {
		stager.ReleasePreparedTask(c.stagedTaskIncident)
	}
	c.stagedTaskIncident = current
}

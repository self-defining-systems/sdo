package runtime

import (
	"fmt"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
)

// ResponderUsage is a transport-neutral retirement accounting boundary: the incident's agent usage is its
// responder session plus every investigator session started for it (their own sessions, `<incident>-inv-<n>`).
// The storage edge owns physical paths; the worker owns provider normalization. A stopped investigator counts
// through its last checkpoint and marks the total partial; nil means nothing was observed.
func (d KubernetesJobDispatcher) ResponderUsage(id string) (*contracts.UsageMetrics, error) {
	total, err := memory.ResponderUsage(d.RepositoryMountPath, d.RepositoryPVCSubPath, id)
	if err != nil || !d.Investigators.Enabled() {
		return total, err
	}
	for index := 1; index <= d.Investigators.Count; index++ {
		usage, err := memory.ResponderUsage(d.RepositoryMountPath, d.RepositoryPVCSubPath, investigatorID(id, index))
		if err != nil {
			return nil, fmt.Errorf("investigator %d usage: %w", index, err)
		}
		if usage == nil {
			continue
		}
		if total == nil {
			total = usage
			continue
		}
		total = memory.AddUsage(total, usage)
	}
	return total, nil
}

func (c *Controller) responderUsage(id string) (*contracts.UsageMetrics, error) {
	if source, ok := c.dispatcher.(interface {
		ResponderUsage(string) (*contracts.UsageMetrics, error)
	}); ok {
		return source.ResponderUsage(id)
	}
	return nil, nil
}

// Bind accounting before freezing ordinary/failed closures, including workers
// that exited without any normalized result or resumable session identity.
// Early closures bind only after grace retires the still-running worker.
func (c *Controller) bindClosureUsage(closure *contracts.IncidentClosure) error {
	usage, err := c.responderUsage(closure.GetRequest().GetIncidentId())
	if err != nil {
		return fmt.Errorf("freeze responder usage: %w", err)
	}
	if usage == nil {
		return nil
	}
	if closure.Result == nil {
		closure.Result = &contracts.IncidentResult{SchemaVersion: ProtocolSchemaVersion,
			IncidentId: closure.GetRequest().GetIncidentId(), Status: string(IncidentFailed),
			Timing: &contracts.TimingMetrics{StartedAt: closure.DispatchedAt, CompletedAt: closure.ResponderCompletedAt}}
	}
	closure.Result.Usage = usage
	return nil
}

package restart_loop

import (
	"context"
	"fmt"
	"time"

	"sdo.dev/operator/sdk"
)

// restartThreshold is the restart count at which a waiting container counts as
// looping rather than recovering from a single crash.
const restartThreshold = 3

type Detector struct{}

func New() sdk.Detector { return Detector{} }

// Spec must agree with manifest.yaml field for field.
func (Detector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{
		ID:                  "restart-loop",
		Class:               sdk.DetectorClassIncident,
		Owner:               sdk.DetectorOwnerResponder,
		Description:         "find containers that keep restarting while waiting in CrashLoopBackOff",
		Watches:             []sdk.WatchKind{{APIVersion: "v1", Kind: "Pod"}},
		Interval:            30 * time.Second,
		Persistence:         sdk.PersistencePolicy{Firing: 1, Clearing: 2},
		Batching:            sdk.BatchingPolicy{Severity: sdk.SeverityCritical},
		Playbooks:           []string{"restart-loop"},
		OriginatingIncident: "example-incident",
		OriginatingCommit:   "example",
	}
}

// Detect is deterministic: it reads only the snapshot, never the environment,
// the clock, or the network.
func (Detector) Detect(_ context.Context, snapshot sdk.DetectionContext) ([]sdk.Finding, error) {
	findings := []sdk.Finding{}
	for _, pod := range snapshot.Pods() {
		for _, status := range pod.Status.ContainerStatuses {
			waiting := status.State.Waiting
			if waiting == nil || waiting.Reason != "CrashLoopBackOff" || status.RestartCount < restartThreshold {
				continue
			}
			podRef := sdk.ObjectRefFrom("Pod", "v1", &pod)
			findings = append(findings, sdk.Finding{
				RuleID:            "restart-loop",
				Status:            sdk.FindingActive,
				Severity:          sdk.SeverityCritical,
				Summary:           "A container keeps restarting",
				Evidence:          fmt.Sprintf("container %s in pod %s/%s restarted %d times and is in CrashLoopBackOff", status.Name, pod.Namespace, pod.Name, status.RestartCount),
				PrimaryResource:   podRef,
				Playbooks:         []string{"restart-loop"},
				ParameterBindings: map[string]sdk.ObjectRef{"target_resource": podRef},
				Fingerprint:       pod.Namespace + "/" + pod.Name + "/" + status.Name,
			})
		}
	}
	return findings, nil
}

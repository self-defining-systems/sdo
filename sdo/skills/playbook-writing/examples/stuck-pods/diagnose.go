// Package stuckpods replaces Pods that crash loop under a Deployment.
package stuckpods

import (
	"context"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type Program struct{}

// Spec declares no object role: the program is tried on every incident and finds its subject itself.
func (Program) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "stuck-pods"} }

type crashed struct {
	pod       sdk.ObjectRef
	container string
}

// crashing finds the cause by relation (Pod -> ReplicaSet -> Deployment owners), filtering on Pod state only.
func crashing(v program.ReadView) []crashed {
	owned := map[string]bool{}
	for _, rs := range v.ReplicaSets() {
		for _, owner := range rs.OwnerReferences {
			if owner.Kind == "Deployment" {
				owned[rs.Namespace+"/"+rs.Name] = true
			}
		}
	}
	var found []crashed
	for _, pod := range v.Pods() {
		if len(pod.OwnerReferences) == 0 || !owned[pod.Namespace+"/"+pod.OwnerReferences[0].Name] {
			continue
		}
		for _, status := range pod.Status.ContainerStatuses {
			if w := status.State.Waiting; w != nil && w.Reason == "CrashLoopBackOff" && status.RestartCount >= 3 {
				found = append(found, crashed{sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name}, status.Name})
				break
			}
		}
	}
	return found
}

func (Program) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	pods := crashing(v)
	if len(pods) == 0 {
		return program.Verdict{Result: program.NoMatch}, nil // no subject: not this fault
	}
	observed := ""
	// Logs cost a read: one call, after the state filter, only as supporting Evidence.
	if logs, err := v.Logs(pods[0].pod, pods[0].container, time.Minute); err == nil {
		observed = logs
	}
	return program.Verdict{Result: program.Match, Evidence: []program.Evidence{{Resource: pods[0].pod, Fact: "a Pod is crash looping", Observed: observed, Expected: "running"}}}, nil
}

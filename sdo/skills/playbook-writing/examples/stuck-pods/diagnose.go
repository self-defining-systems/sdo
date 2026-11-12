// Package stuckpods replaces the Pods of a workload that are stuck terminating or crash looping.
package stuckpods

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type Program struct{}

func (Program) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "stuck-pods", Params: []program.ParamSpec{{Name: "component", Kind: "Deployment", Required: true}}}
}

// crashing lists the component's Pods that are crash looping, found by relation (the selector), not by name.
func crashing(v program.ReadView, b program.Bindings) ([]sdk.ObjectRef, error) {
	ref := b.Objects["component"]
	for _, deployment := range v.Deployments() {
		if deployment.Namespace != ref.Namespace || deployment.Name != ref.Name || deployment.Spec.Selector == nil {
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
		if err != nil {
			return nil, err
		}
		var pods []sdk.ObjectRef
		for _, pod := range v.Pods() {
			if pod.Namespace != ref.Namespace || !selector.Matches(labels.Set(pod.Labels)) {
				continue
			}
			for _, status := range pod.Status.ContainerStatuses {
				if status.State.Waiting != nil && status.State.Waiting.Reason == "CrashLoopBackOff" {
					pods = append(pods, sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name})
					break
				}
			}
		}
		return pods, nil
	}
	return nil, fmt.Errorf("component %s/%s is not in the snapshot", ref.Namespace, ref.Name)
}

func (Program) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	pods, err := crashing(v, b)
	if err != nil {
		return program.Verdict{}, err
	}
	if len(pods) == 0 {
		return program.Verdict{Result: program.NoMatch}, nil
	}
	return program.Verdict{Result: program.Match, Evidence: []program.Evidence{{Resource: b.Objects["component"], Fact: "Pods are crash looping", Observed: pods[0].Name, Expected: "running"}}}, nil
}

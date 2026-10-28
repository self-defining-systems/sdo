// Package crashloopscript saves a recorded hand command as a RunScript step.
// Prefer a typed action when one fits; RunScript encodes any command exactly.
package crashloopscript

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sdo.dev/operator/sdk/program"
)

type Program struct{}

func (Program) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "crashloop-script-repair", Params: []program.ParamSpec{{Name: "deployment", Kind: "Deployment", Required: true}}}
}

func (Program) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	ref := b.Objects["deployment"]
	for _, deployment := range v.Deployments() {
		if deployment.Namespace != ref.Namespace || deployment.Name != ref.Name || deployment.Spec.Selector == nil {
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
		if err != nil {
			return program.Verdict{}, err
		}
		for _, pod := range v.Pods() {
			if pod.Namespace != ref.Namespace || !selector.Matches(labels.Set(pod.Labels)) {
				continue
			}
			for _, status := range pod.Status.ContainerStatuses {
				if status.State.Waiting != nil && status.State.Waiting.Reason == "CrashLoopBackOff" {
					return program.Verdict{Result: program.Match, Evidence: []program.Evidence{{Resource: ref, Fact: "selected Pod is crash looping", Observed: pod.Name, Expected: "running"}}}, nil
				}
			}
		}
		return program.Verdict{Result: program.NoMatch}, nil
	}
	return program.Verdict{Result: program.Unknown}, nil
}

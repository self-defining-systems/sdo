// Package staleprocess restarts a workload whose process logged a fatal stale-state error.
package staleprocess

import (
	"context"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type Program struct{}

func (Program) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "stale-process", Params: []program.ParamSpec{{Name: "component", Kind: "Deployment", Required: true}}}
}

// Diagnose matches when a Pod of the component logged the error that confirmed the cause.
func (Program) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	ref := b.Objects["component"]
	for _, deployment := range v.Deployments() {
		if deployment.Namespace != ref.Namespace || deployment.Name != ref.Name || deployment.Spec.Selector == nil {
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
		if err != nil {
			return program.Verdict{}, err
		}
		for _, pod := range v.Pods() {
			if pod.Namespace != ref.Namespace || !selector.Matches(labels.Set(pod.Labels)) || len(pod.Spec.Containers) == 0 {
				continue
			}
			logs, err := v.Logs(sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name}, pod.Spec.Containers[0].Name, 5*time.Minute)
			if err != nil {
				return program.Verdict{}, err // unavailable evidence is Unknown, never NoMatch
			}
			if strings.Contains(logs, "stale connection") {
				return program.Verdict{Result: program.Match, Evidence: []program.Evidence{{Resource: ref, Fact: "a Pod logged a stale connection error", Observed: pod.Name, Expected: "no such error"}}}, nil
			}
		}
		return program.Verdict{Result: program.NoMatch}, nil
	}
	return program.Verdict{Result: program.Unknown}, nil
}

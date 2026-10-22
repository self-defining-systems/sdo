// Package sourceimage demonstrates replayable accepted-source image restoration.
package sourceimage

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sdo.dev/operator/sdk/program"
)

type Program struct{}

func (Program) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "image-pull-failure", Params: []program.ParamSpec{{Name: "deployment", Kind: "Deployment", Required: true}}}
}
func (Program) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	ref := b["deployment"]
	diff, err := v.DiffAgainstSource(ref)
	if err != nil {
		return program.Verdict{}, err
	}
	// NoMatch proves the declared source contradiction cleared, not rollout health.
	if !diff.Changed {
		return program.Verdict{Result: program.NoMatch}, nil
	}
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
				waiting := status.State.Waiting
				if waiting != nil && (waiting.Reason == "ErrImagePull" || waiting.Reason == "ImagePullBackOff") {
					return program.Verdict{Result: program.Match, Evidence: []program.Evidence{{Resource: ref, Fact: "accepted source differs and selected pod image pull fails", Observed: waiting.Reason, Expected: "accepted source"}}}, nil
				}
			}
		}
	}
	return program.Verdict{Result: program.Unknown}, nil
}
func (p Program) Mitigate(ctx context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	verdict, err := p.Diagnose(ctx, v, b)
	if err != nil || verdict.Result != program.Match {
		return program.Plan{}, err
	}
	return program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: b["deployment"]}}}, nil
}

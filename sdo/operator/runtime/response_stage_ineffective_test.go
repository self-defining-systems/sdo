package runtime

import (
	"context"
	"sync/atomic"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// podRestartDetector reports the fixture's image-pull fault with the failing
// Pod as related evidence, as workload detectors do for an OOM-killed Pod. The
// finding's identity is the Deployment, so a replacement Pod keeps it.
type podRestartDetector struct{ pipelineDetector }

func (podRestartDetector) Spec() sdk.DetectorSpec {
	spec := (pipelineDetector{}).Spec()
	spec.Playbooks = append(spec.Playbooks, "pod-restart")
	return spec
}

func (d podRestartDetector) Detect(ctx context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	findings, err := d.pipelineDetector.Detect(ctx, view)
	for i := range findings {
		if findings[i].RuleID != "image-pull" {
			continue
		}
		findings[i].Playbooks = []string{"pod-restart"}
		for _, pod := range view.Pods() {
			if pod.Labels["app"] == findings[i].PrimaryResource.Name {
				findings[i].RelatedResources = append(findings[i].RelatedResources, sdk.ObjectRefFrom("Pod", "v1", &pod))
			}
		}
	}
	return findings, err
}

// podRestartProgram restarts the failing Pod. The restart cannot fix the
// template, so the replacement fails the same way under a new name.
type podRestartProgram struct{}

func (podRestartProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "pod-restart", Params: []program.ParamSpec{{Name: "pod", Kind: "Pod", Required: true}}}
}

func (podRestartProgram) Diagnose(_ context.Context, view program.ReadView, b program.Bindings) (program.Verdict, error) {
	for _, pod := range view.Pods() {
		if pod.Name == b.Objects["pod"].Name && len(pod.Status.ContainerStatuses) > 0 {
			if waiting := pod.Status.ContainerStatuses[0].State.Waiting; waiting != nil && waiting.Reason == "ImagePullBackOff" {
				return program.Verdict{Result: program.Match}, nil
			}
		}
	}
	return program.Verdict{Result: program.NoMatch}, nil
}

func (podRestartProgram) Mitigate(_ context.Context, _ program.ReadView, b program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.DeletePod{Ref: b.Objects["pod"]}}}, nil
}

type countingUnknownProgram struct {
	program.Diagnoser
	program.Mitigator
	calls *atomic.Int32
}

func (p countingUnknownProgram) Spec() program.ProgramSpec { return p.Diagnoser.Spec() }

func (p countingUnknownProgram) Diagnose(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {
	p.calls.Add(1)
	return program.Verdict{Result: program.Unknown}, nil
}


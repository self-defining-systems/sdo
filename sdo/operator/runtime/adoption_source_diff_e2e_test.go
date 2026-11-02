package runtime

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// hostPortConflict is the playbook reflection would author for the slot-17
// ServicePortConflict fault: the live Deployment contradicts accepted source
// while its Pod cannot schedule, and accepted source is the repair.
type hostPortConflict struct{}

func (hostPortConflict) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "service-port-conflict", Params: []program.ParamSpec{{Name: "deployment", Kind: "Deployment", Required: true}}}
}

func (hostPortConflict) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	ref := b.Objects["deployment"]
	diff, err := v.DiffAgainstSource(ref)
	if err != nil {
		return program.Verdict{}, err
	}
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
			for _, condition := range pod.Status.Conditions {
				if condition.Type == corev1.PodScheduled && condition.Status == corev1.ConditionFalse {
					return program.Verdict{Result: program.Match, Evidence: []program.Evidence{{Resource: ref, Fact: "accepted source differs and the selected Pod cannot schedule", Observed: condition.Reason, Expected: "accepted source"}}}, nil
				}
			}
		}
	}
	return program.Verdict{Result: program.Unknown}, nil
}

func (p hostPortConflict) Mitigate(ctx context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	verdict, err := p.Diagnose(ctx, v, b)
	if err != nil || verdict.Result != program.Match {
		return program.Plan{}, err
	}
	return program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: b.Objects["deployment"]}}}, nil
}

// unschedulable is the health detector for this fixture: any Pending Pod.
type unschedulable struct{}

func (unschedulable) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "unschedulable", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge, Interval: time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Watches: []sdk.WatchKind{{APIVersion: "apps/v1", Kind: "Deployment"}}, Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical}, Playbooks: []string{"base"}, OriginatingCommit: "seed"}
}

func (unschedulable) Detect(_ context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	var findings []sdk.Finding
	for _, deployment := range view.Deployments() {
		for _, container := range deployment.Spec.Template.Spec.Containers {
			for _, port := range container.Ports {
				if port.HostPort != 0 {
					ref := sdk.ObjectRefFrom("Deployment", "apps/v1", &deployment)
					findings = append(findings, sdk.Finding{DetectorID: "unschedulable", RuleID: "host-port", Status: sdk.FindingActive, Severity: sdk.SeverityCritical, Summary: "host port blocks scheduling", PrimaryResource: ref})
				}
			}
		}
	}
	return findings, nil
}


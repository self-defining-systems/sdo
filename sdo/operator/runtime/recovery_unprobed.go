package runtime

import (
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/labels"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/traffic"
)

// recoveryUnprobedEvaluation is the pending evaluation that carries this
// reason into the outcome's detector history, as dataAtRiskEvaluation does. It
// is not a detector; the name is the contract reflection's view reads.
const recoveryUnprobedEvaluation = validate.RecoveryUnprobedDetector

// recoveryUnprobed names each Service the incident's findings reach (the
// Service itself, or the Services selecting a named Pod or workload's Pods)
// that no qualified health-probe scenario enters or crosses. A Pod becoming
// Ready shows its process answers its own probe, not that users' requests
// through it succeed again: only a probe crossing the component shows that.
// Health-clear detectors cannot speak for such a Service, so its closure is
// not verified recovery. An application without health probes is outside this
// rule; its closures rest on its detectors as before.
func recoveryUnprobed(view sdk.DetectionContext, findings []sdk.Finding, namespace string) []string {
	windows, ok := view.(traffic.WindowSet)
	if !ok {
		return nil
	}
	probed, crossed := false, map[string]bool{}
	for _, window := range windows.TrafficWindows() {
		if window.Workload.Purpose != traffic.PurposeHealthProbe || window.LoadError != "" {
			continue
		}
		probed = true
		for _, observed := range window.Scenarios {
			if !observed.Qualified {
				continue
			}
			crossed[observed.Scenario.Target.Service] = true
			for _, service := range observed.Scenario.DependsOn {
				crossed[service] = true
			}
		}
	}
	if !probed {
		return nil
	}
	reasons := map[string]string{}
	for _, finding := range findings {
		if strings.HasPrefix(finding.DetectorID, "drift.") {
			continue
		}
		for _, ref := range append([]sdk.ObjectRef{finding.PrimaryResource}, finding.RelatedResources...) {
			if ref.Namespace != "" && ref.Namespace != namespace {
				continue
			}
			for _, service := range servicesReaching(view, ref, namespace) {
				if !crossed[service] && reasons[service] == "" {
					reasons[service] = fmt.Sprintf("Service %s (%s %s) is crossed by no qualified health probe, so only readiness showed it recovered; whether users' requests through it succeed is unverified", service, ref.Kind, ref.Name)
				}
			}
		}
	}
	out := make([]string, 0, len(reasons))
	for _, reason := range reasons {
		out = append(out, reason)
	}
	sort.Strings(out)
	return out
}

// servicesReaching lists the Services of namespace that route to ref: the
// Service itself, those selecting a Pod's labels, or those selecting a
// workload's Pod template labels.
func servicesReaching(view sdk.DetectionContext, ref sdk.ObjectRef, namespace string) []string {
	services := view.Services()
	if ref.Kind == "Service" || ref.Kind == "Endpoints" {
		for _, service := range services {
			if service.Namespace == namespace && service.Name == ref.Name {
				return []string{service.Name}
			}
		}
		return nil
	}
	var podLabels map[string]string
	switch ref.Kind {
	case "Pod":
		for _, pod := range view.Pods() {
			if pod.Namespace == namespace && pod.Name == ref.Name {
				podLabels = pod.Labels
			}
		}
	case "Deployment":
		for _, workload := range view.Deployments() {
			if workload.Namespace == namespace && workload.Name == ref.Name {
				podLabels = workload.Spec.Template.Labels
			}
		}
	case "StatefulSet":
		for _, workload := range view.StatefulSets() {
			if workload.Namespace == namespace && workload.Name == ref.Name {
				podLabels = workload.Spec.Template.Labels
			}
		}
	case "DaemonSet":
		for _, workload := range view.DaemonSets() {
			if workload.Namespace == namespace && workload.Name == ref.Name {
				podLabels = workload.Spec.Template.Labels
			}
		}
	case "ReplicaSet":
		for _, workload := range view.ReplicaSets() {
			if workload.Namespace == namespace && workload.Name == ref.Name {
				podLabels = workload.Spec.Template.Labels
			}
		}
	}
	if len(podLabels) == 0 {
		return nil
	}
	var out []string
	for _, service := range services {
		if service.Namespace == namespace && len(service.Spec.Selector) > 0 && labels.SelectorFromSet(service.Spec.Selector).Matches(labels.Set(podLabels)) {
			out = append(out, service.Name)
		}
	}
	return out
}

// closureRecoveryUnprobed is recoveryUnprobed for a closure about to commit.
func closureRecoveryUnprobed(snapshot sdk.DetectionContext, closure IncidentClosure) string {
	if snapshot == nil || len(closure.FinalDetectorStates) == 0 {
		return ""
	}
	gaps := recoveryUnprobed(snapshot, closure.Request.Findings, closure.Request.Namespace)
	if len(gaps) == 0 {
		return ""
	}
	slog.Warn("closure_recovery_unprobed", "incident_id", closure.Request.IncidentID, "gaps", gaps)
	return strings.Join(gaps, "; ")
}

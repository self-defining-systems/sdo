package runtime

import (
	"cmp"
	"fmt"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/traffic"
)

// corroboratedFailures adds to a traffic detector's findings the user-test
// failures its persistence hold still withholds (9 s partial, 4 s confirmed)
// that a learned playbook already explains. The hold filters failing samples
// that are not faults: data-plane stalls and restart answers after pod-network
// changes. Neither makes a learned playbook's state detector report, so a hard
// failure (an error answer, not slowness) on a path that detector reports on
// is a fault, and the user test opens, or joins the open incident, at once.
//
// Corroborated means: an active finding of an installed learned detector that
// names a learned playbook, on a component of the test's operation path in
// the accepted map, first seen before the test's first failing sample and no
// earlier than AlertJoinWindow before it (a finding active through a healthy
// period explains nothing new), with no new Pod or ReplicaSet and no
// container termination of any component on that path since (restart answers
// anywhere on the path are what the hold is for). Anything else keeps the hold. A failure
// reported early stays reported while it keeps failing, so it never flaps
// back under the hold. The opener is still the failing user test: the startup
// period, the batcher and the join window apply as for any opener.
func (c *Controller) corroboratedFailures(detector sdk.Detector, view sdk.DetectionContext, findings []sdk.Finding) []sdk.Finding {
	workload := traffic.ScenarioSLOWorkload(detector)
	if workload == "" || view == nil {
		return findings
	}
	held := traffic.HeldFailures(detector.Spec(), view, workload)
	if len(held) == 0 {
		return findings
	}
	states := c.tracker.Snapshot()
	corroborating := c.learnedPlaybookFindings(states)
	tests, m := c.openerView()
	pm := m.Program()
	reported := map[string]bool{}
	for _, finding := range findings {
		reported[FindingFingerprint(finding)] = true
	}
	for _, failure := range held {
		if !tests[failure.ScenarioID] {
			continue
		}
		candidate := failure.Finding("")
		fingerprint := FindingFingerprint(candidate)
		if reported[fingerprint] {
			continue
		}
		reason := ""
		if state, ok := states[FindingStateKey(detector.Spec().ID, fingerprint)]; ok && state.Active {
			reason = "still failing since it was reported early"
		} else if failure.Hard && len(corroborating) > 0 {
			reason = corroboration(view, m, pm, failure.ScenarioID, corroborating, failure.FirstFailureAt)
		}
		if reason == "" {
			continue
		}
		findings = append(findings, failure.Finding(reason))
		reported[fingerprint] = true
	}
	return findings
}

// learnedPlaybookFindings are the tracker's active findings of installed
// learned detectors that name a learned playbook.
func (c *Controller) learnedPlaybookFindings(states map[string]FindingState) []FindingState {
	var out []FindingState
	for _, state := range states {
		detector, ok := c.detectors[state.DetectorID]
		if !ok || !state.Active || state.Finding.Status != sdk.FindingActive || !namesLearnedPlaybook(detector.Spec()) {
			continue
		}
		out = append(out, state)
	}
	slices.SortFunc(out, func(a, b FindingState) int {
		return cmp.Or(a.FirstSeenAt.Compare(b.FirstSeenAt), cmp.Compare(a.DetectorID, b.DetectorID))
	})
	return out
}

// namesLearnedPlaybook reports a learned detector that names a playbook other
// than the generic health objective and the built-in source restore.
func namesLearnedPlaybook(spec sdk.DetectorSpec) bool {
	if spec.Class != sdk.DetectorClassLearned {
		return false
	}
	for _, reference := range spec.Playbooks {
		if id, err := layout.PlaybookID(reference); err == nil && id != layout.HealthObjectivePlaybook && id != SourceRestoreID {
			return true
		}
	}
	return false
}

// corroboration names the first finding that corroborates a failure of the
// scenario on its operations' path, or is empty when none does.
func corroboration(view sdk.DetectionContext, m appmap.Map, pm program.AppMap, scenario string, findings []FindingState, firstFailure time.Time) string {
	var operations []string
	for _, probe := range m.Probes {
		if probe.Scenario == scenario {
			operations = append(operations, probe.Operations...)
		}
	}
	names, ok := operationComponents(m, pm, operations)
	if !ok {
		return ""
	}
	var path []program.AppComponent
	for _, name := range names {
		if component, ok := pm.Component(name); ok && !slices.ContainsFunc(path, func(c program.AppComponent) bool { return c.Name == name }) {
			path = append(path, component)
		}
	}
	for _, state := range findings {
		component, ok := pm.ComponentOf(view, state.Finding.PrimaryResource)
		if !ok || !slices.Contains(names, component.Name) || !state.FirstSeenAt.Before(firstFailure) ||
			state.FirstSeenAt.Before(firstFailure.Add(-AlertJoinWindow)) ||
			restartedSince(view, path, state.FirstSeenAt) {
			continue
		}
		return fmt.Sprintf("failing hard while learned detector %s reports component %s on its path (since %s)",
			state.DetectorID, component.Name, state.FirstSeenAt.UTC().Format(time.RFC3339))
	}
	return ""
}

// restartedSince reports a Pod or ReplicaSet of a path component's workload
// created at or after since, or a container of such a Pod last terminated at
// or after since (an in-Pod restart: a liveness or OOM kill makes no new Pod).
// Kubernetes timestamps have one-second resolution, so the same second counts
// as after.
func restartedSince(view sdk.DetectionContext, path []program.AppComponent, since time.Time) bool {
	floor := since.Truncate(time.Second)
	onPath := func(kind, namespace, name string) bool {
		workload := program.WorkloadOf(view, sdk.ObjectRef{Kind: kind, Namespace: namespace, Name: name})
		return slices.ContainsFunc(path, func(c program.AppComponent) bool { return c.Workload == workload })
	}
	for _, pod := range view.Pods() {
		if (!pod.CreationTimestamp.Time.Before(floor) || containerTerminatedSince(pod.Status.ContainerStatuses, floor)) &&
			onPath("Pod", pod.Namespace, pod.Name) {
			return true
		}
	}
	for _, replicaSet := range view.ReplicaSets() {
		if !replicaSet.CreationTimestamp.Time.Before(floor) && onPath("ReplicaSet", replicaSet.Namespace, replicaSet.Name) {
			return true
		}
	}
	return false
}

func containerTerminatedSince(statuses []corev1.ContainerStatus, floor time.Time) bool {
	return slices.ContainsFunc(statuses, func(status corev1.ContainerStatus) bool {
		terminated := status.LastTerminationState.Terminated
		return terminated != nil && !terminated.FinishedAt.Time.Before(floor)
	})
}

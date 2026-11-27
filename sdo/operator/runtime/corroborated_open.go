package runtime

import (
	"fmt"
	"slices"
	"time"

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
// the accepted map, first seen before the test's first failing sample, with
// no rollout or new revision of that component since (a rollout's own restart
// answers are what the hold is for). Anything else keeps the hold. A failure
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
	slices.SortFunc(out, func(a, b FindingState) int { return a.FirstSeenAt.Compare(b.FirstSeenAt) })
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
	components, ok := operationComponents(m, pm, operations)
	if !ok {
		return ""
	}
	for _, state := range findings {
		component, ok := pm.ComponentOf(view, state.Finding.PrimaryResource)
		if !ok || !slices.Contains(components, component.Name) || !state.FirstSeenAt.Before(firstFailure) ||
			rolledOutSince(view, component, state.FirstSeenAt) {
			continue
		}
		return fmt.Sprintf("failing hard while learned detector %s reports component %s on its path (since %s)",
			state.DetectorID, component.Name, state.FirstSeenAt.UTC().Format(time.RFC3339))
	}
	return ""
}

// rolledOutSince reports a Pod or ReplicaSet of the component's workload
// created at or after since. Creation times have one-second resolution, so a
// creation in the same second counts as after.
func rolledOutSince(view sdk.DetectionContext, component program.AppComponent, since time.Time) bool {
	floor := since.Truncate(time.Second)
	for _, pod := range view.Pods() {
		if !pod.CreationTimestamp.Time.Before(floor) &&
			program.WorkloadOf(view, sdk.ObjectRef{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name}) == component.Workload {
			return true
		}
	}
	for _, replicaSet := range view.ReplicaSets() {
		if !replicaSet.CreationTimestamp.Time.Before(floor) &&
			program.WorkloadOf(view, sdk.ObjectRef{Kind: "ReplicaSet", Namespace: replicaSet.Namespace, Name: replicaSet.Name}) == component.Workload {
			return true
		}
	}
	return false
}

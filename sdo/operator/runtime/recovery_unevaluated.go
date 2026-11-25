package runtime

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/traffic"
)

// unevaluatedRecoveryTests are the user tests of the incident's services that
// pass no verdict yet, and how long their schedule needs to give one. A test
// that never succeeded or is known broken proves nothing and is not waited for.
func unevaluatedRecoveryTests(view sdk.DetectionContext, request IncidentRequest) ([]string, time.Duration) {
	set, ok := view.(traffic.WindowSet)
	if !ok {
		return nil, 0
	}
	services := alertServices(view, set.TrafficWindows(), request)
	var waiting []string
	var need time.Duration
	for _, window := range set.TrafficWindows() {
		if window.LoadError != "" {
			continue
		}
		for _, observed := range window.Scenarios {
			id := observed.Scenario.ID
			if !observed.UserTest || !observed.Qualified || window.Workload.KnownBrokenScenario(id) != nil {
				continue
			}
			if len(services) > 0 && !slices.ContainsFunc(scenarioServices(observed.Scenario), func(service string) bool { return services[service] }) {
				continue
			}
			slo := window.Workload.ScenarioSLO(id)
			if traffic.Evaluate(id, slo, observed.Samples, window.ObservedAt).Evaluated {
				continue
			}
			waiting = append(waiting, id)
			need = max(need, slo.MaxAge.Duration(), time.Duration(slo.MinSamples)*window.Workload.ScenarioArrival(id))
		}
	}
	sort.Strings(waiting)
	return waiting, need
}

// alertServices are the Services the alert set is about. Empty when an alert
// cannot be placed: every user test is then the incident's.
func alertServices(view sdk.DetectionContext, windows map[string]traffic.Window, request IncidentRequest) map[string]bool {
	services := map[string]bool{}
	for _, key := range alertKeys(&request) {
		index := slices.IndexFunc(request.Findings, func(f sdk.Finding) bool { return findingKey(f) == key })
		if index < 0 {
			return nil
		}
		var placed []string
		if workload, scenario, ok := openingUserTest(request, key); ok {
			for _, observed := range windows[workload].Scenarios {
				if observed.Scenario.ID == scenario {
					placed = scenarioServices(observed.Scenario)
				}
			}
		} else {
			placed = servicesOf(view, request.Findings[index].PrimaryResource)
		}
		if len(placed) == 0 {
			return nil
		}
		for _, service := range placed {
			services[service] = true
		}
	}
	return services
}

// servicesOf are the Services an object is, or that select its Pods.
func servicesOf(view sdk.DetectionContext, ref sdk.ObjectRef) []string {
	var template map[string]string
	switch ref.Kind {
	case "Service":
		return []string{ref.Name}
	case "Pod":
		if i := slices.IndexFunc(view.Pods(), func(pod corev1.Pod) bool { return pod.Namespace == ref.Namespace && pod.Name == ref.Name }); i >= 0 {
			template = view.Pods()[i].Labels
		}
	case "Deployment":
		for _, d := range view.Deployments() {
			if d.Namespace == ref.Namespace && d.Name == ref.Name {
				template = d.Spec.Template.Labels
			}
		}
	case "StatefulSet":
		for _, s := range view.StatefulSets() {
			if s.Namespace == ref.Namespace && s.Name == ref.Name {
				template = s.Spec.Template.Labels
			}
		}
	}
	var out []string
	for _, service := range view.Services() {
		if len(template) > 0 && service.Namespace == ref.Namespace && len(service.Spec.Selector) > 0 && labels.SelectorFromSet(service.Spec.Selector).Matches(labels.Set(template)) {
			out = append(out, service.Name)
		}
	}
	return out
}

// unevaluatedHoldLocked holds a proved recovery while user tests of the
// incident's services have no verdict yet: from the start of the stability
// window for as long as their schedule needs, and never past the verification
// timeout or any deadline that already governs this recovery. A test that
// never evaluates therefore falls back to the proof alone. "" holds nothing.
func (c *Controller) unevaluatedHoldLocked(now time.Time, waiting []string, need time.Duration) string {
	c.unevaluatedHoldUntil = time.Time{}
	if len(waiting) == 0 || c.closureStableSince.IsZero() {
		return ""
	}
	until := c.closureStableSince.Add(min(need, c.config.VerificationTimeout))
	limit := func(at time.Time) {
		if !at.IsZero() && at.Before(until) {
			until = at
		}
	}
	limit(c.currentIncidentRequest.ResponseDeadline)
	if stage := c.awaitingAutomaticRecoveryLocked(); stage != nil {
		limit(c.automaticDeadlineLocked(stage))
	} else if c.responderDone {
		limit(c.recoveryGraceEndsLocked())
	}
	if !now.Before(until) {
		return ""
	}
	c.unevaluatedHoldUntil = until
	return fmt.Sprintf("alerts are clear; waiting until %s at most for user tests with too few samples: %s", until.UTC().Format(time.RFC3339), strings.Join(waiting, ", "))
}

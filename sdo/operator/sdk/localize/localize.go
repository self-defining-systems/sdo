// Package localize ranks the application components that can explain the
// active synthetic-traffic failures, so incident detectors and playbook
// programs bind and gate a repair on the faulty component instead of on every
// Service a failing journey touches.
//
// It is deterministic and reads only observations the controller already
// provides: the judged traffic window of one workload (scenario paths are the
// scenario's target Service plus its DependsOn list) and, when a detector
// consumes it, the Service reachability window.
//
// Suspects are the components on every failing scenario path (or, when the
// failing paths share none, on any failing path) plus every Service with a
// sustained run of failed dials of its own, even one no scenario lists. A
// healthy scenario never removes a suspect: requests skip dependencies (cache
// hits, empty results, writes that do not exercise the faulty path), so a
// culprit can sit on a healthy path. Scores only order the suspects: 2 for
// being on every failing path, 1 for being on no healthy path, 2 for failed
// dials. Suspects rank evidence; they never prove a cause: a program still
// checks its own evidence on the bound component (an exec, a status, a source
// diff) before it matches.
package localize

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/traffic"
)

// ErrNoTraffic means the traffic window of the workload is not observed.
// Diagnose returns it (Unknown), never NoMatch: the evidence is unavailable.
var ErrNoTraffic = errors.New("localize: synthetic traffic window unavailable")

// Component is one Service on a failing scenario path, or with failed dials,
// and the workloads that back it.
type Component struct {
	Service   string          `json:"service"`
	Workloads []sdk.ObjectRef `json:"workloads,omitempty"`
	// Suspect is true for a component on every failing path or with failed
	// dials of its own; Score orders suspects and never removes one.
	Suspect bool     `json:"suspect"`
	Score   int      `json:"score"`
	Reasons []string `json:"reasons"`
}

// Result is the ranked localization of the current traffic failures.
type Result struct {
	Workload string `json:"workload"`
	// Failing and Healthy are the evaluated scenario IDs by verdict.
	Failing    []string    `json:"failing"`
	Healthy    []string    `json:"healthy"`
	Components []Component `json:"components"`
}

// Inputs declares the observations localization reads. Embed it in an
// incident detector that calls FromContext so the controller sends the
// traffic window and Service reachability to that detector.
type Inputs struct {
	// Workload is the traffic workload to judge, "health" when empty.
	Workload string
}

// TrafficWorkloads implements traffic.Consumer.
func (i Inputs) TrafficWorkloads() []string { return []string{i.workload()} }

// ConsumesServiceReachability implements servicehealth.ReachabilityConsumer.
func (Inputs) ConsumesServiceReachability() bool { return true }

func (i Inputs) workload() string {
	if i.Workload == "" {
		return "health"
	}
	return i.Workload
}

// FromContext localizes the failing scenarios of one traffic workload ("health"
// when empty). It returns ErrNoTraffic when the window is not observed and the
// workload's load error when it could not run. A Result with no Failing
// scenarios has no components.
func FromContext(ctx sdk.DetectionContext, workload string) (Result, error) {
	workload = Inputs{Workload: workload}.workload()
	window, ok := traffic.WindowFrom(ctx, workload)
	if !ok {
		return Result{}, ErrNoTraffic
	}
	if window.LoadError != "" {
		return Result{}, fmt.Errorf("localize: traffic workload %s: %s", workload, window.LoadError)
	}
	reachability, _ := servicehealth.ReachabilityFrom(ctx)
	return Localize(window, reachability, ctx), nil
}

// Localize is FromContext over explicit observations; objects maps Services
// to their backing workloads and may be nil.
func Localize(window traffic.Window, reachability servicehealth.ReachabilityWindow, objects sdk.DetectionContext) Result {
	result := Result{Workload: window.Workload.Name}
	paths := map[string][]string{}
	for _, observed := range window.Scenarios {
		paths[observed.Scenario.ID] = scenarioPath(observed.Scenario)
	}
	for _, verdict := range traffic.Judge(window) {
		if !verdict.Evaluated {
			continue
		}
		if verdict.Healthy {
			result.Healthy = append(result.Healthy, verdict.ScenarioID)
		} else {
			result.Failing = append(result.Failing, verdict.ScenarioID)
		}
	}
	sort.Strings(result.Failing)
	sort.Strings(result.Healthy)
	if len(result.Failing) == 0 {
		return result
	}
	unreachable := unreachableServices(reachability)
	var services []string
	for _, id := range result.Failing {
		for _, service := range paths[id] {
			if !slices.Contains(services, service) {
				services = append(services, service)
			}
		}
	}
	anyOnAll := false
	for _, service := range services {
		anyOnAll = anyOnAll || onEvery(paths, result.Failing, service)
	}
	for service := range unreachable {
		if !slices.Contains(services, service) {
			services = append(services, service)
		}
	}
	for _, service := range services {
		component := Component{Service: service}
		onFailing := false
		for _, id := range result.Failing {
			onFailing = onFailing || slices.Contains(paths[id], service)
		}
		onAll := onEvery(paths, result.Failing, service)
		if onAll {
			component.Score += 2
			component.Reasons = append(component.Reasons, "on every failing scenario path ("+strings.Join(result.Failing, ", ")+")")
		} else if onFailing {
			component.Reasons = append(component.Reasons, "on some failing scenario paths")
		} else {
			component.Reasons = append(component.Reasons, "on no failing scenario path")
		}
		var healthy []string
		for _, other := range result.Healthy {
			if slices.Contains(paths[other], service) {
				healthy = append(healthy, other)
			}
		}
		if len(healthy) == 0 {
			component.Score++
			component.Reasons = append(component.Reasons, "on no healthy scenario path")
		} else {
			component.Reasons = append(component.Reasons, "also on healthy scenario paths ("+strings.Join(healthy, ", ")+")")
		}
		detail, failsDials := unreachable[service]
		if failsDials {
			component.Score += 2
			component.Reasons = append(component.Reasons, detail)
		}
		component.Suspect = onAll || failsDials || onFailing && !anyOnAll
		if objects != nil {
			component.Workloads = backingWorkloads(objects, service)
		}
		result.Components = append(result.Components, component)
	}
	sort.SliceStable(result.Components, func(i, j int) bool {
		a, b := result.Components[i], result.Components[j]
		if a.Suspect != b.Suspect {
			return a.Suspect
		}
		if a.Score != b.Score {
			return a.Score > b.Score
		}
		return a.Service < b.Service
	})
	return result
}

func onEvery(paths map[string][]string, scenarios []string, service string) bool {
	for _, id := range scenarios {
		if !slices.Contains(paths[id], service) {
			return false
		}
	}
	return len(scenarios) > 0
}

// Suspects are the suspect components, highest score first.
func (r Result) Suspects() []Component {
	var suspects []Component
	for _, component := range r.Components {
		if component.Suspect {
			suspects = append(suspects, component)
		}
	}
	return suspects
}

// Implicates reports whether ref (a Service, or a workload backing one) is a
// suspect, with the reasons as evidence text for a Verdict.
func (r Result) Implicates(ref sdk.ObjectRef) (bool, string) {
	for _, component := range r.Suspects() {
		match := ref.Kind == "Service" && ref.Name == component.Service
		for _, workload := range component.Workloads {
			match = match || workload.Kind == ref.Kind && workload.Namespace == ref.Namespace && workload.Name == ref.Name
		}
		if match {
			return true, fmt.Sprintf("Service %s is a suspect of failing scenarios %s: %s", component.Service, strings.Join(r.Failing, ", "), strings.Join(component.Reasons, "; "))
		}
	}
	if len(r.Failing) == 0 {
		return false, "no evaluated synthetic scenario is failing"
	}
	names := make([]string, 0)
	for _, component := range r.Suspects() {
		names = append(names, component.Service)
	}
	return false, fmt.Sprintf("%s/%s is not a suspect of failing scenarios %s (suspects: %s)", ref.Kind, ref.Name, strings.Join(r.Failing, ", "), strings.Join(names, ", "))
}

// BindSuspects returns one copy of finding per suspect workload, with role
// bound to that workload in ParameterBindings and a fingerprint suffixed by
// it, so each suspect is a distinct finding diagnosed on its own. The suffix
// extends the finding's own fingerprint, or the controller's default one when
// the detector set none. It returns nothing when no suspect has a backing
// workload.
func BindSuspects(finding sdk.Finding, r Result, role string) []sdk.Finding {
	var findings []sdk.Finding
	for _, component := range r.Suspects() {
		for _, workload := range component.Workloads {
			bound := finding
			bound.ParameterBindings = map[string]sdk.ObjectRef{}
			for name, ref := range finding.ParameterBindings {
				bound.ParameterBindings[name] = ref
			}
			bound.ParameterBindings[role] = workload
			bound.RelatedResources = append(append([]sdk.ObjectRef(nil), finding.RelatedResources...), workload)
			bound.Evidence = strings.TrimSpace(finding.Evidence + " Localized to " + workload.Kind + "/" + workload.Name + ": " + strings.Join(component.Reasons, "; ") + ".")
			bound.Fingerprint = sdk.FindingFingerprint(finding) + "/" + workload.Kind + "/" + workload.Name
			findings = append(findings, bound)
		}
	}
	return findings
}

// scenarioPath is the target Service followed by DependsOn, without repeats.
func scenarioPath(scenario traffic.Descriptor) []string {
	var path []string
	for _, service := range append([]string{scenario.Target.Service}, scenario.DependsOn...) {
		if service != "" && !slices.Contains(path, service) {
			path = append(path, service)
		}
	}
	return path
}

// unreachableServices are Services whose latest dial samples on some port are
// a run of at least the window's failure threshold.
func unreachableServices(window servicehealth.ReachabilityWindow) map[string]string {
	needed := window.Failures
	if needed <= 0 {
		needed = servicehealth.DefaultReachabilityFailures
	}
	result := map[string]string{}
	for _, target := range window.Targets {
		run := 0
		for index := len(target.Samples) - 1; index >= 0 && !target.Samples[index].OK; index-- {
			run++
		}
		if run >= needed {
			last := target.Samples[len(target.Samples)-1]
			result[target.Target.Service] = fmt.Sprintf("its port %d failed the last %d dials (%s)", target.Target.Port, run, last.Class)
		}
	}
	return result
}

// backingWorkloads are the Deployments and StatefulSets whose Pod template
// labels contain the Service's selector.
func backingWorkloads(objects sdk.DetectionContext, name string) []sdk.ObjectRef {
	var selector map[string]string
	namespace := ""
	for _, service := range objects.Services() {
		if service.Name == name {
			selector, namespace = service.Spec.Selector, service.Namespace
			break
		}
	}
	if len(selector) == 0 {
		return nil
	}
	contains := func(labels map[string]string) bool {
		for key, value := range selector {
			if labels[key] != value {
				return false
			}
		}
		return true
	}
	var workloads []sdk.ObjectRef
	for _, deployment := range objects.Deployments() {
		if deployment.Namespace == namespace && contains(deployment.Spec.Template.Labels) {
			workloads = append(workloads, sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: namespace, Name: deployment.Name})
		}
	}
	for _, statefulSet := range objects.StatefulSets() {
		if statefulSet.Namespace == namespace && contains(statefulSet.Spec.Template.Labels) {
			workloads = append(workloads, sdk.ObjectRef{APIVersion: "apps/v1", Kind: "StatefulSet", Namespace: namespace, Name: statefulSet.Name})
		}
	}
	return workloads
}

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
// A component scores 2 when it is on every failing scenario path, 1 more when
// it is on no healthy evaluated scenario path, and 2 more when its own Service
// has a sustained run of failed dials. Suspects are the components with the
// highest score. Scores rank evidence; they never prove a cause: a program
// still checks its own evidence on the bound component (an exec, a status, a
// source diff) before it matches.
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

// Component is one Service on a failing scenario path and the workloads that
// back it.
type Component struct {
	Service   string          `json:"service"`
	Workloads []sdk.ObjectRef `json:"workloads,omitempty"`
	Score     int             `json:"score"`
	Reasons   []string        `json:"reasons"`
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
	seen := map[string]bool{}
	for _, id := range result.Failing {
		for _, service := range paths[id] {
			if seen[service] {
				continue
			}
			seen[service] = true
			component := Component{Service: service}
			onAll := true
			for _, other := range result.Failing {
				onAll = onAll && slices.Contains(paths[other], service)
			}
			if onAll {
				component.Score += 2
				component.Reasons = append(component.Reasons, "on every failing scenario path ("+strings.Join(result.Failing, ", ")+")")
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
			if detail, ok := unreachable[service]; ok {
				component.Score += 2
				component.Reasons = append(component.Reasons, detail)
			}
			if objects != nil {
				component.Workloads = backingWorkloads(objects, service)
			}
			result.Components = append(result.Components, component)
		}
	}
	sort.SliceStable(result.Components, func(i, j int) bool {
		if result.Components[i].Score != result.Components[j].Score {
			return result.Components[i].Score > result.Components[j].Score
		}
		return result.Components[i].Service < result.Components[j].Service
	})
	return result
}

// Suspects are the highest-scoring components.
func (r Result) Suspects() []Component {
	var suspects []Component
	for _, component := range r.Components {
		if len(suspects) > 0 && component.Score < suspects[0].Score {
			break
		}
		suspects = append(suspects, component)
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
// it, so each suspect is diagnosed on its own. It returns nothing when no
// suspect has a backing workload.
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
			if finding.Fingerprint != "" {
				bound.Fingerprint = finding.Fingerprint + "/" + workload.Kind + "/" + workload.Name
			}
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

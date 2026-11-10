package runtime

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// workloads lists the holds_data workloads in a stable order.
func (h dataHolders) workloads() []string {
	workloads := make([]string, 0, len(h))
	for workload := range h {
		workloads = append(workloads, workload)
	}
	sort.Strings(workloads)
	return workloads
}

// named returns the holds_data component with this map name.
func (h dataHolders) named(name string) appmap.Component {
	for _, component := range h {
		if component.Name == name {
			return component
		}
	}
	return appmap.Component{}
}

// dataEffects is what SDO can see of one action's reach into the components
// the application map marks holds_data.
type dataEffects struct {
	verb    string
	touches []program.Touch
	// unreadInput is an input the action uses that cannot be read here, so it
	// may reach any holds_data component.
	unreadInput bool
}

// dataReach classifies actions against one view of the cluster.
type dataReach struct {
	view      sdk.DetectionContext
	holders   dataHolders
	namespace string
}

// effects assigns an external action its data effect on each holds_data
// component it reaches. SDO computes this from what it can see, and nothing an
// author writes lowers it. An opaque command, script or Job reaches the
// component it runs in and every one its inputs name (a Service, workload or
// Pod, by object or by name in its text), and its effect there is unknown. A
// typed database user change is a store setting on its target. Every other
// action has no external data effect.
func (r dataReach) effects(action program.Action) dataEffects {
	result := dataEffects{}
	reached := map[string]bool{}
	effect := program.EffectUnknown
	object := func(ref sdk.ObjectRef) {
		if component := r.component(ref); component != "" {
			reached[component] = true
		}
	}
	text := func(text string) {
		for _, component := range r.named(text) {
			reached[component] = true
		}
	}
	value := func(value program.Value) {
		if value.Origin() != program.ParameterValue {
			object(value.Ref())
		}
		resolved, _, err := program.ResolveValue(value, program.ValueInputs{})
		if err != nil {
			result.unreadInput = true
			return
		}
		text(fmt.Sprint(resolved))
	}
	switch a := actionValue(action).(type) {
	case program.Exec:
		result.verb = "exec"
		object(a.Pod)
		for _, word := range a.Command {
			value(word)
		}
	case program.EnsureExec:
		result.verb = "exec"
		for _, command := range []program.Command{a.Observe, a.Ensure} {
			object(command.Pod)
			text(strings.Join(command.Args, " "))
			text(command.Stdin)
		}
	case program.EnsureHTTP:
		result.verb = "http request"
		for _, request := range []program.HTTPRequest{a.Observe, a.Ensure} {
			object(request.Pod)
			text(request.URL)
			text(request.Body)
			for _, header := range request.Headers {
				text(header)
			}
		}
	case program.RunScript:
		result.verb = "script"
		text(a.Script)
		for _, input := range a.Env {
			value(input)
		}
	case program.RunJob:
		result.verb = "job"
		// A round trip turns typed Go values into the plain JSON shape.
		var manifest any
		encoded, err := json.Marshal(a.Object.Object)
		if err == nil {
			err = json.Unmarshal(encoded, &manifest)
		}
		if err != nil || len(a.Object.Object) == 0 {
			result.unreadInput = true
		}
		for _, word := range manifestStrings(manifest) {
			text(word)
		}
	case program.EnsureDatabaseUser:
		result.verb = "database user change"
		effect = program.EffectStoreSetting
		object(a.Target.Pod)
	default:
		return dataEffects{}
	}
	for _, workload := range r.holders.workloads() {
		if name := r.holders[workload].Name; reached[name] {
			result.touches = append(result.touches, program.Touch{Component: name, Effect: effect})
		}
	}
	return result
}

// component names the holds_data component that ref is, or belongs to: one of
// its Services, its workload, or one of that workload's Pods or ReplicaSets.
func (r dataReach) component(ref sdk.ObjectRef) string {
	if ref.Name == "" || (ref.Namespace != "" && ref.Namespace != r.namespace) {
		return ""
	}
	ref.Namespace = r.namespace
	if ref.Kind == "Service" {
		for _, workload := range r.holders.workloads() {
			if slices.Contains(r.holders[workload].Services, ref.Name) {
				return r.holders[workload].Name
			}
		}
		return ""
	}
	return r.holders[owningWorkload(r.view, ref)].Name
}

// named lists the holds_data components whose Services, workload or Pods the
// text names as a whole DNS name, as in a host, a URL or a kubectl argument.
func (r dataReach) named(text string) []string {
	if text == "" {
		return nil
	}
	var components []string
	for _, workload := range r.holders.workloads() {
		holder := r.holders[workload]
		_, name, _ := strings.Cut(workload, "/")
		candidates := append([]string{name}, holder.Services...)
		for _, pod := range r.view.Pods() {
			if pod.Namespace == r.namespace && owningWorkload(r.view, sdk.ObjectRef{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name}) == workload {
				candidates = append(candidates, pod.Name)
			}
		}
		if slices.ContainsFunc(candidates, func(candidate string) bool { return mentionsName(text, candidate) }) {
			components = append(components, holder.Name)
		}
	}
	return components
}

// mentionsName holds when name appears in text with no DNS label character
// (a lowercase letter, digit or '-') on either side.
func mentionsName(text, name string) bool {
	if name == "" {
		return false
	}
	label := func(b byte) bool { return b >= 'a' && b <= 'z' || b >= '0' && b <= '9' || b == '-' }
	for start := 0; start < len(text); {
		index := strings.Index(text[start:], name)
		if index < 0 {
			return false
		}
		index += start
		end := index + len(name)
		if (index == 0 || !label(text[index-1])) && (end == len(text) || !label(text[end])) {
			return true
		}
		start = index + 1
	}
	return false
}

// manifestStrings lists the string values of a manifest (its arguments,
// environment, hosts), not its field names.
func manifestStrings(value any) []string {
	switch typed := value.(type) {
	case string:
		return []string{typed}
	case map[string]any:
		var words []string
		for _, item := range typed {
			words = append(words, manifestStrings(item)...)
		}
		return words
	case []any:
		var words []string
		for _, item := range typed {
			words = append(words, manifestStrings(item)...)
		}
		return words
	}
	return nil
}

// mayChangeRecords is an effect that can change a store's records.
func mayChangeRecords(effect program.DataEffect) bool {
	return effect == program.EffectDataWrite || effect == program.EffectUnknown
}

// unbackedDataWrites names the holds_data workloads whose records an applied
// action may have changed with no backup taken first. The SDK has no backup
// action yet, so every such write is unbacked. Health probes see only the data
// they wrote, so a repair like this cannot be shown to have kept the rest. The
// responder stays free to make it; the closure just cannot feed learning. Only
// inputs SDO can still see count here: a recorded plan keeps its objects but not
// the text of its bound values.
func unbackedDataWrites(view sdk.DetectionContext, applications []RecordedPlanApplication, holders dataHolders, namespace string) []string {
	if len(holders) == 0 || view == nil {
		return nil
	}
	reach := dataReach{view: view, holders: holders, namespace: namespace}
	risks := map[string]string{}
	for _, application := range applications {
		for _, action := range application.Result.ActionsApplied {
			effects := reach.effects(action)
			for _, touch := range effects.touches {
				workload := holders.named(touch.Component).Workload
				if mayChangeRecords(touch.Effect) && risks[workload] == "" {
					risks[workload] = fmt.Sprintf("%s holds data, and a %s reached it during the incident with no backup taken first (data effect %s); health probes cannot show whether its records survived", workload, effects.verb, touch.Effect)
				}
			}
		}
	}
	result := make([]string, 0, len(risks))
	for _, risk := range risks {
		result = append(result, risk)
	}
	sort.Strings(result)
	return result
}

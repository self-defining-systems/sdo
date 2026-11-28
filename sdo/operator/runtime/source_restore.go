package runtime

import (
	"context"
	"fmt"
	"sync"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// SourceRestoreID is the built-in playbook that puts a component's departures
// from accepted source back. It is part of SDO, not of operational memory: it
// has no repository source, is never learned, edited or credited, and no
// prompt or skill says when to use it.
const SourceRestoreID = "source-restore"

// SourceRestoreComponentParam names the component whose objects the playbook restores.
const SourceRestoreComponentParam = "component"

// sourceRestoreEvidence is the incident evidence the playbook's Diagnose
// classifies with: what the loop knows, set whenever it takes a snapshot.
type sourceRestoreEvidence struct {
	mu      sync.Mutex
	request IncidentRequest
}

func (e *sourceRestoreEvidence) set(request IncidentRequest) {
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.request = request
}

// automaticRestoreKey marks the context of the automatic response stage.
type automaticRestoreKey struct{}

// forAutomaticRestore makes the built-in playbook count only departures not
// present while the application was last healthy: such a change did not break
// it, and restoring it would restart a component for nothing.
func forAutomaticRestore(ctx context.Context) context.Context {
	return context.WithValue(ctx, automaticRestoreKey{}, true)
}

// departures is the shared classifier over the component's objects, restricted
// to what a restore can reach (a Secret is never restored from here) and, for
// the automatic stage, to what was not seen healthy.
func (e *sourceRestoreEvidence) departures(ctx context.Context, view program.ReadView, componentName string) ([]SourceDeparture, error) {
	automatic := ctx.Value(automaticRestoreKey{}) != nil
	e.mu.Lock()
	request := e.request
	e.mu.Unlock()
	m, err := view.AppMap()
	if err != nil {
		return nil, err
	}
	component, ok := m.Component(componentName)
	if !ok || component.Workload == "" {
		return nil, fmt.Errorf("component %q is not in the application map", componentName)
	}
	namespace := request.Namespace
	if namespace == "" {
		namespace = view.Namespace()
	}
	set := buildSuspectSet(detectInput{Request: request, View: view})
	var restorable []SourceDeparture
	for _, departure := range SourceDepartures(departureInput(request, view, set.onset), componentObjects(view, component, namespace)) {
		if departure.Command() != "" && !(automatic && departure.SeenHealthy) {
			restorable = append(restorable, departure)
		}
	}
	return restorable, nil
}

type sourceRestoreProgram struct{ evidence *sourceRestoreEvidence }

func sourceRestoreSpec() program.ProgramSpec {
	return program.ProgramSpec{ID: SourceRestoreID, Params: []program.ParamSpec{{Name: SourceRestoreComponentParam, ValueKind: program.StringValue, Required: true}}}
}

func (sourceRestoreProgram) Spec() program.ProgramSpec { return sourceRestoreSpec() }

func restoreComponent(bindings program.Bindings) string {
	name, _ := bindings.Values[SourceRestoreComponentParam].(string)
	return name
}

// Diagnose matches when the component has a departure from accepted source made
// by a manager other than SDO since the last deployment (SourceDepartures).
func (p sourceRestoreProgram) Diagnose(ctx context.Context, view program.ReadView, bindings program.Bindings) (program.Verdict, error) {
	departures, err := p.evidence.departures(ctx, view, restoreComponent(bindings))
	if err != nil {
		return program.Verdict{Result: program.Unknown, Evidence: []program.Evidence{{Fact: "source comparison", Observed: err.Error(), Expected: "readable"}}}, nil
	}
	if len(departures) == 0 {
		return program.Verdict{Result: program.NoMatch}, nil
	}
	verdict := program.Verdict{Result: program.Match}
	for _, departure := range departures {
		verdict.Evidence = append(verdict.Evidence, program.Evidence{Resource: departure.Ref, Fact: "departs from accepted source", Observed: departure.Text(), Expected: "as accepted source declares it"})
	}
	return verdict, nil
}

// Mitigate restores each departed object with the built-in source actions:
// ReconcileFromSource, CreateFromSource, or DeleteObject for a source-absent
// object. Then it restarts, with RestartRollout, each workload that reads a
// restored ConfigMap only at container start and runs a Pod that loaded the
// departed content, because the restore alone does not change what that Pod
// runs. A data store's workload is never restarted here (it is not a
// Consumer): restarting it is the responder's call, so it never makes this
// playbook match.
func (p sourceRestoreProgram) Mitigate(ctx context.Context, view program.ReadView, bindings program.Bindings) (program.Plan, error) {
	departures, err := p.evidence.departures(ctx, view, restoreComponent(bindings))
	if err != nil {
		return program.Plan{}, err
	}
	plan := program.Plan{Rationale: "put objects changed outside SDO back to accepted source"}
	var restarts []program.Action
	restarting := map[string]bool{}
	for _, departure := range departures {
		reason := "restore to accepted source"
		switch departure.Kind {
		case DepartureChanged:
			plan.Actions = append(plan.Actions, program.ReconcileFromSource{Ref: departure.Ref})
		case DepartureMissing:
			plan.Actions = append(plan.Actions, program.CreateFromSource{Ref: departure.Ref, Reason: reason})
		case DepartureExtra:
			plan.Actions = append(plan.Actions, program.DeleteObject{Ref: departure.Ref, Reason: "remove an object accepted source does not declare"})
		}
		for _, consumer := range departure.Consumers {
			key := consumer.Kind + "/" + consumer.Name
			if restarting[key] {
				continue
			}
			restarting[key] = true
			restarts = append(restarts, program.RestartRollout{Ref: consumer, Reason: "load " + departure.Ref.Kind + "/" + departure.Ref.Name + " as accepted source declares it; it is read only at container start"})
		}
	}
	plan.Actions = append(plan.Actions, restarts...)
	if len(plan.Actions) == 0 {
		return program.Plan{}, fmt.Errorf("component %q has no departure from accepted source to restore", restoreComponent(bindings))
	}
	return plan, nil
}

// holdsData reports whether workload ("Kind/name") is a data store's in the map.
func holdsData(m program.AppMap, workload string) bool {
	for _, component := range m.Components {
		if component.Workload == workload {
			return component.HoldsData || component.Data != nil
		}
	}
	return false
}

// withSourceRestore adds the built-in playbook to a registry. It lists beside
// learned playbooks and is diagnosed, planned and applied the same way.
type sourceRestoreRegistry struct {
	program.ProgramRegistry
	restore sourceRestoreProgram
}

func withSourceRestore(inner program.ProgramRegistry, evidence *sourceRestoreEvidence) program.ProgramRegistry {
	return sourceRestoreRegistry{ProgramRegistry: inner, restore: sourceRestoreProgram{evidence: evidence}}
}

func (r sourceRestoreRegistry) List() []program.ProgramSpec {
	var specs []program.ProgramSpec
	if r.ProgramRegistry != nil {
		specs = r.ProgramRegistry.List()
	}
	return append(specs, sourceRestoreSpec())
}

func (r sourceRestoreRegistry) Diagnoser(id string) (program.Diagnoser, bool) {
	if id == SourceRestoreID {
		return r.restore, true
	}
	if r.ProgramRegistry == nil {
		return nil, false
	}
	return r.ProgramRegistry.Diagnoser(id)
}

func (r sourceRestoreRegistry) Mitigator(id string) (program.Mitigator, bool) {
	if id == SourceRestoreID {
		return r.restore, true
	}
	if r.ProgramRegistry == nil {
		return nil, false
	}
	return r.ProgramRegistry.Mitigator(id)
}

// suspectNode reports whether node is a suspect: a mapped component on a
// failing path, or the opening condition's own. A passing path through it does
// not exclude it.
func suspectNode(node *candidateNode) bool {
	onFailing := node.Class == PositionFailingOnly || node.Class == PositionShared
	return (node.Condition || onFailing) && node.Component.Workload != ""
}

// suspects reports whether a resource of view belongs to a suspect's workload.
func (set detectSuspectSet) suspects(view sdk.DetectionContext) func(sdk.ObjectRef) bool {
	workloads := map[string]bool{}
	for _, node := range set.nodes {
		if suspectNode(node) {
			workloads[node.Component.Workload] = true
		}
	}
	return func(ref sdk.ObjectRef) bool {
		// A namesake in another namespace is no component of this application.
		if ref.Namespace != "" && view.Namespace() != "" && ref.Namespace != view.Namespace() {
			return false
		}
		return workloads[program.WorkloadOf(view, ref)]
	}
}

// restoreCandidates binds the built-in playbook to each suspect component, for
// the opening findings. Diagnose matches only on a non-SDO departure from
// accepted source made before the onset, and that departure is the evidence;
// being a suspect only keeps components that cannot affect a failing operation
// out. Nothing here decides to apply it.
func (set detectSuspectSet) restoreCandidates() []sourceRestoreCandidate {
	var result []sourceRestoreCandidate
	for rank, node := range set.nodes {
		if !suspectNode(node) {
			continue
		}
		bindings := program.Bindings{Values: map[string]any{SourceRestoreComponentParam: node.Name}}
		for _, finding := range set.openingFindings {
			result = append(result, sourceRestoreCandidate{bindings: bindings, fingerprint: FindingFingerprint(finding), rank: rank + 1})
		}
	}
	return result
}

type sourceRestoreCandidate struct {
	bindings    program.Bindings
	fingerprint string
	rank        int
}

package runtime

import (
	"context"
	"fmt"
	"sync"
	"time"

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

// departures is the shared classifier over the component's objects, restricted
// to what a restore can reach (a Secret is never restored from here).
func (e *sourceRestoreEvidence) departures(view program.ReadView, componentName string) ([]SourceDeparture, error) {
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
		if departure.Command() != "" {
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
func (p sourceRestoreProgram) Diagnose(_ context.Context, view program.ReadView, bindings program.Bindings) (program.Verdict, error) {
	departures, err := p.evidence.departures(view, restoreComponent(bindings))
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
// ReconcileFromSource, CreateFromSource, or DeleteObject for a source-absent object.
func (p sourceRestoreProgram) Mitigate(_ context.Context, view program.ReadView, bindings program.Bindings) (program.Plan, error) {
	departures, err := p.evidence.departures(view, restoreComponent(bindings))
	if err != nil {
		return program.Plan{}, err
	}
	plan := program.Plan{Rationale: "put objects changed outside SDO back to accepted source"}
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
	}
	if len(plan.Actions) == 0 {
		return program.Plan{}, fmt.Errorf("component %q has no departure from accepted source to restore", restoreComponent(bindings))
	}
	return plan, nil
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

// sourceRestoreCandidates binds the playbook to each suspect on every failing
// path that is not also on a passing path, for the opening findings. Nothing
// here decides to apply it.
func sourceRestoreCandidates(view program.ReadView, request IncidentRequest, findings []sdk.Finding) []sourceRestoreCandidate {
	set := buildSuspectSet(detectInput{Request: request, View: view, Findings: findings, Now: time.Time{}})
	var result []sourceRestoreCandidate
	for rank, node := range set.nodes {
		// On every failing path (or the opening condition's own component) and
		// on no passing one.
		onEvery := node.Condition || node.OfN > 0 && node.OnK == node.OfN
		if node.Class != PositionFailingOnly || !onEvery || len(node.Serves) > 0 || node.Component.Workload == "" {
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

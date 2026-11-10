package validate

// The map-resolved form of a derived-cache refresh: the program takes the
// failing store Pod by role, finds its component and the caches rebuilt from
// it in the application map, confirms an accepted data check fires on it,
// and resolves the cache's store target from the map. It names no component.

const mapDiagnose = `// Package derivedrefresh refreshes the caches derived from a damaged store.
package derivedrefresh

import (
	"context"

	"sdo.dev/operator/sdk/datacheck"
	"sdo.dev/operator/sdk/program"
)

type Program struct{}

func (Program) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "derived-refresh", Params: []program.ParamSpec{{Name: "store", Kind: "Pod", Required: true}}}
}

func (Program) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	store := b.Objects["store"]
	m, err := v.AppMap()
	if err != nil {
		return program.Verdict{}, err
	}
	component, ok := m.ComponentOf(v, store)
	if !ok || len(m.Derived(component.Name)) == 0 {
		return program.Verdict{Result: program.NoMatch}, nil
	}
	for _, verdict := range datacheck.Verdicts(v) {
		if verdict.Firing() && verdict.Lookup == component.Name {
			return program.Verdict{Result: program.Match, CauseClass: "data-corrupt", Reach: program.ReachInScope,
				Evidence: []program.Evidence{{Resource: store, Fact: "a derived copy differs from its source", Observed: verdict.ID}}}, nil
		}
	}
	return program.Verdict{Result: program.NoMatch}, nil
}
`

const mapMitigate = `package derivedrefresh

import (
	"context"

	"sdo.dev/operator/sdk/program"
)

func (Program) Mitigate(_ context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	m, err := v.AppMap()
	if err != nil {
		return program.Plan{}, err
	}
	component, _ := m.ComponentOf(v, b.Objects["store"])
	var actions []program.Action
	for _, derived := range m.Derived(component.Name) {
		target, err := program.StoreTargetFor(v, derived.Name)
		if err != nil {
			return program.Plan{}, err
		}
		actions = append(actions, program.InvalidateDerived{Target: target, Scope: program.DataScope{Selector: "*"}, Reason: "refill copies from the repaired source"})
	}
	return program.Plan{Actions: actions, Rationale: "drop the derived copies the map names"}, nil
}
`

func init() {
	gateRows = append(gateRows,
		gateComponentLiteral("new playbook resolves its stores from the application map", false, []string{"derived-refresh"},
			writePlaybook("derived-refresh", mapDiagnose, mapMitigate)))
}

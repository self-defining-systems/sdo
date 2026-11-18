package onboardcheck

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"sdo.dev/operator/memory/appmap"
)

// ItemRoutesAssigned is red while the map has no route list, and per listed
// route that is neither a journey's route nor excluded with a reason. It
// accepts no reason: exclude the route instead.
const ItemRoutesAssigned = "routes-assigned"

// RouteLister runs the map's route-list command in the component's container
// and returns its output lines.
type RouteLister interface {
	List(ctx context.Context, component appmap.Component, list appmap.RouteList) ([]string, error)
}

func judgeRoutes(ctx context.Context, cfg Config, report *Report) {
	m := cfg.Map
	if m.Routes == nil || m.Routes.List == nil || len(m.Routes.List.Command) == 0 {
		report.add(ItemRoutesAssigned, "routes", Red, "the map has no route list: add routes.list (component, command) whose output is one \"METHOD route\" per line, so every route of the application is a journey or excluded with a reason")
		return
	}
	for _, x := range m.Routes.Excluded {
		if strings.TrimSpace(x.Why) == "" {
			report.add(ItemRoutesAssigned, x.Route, Red, "excluded route has no reason")
		}
	}
	var comp *appmap.Component
	for i := range m.Components {
		if m.Components[i].Name == m.Routes.List.Component {
			comp = &m.Components[i]
		}
	}
	if comp == nil {
		report.add(ItemRoutesAssigned, "routes", Red, fmt.Sprintf("routes.list names component %q, which is not declared", m.Routes.List.Component))
		return
	}
	if cfg.Routes == nil {
		report.add(ItemRoutesAssigned, "routes", Red, "the route list was not run: no way to execute it from here")
		return
	}
	lines, err := cfg.Routes.List(ctx, *comp, *m.Routes.List)
	if err != nil {
		report.add(ItemRoutesAssigned, "routes", Red, "the route-list command failed: "+err.Error())
		return
	}
	var listed, missing []string
	seen := map[string]bool{}
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l == "" || seen[l] {
			continue
		}
		seen[l] = true
		listed = append(listed, l)
		if !m.Assigned(l) {
			missing = append(missing, l)
		}
	}
	sort.Strings(missing)
	if len(listed) == 0 {
		report.add(ItemRoutesAssigned, "routes", Red, "the route-list command printed no routes")
		return
	}
	for _, r := range missing {
		report.add(ItemRoutesAssigned, r, Red, "listed route that no journey covers and nothing excludes: add a journey for it or exclude it with a reason")
	}
	if len(missing) == 0 {
		report.add(ItemRoutesAssigned, "routes", Green, fmt.Sprintf("all %d listed routes are a journey's route or excluded", len(listed)))
	}
}

// ItemGivenJourneys is red for a human-given journey no operation serves and
// for an operation naming a journey goal.md does not list. Each operation of a
// given journey is held to the same checklist as any other.
const ItemGivenJourneys = "given-journeys"

func judgeGivenJourneys(cfg Config, report *Report) {
	if len(cfg.Journeys) == 0 {
		return
	}
	known := map[string]bool{}
	served := map[string][]string{}
	for _, g := range cfg.Journeys {
		known[g.ID] = true
	}
	for _, op := range cfg.Map.Operations {
		switch {
		case op.Journey == "":
		case !known[op.Journey]:
			report.add(ItemGivenJourneys, op.ID, Red, fmt.Sprintf("names journey %q, which goal.md does not list: use a listed journey id, or drop the field", op.Journey))
		default:
			served[op.Journey] = append(served[op.Journey], op.ID)
		}
	}
	for _, g := range cfg.Journeys {
		if len(served[g.ID]) == 0 {
			report.add(ItemGivenJourneys, g.ID, Red, fmt.Sprintf("no operation serves it (%s): map the operations that do it with journey: %s", g.What, g.ID))
			continue
		}
		report.add(ItemGivenJourneys, g.ID, Green, "served by "+strings.Join(served[g.ID], ", "))
	}
}

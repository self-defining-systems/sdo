package validate

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/sdk/datacheck"
)

// dataCheckRules checks every data check against its schema, the read-only
// allowlist and bounds of its engine (datacheck.Validate), and the application
// map: each store it reads is a map component with a data role of the same
// engine; an equivalence compares a derived component with a component it is
// derived from.
func (s *run) dataCheckRules() {
	if len(s.dataChecks) == 0 {
		return
	}
	components, mapErr := s.mapDataComponents()
	ids := make([]string, 0, len(s.dataChecks))
	for id := range s.dataChecks {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		file := s.dataChecks[id]
		check, err := datacheck.Parse(file.content)
		if err == nil {
			err = check.Validate(id)
		}
		if err != nil {
			for _, problem := range strings.Split(err.Error(), "\n") {
				s.add(Semantic, Diagnostic{Rule: RuleDataCheckInvalid, Path: file.path, Line: errorLine(err, 0), Message: problem,
					Hint: "a data check names one store, a sampled read (ids, range or first) and assertions: shape, bounds, reference or equivalence"})
			}
			continue
		}
		if mapErr != "" {
			s.add(Semantic, Diagnostic{Rule: RuleDataCheckComponent, Path: file.path, Message: "data checks name application-map components, but " + mapErr,
				Hint: "write the application map with a data entry for every store"})
			continue
		}
		for _, problem := range dataCheckComponentProblems(check, components) {
			s.add(Semantic, Diagnostic{Rule: RuleDataCheckComponent, Path: file.path, Message: problem,
				Hint: "a data check reads stores the application map declares with data: {engine, role}; an equivalence compares a derived store with a store it is derived_from"})
		}
	}
}

// mapDataComponents returns the map's components by name, or why there are
// none to resolve against.
func (s *run) mapDataComponents() (map[string]appmap.Component, string) {
	if s.arch == nil {
		return nil, "arch.md is missing"
	}
	region, found, err := appmap.Find(s.arch.doc.Source)
	if !found || err != nil {
		return nil, "arch.md has no application map"
	}
	parsed, err := appmap.Parse(region.YAML)
	if err != nil {
		return nil, "the application map does not parse"
	}
	out := map[string]appmap.Component{}
	for _, c := range parsed.Components {
		out[c.Name] = c
	}
	return out, ""
}

func dataCheckComponentProblems(check datacheck.Check, components map[string]appmap.Component) []string {
	var problems []string
	store := func(what string, s datacheck.Store) (appmap.Component, bool) {
		c, ok := components[s.Component]
		switch {
		case !ok:
			problems = append(problems, fmt.Sprintf("%s component %s is not in the application map", what, s.Component))
		case c.Data == nil:
			problems = append(problems, fmt.Sprintf("%s component %s has no data entry in the application map", what, s.Component))
		case c.Data.Engine != string(s.Engine):
			problems = append(problems, fmt.Sprintf("%s component %s is a %s store in the application map, not %s", what, s.Component, c.Data.Engine, s.Engine))
		default:
			return c, true
		}
		return c, false
	}
	subject, subjectOK := store("checked", check.Store)
	if check.Lookup == nil {
		return problems
	}
	lookup, lookupOK := store("lookup", *check.Lookup)
	equivalence := slices.ContainsFunc(check.Assert, func(a datacheck.Assertion) bool { return a.Kind() == datacheck.KindEquivalence })
	if equivalence && subjectOK && lookupOK && (subject.Data.Role != appmap.RoleDerived || !slices.Contains(subject.Data.DerivedFrom, lookup.Name)) {
		problems = append(problems, fmt.Sprintf("equivalence compares a derived copy with its source: %s must be role derived with %s in its derived_from", subject.Name, lookup.Name))
	}
	return problems
}

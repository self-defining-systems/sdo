package appmap

// WithProbeMembership adds each map probe's scenario to the probe workload of
// its kind when none of the three probe workloads lists it: the probe line
// states the kind once, and a workload file lists a scenario only to give it
// per-scenario settings (knownBroken, an SLO). A scenario listed in a probe
// workload is left where it is, so one listed in the wrong workload stays an
// error. A missing workload gets nothing; the probe check reports it.
func (m Map) WithProbeMembership(workloads []Workload) []Workload {
	listed := map[string]bool{}
	index := map[string]int{}
	out := make([]Workload, len(workloads))
	for i, w := range workloads {
		w.Scenarios = append([]string(nil), w.Scenarios...)
		out[i] = w
		for kind, name := range WorkloadForKind {
			if w.Name == name {
				index[kind] = i
				for _, id := range w.Scenarios {
					listed[id] = true
				}
			}
		}
	}
	for _, p := range m.Probes {
		i, ok := index[p.Kind]
		if !ok || listed[p.Scenario] {
			continue
		}
		listed[p.Scenario] = true
		out[i].Scenarios = append(out[i].Scenarios, p.Scenario)
	}
	return out
}

// ProbePaths maps each probe's scenario to the Services of every component on
// its operations' paths, one list per component in path order: Go derives a
// scenario's empty DependsOn from it (traffic.CompleteCatalog), so the author
// states a probe's reach once, in the map.
func (m Map) ProbePaths() map[string][][]string {
	components := map[string]Component{}
	for _, c := range m.Components {
		components[c.Name] = c
	}
	ops := opPaths(m.Operations)
	out := map[string][][]string{}
	for _, p := range m.Probes {
		seen := map[string]bool{}
		var lists [][]string
		for _, id := range p.Operations {
			for _, name := range ops[id].Path {
				services := components[name].Services
				if seen[name] || len(services) == 0 {
					continue
				}
				seen[name] = true
				lists = append(lists, append([]string(nil), services...))
			}
		}
		if len(lists) > 0 {
			out[p.Scenario] = lists
		}
	}
	return out
}

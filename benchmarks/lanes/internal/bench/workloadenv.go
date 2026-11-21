package bench

import (
	"fmt"
	"sort"
	"strings"
)

// judgeWorkloadEnv checks a host's workload_env against the workloads of the specs that run on it. A key is the
// spec's `workload` string (Host.EnvFor), so a key named after the adapter instead of that string matches no spec and
// the adapter then runs without its settings. A key that no spec uses, beside a workload that has no key, is that
// mistake; a workload served from the plain host env while every key is used is fine.
func judgeWorkloadEnv(h Host, workloads []string) (bool, string) {
	if len(h.WorkloadEnv) == 0 {
		return true, "no workload_env"
	}
	used := map[string]bool{}
	var missing []string
	for _, w := range workloads {
		id := w
		if id == "" {
			id = DefaultWorkloadID
		}
		if _, ok := h.WorkloadEnv[id]; ok {
			used[id] = true
		} else {
			missing = append(missing, id)
		}
	}
	var unused []string
	for k := range h.WorkloadEnv {
		if !used[k] {
			unused = append(unused, k)
		}
	}
	sort.Strings(unused)
	sort.Strings(missing)
	if len(unused) == 0 || len(missing) == 0 {
		return true, "every workload_env key belongs to a workload of the specs"
	}
	return false, fmt.Sprintf("workload_env keys %s match no spec; workloads %s have no key (a key is the spec's `workload` string)",
		strings.Join(quoteAll(unused), ", "), strings.Join(quoteAll(missing), ", "))
}

func quoteAll(in []string) []string {
	out := make([]string, len(in))
	for i, s := range in {
		out[i] = fmt.Sprintf("%q", s)
	}
	return out
}

package bench

import (
	"fmt"
	"sort"
	"strings"
)

// Lane labels and spec selectors: which specs a lane may take. A host labels its lanes (`lane_labels: {lane: {k: v}}`);
// a spec may carry a `selector: {k: v}`. A lane takes a spec when every selector pair is among its labels. A lane with
// labels is not taken by a spec with no selector, unless one of its labels is `shared: "true"`. Unlabeled lanes take
// any spec that has no selector, as they always did. Lanes knows nothing about what a label means.

// SharedLabel marks a labeled lane that specs without a selector may take.
const SharedLabel = "shared"

// LaneAccepts says whether the lane (cluster) of host may take a build of s.
func (inv Inventory) LaneAccepts(host, cluster string, s Spec) bool {
	labels := inv.Hosts[host].LaneLabels[cluster]
	if len(s.Selector) == 0 {
		return len(labels) == 0 || labels[SharedLabel] == "true"
	}
	for k, v := range s.Selector {
		if labels[k] != v {
			return false
		}
	}
	return true
}

// laneLabelProblems are the errors of a host's lane_labels: each names a lane the host lists.
func (h Host) laneLabelProblems() []string {
	var out []string
	for lane := range h.LaneLabels {
		found := false
		for _, c := range h.Clusters {
			found = found || c == lane
		}
		if !found {
			out = append(out, fmt.Sprintf("lane_labels names %q, which is not in clusters", lane))
		}
	}
	sort.Strings(out)
	return out
}

// labelsText is "k=v,k=v", sorted, for messages.
func labelsText(m map[string]string) string {
	var kv []string
	for k, v := range m {
		kv = append(kv, k+"="+v)
	}
	sort.Strings(kv)
	return strings.Join(kv, ",")
}

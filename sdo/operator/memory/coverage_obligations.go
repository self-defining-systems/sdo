package memory

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
)

// CoverageObligations renders, for the detection designer's brief, the mapped
// edges from entry components that no probed operation crosses and that record
// no unprobed reason. It is empty when the checkout has no parsable map or
// every such edge is covered; the memory check reports the same edges as
// map.uncovered-edge.
func CoverageObligations(checkout string) string {
	data, err := os.ReadFile(filepath.Join(checkout, layout.ArchitectureFile))
	if err != nil {
		return ""
	}
	region, found, err := appmap.Find(string(data))
	if !found || err != nil {
		return ""
	}
	m, err := appmap.Parse(region.YAML)
	if err != nil {
		return ""
	}
	edges := m.UncoveredEdges()
	if len(edges) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("# Probe coverage (Go check of the accepted map, not part of the architecture file)\n\n")
	b.WriteString("These mapped edges from entry components lie on no probed operation path, so a fault on them stays invisible while pods are Ready:\n")
	for _, e := range edges {
		fmt.Fprintf(&b, "- %s -> %s (%s)\n", e.From, e.To, e.Source)
	}
	b.WriteString("\nCover each with an operation whose path crosses it and a probe that exercises it. Record `unprobed: <why>` on an edge only when no synthetic request can cross it.\n")
	return b.String()
}

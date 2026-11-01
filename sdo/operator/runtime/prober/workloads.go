package prober

import (
	"fmt"
	"sort"

	"sdo.dev/operator/sdk/traffic"
)

// ParseWorkloads decodes workload documents (JSON) and rejects duplicates.
func ParseWorkloads(documents map[string][]byte) ([]traffic.Workload, error) {
	workloads := make([]traffic.Workload, 0, len(documents))
	names := make([]string, 0, len(documents))
	for name := range documents {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		document := documents[name]
		workload, err := traffic.ParseWorkloadJSON(document)
		if err != nil {
			return nil, fmt.Errorf("workload %s: %w", name, err)
		}
		if workload.Name != name {
			return nil, fmt.Errorf("workload file %s declares name %q", name, workload.Name)
		}
		workloads = append(workloads, workload)
	}
	return workloads, nil
}

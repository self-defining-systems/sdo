//go:build integration

package bench

import (
	"context"
	"os"
	"strings"
	"testing"
)

// The capacity the plan alarms read has only hosts that have lanes. A host in hosts.yaml with no clusters (drained,
// kept for later) never runs anything of ours, so its load or disk is no pressure on the plan and raises no alarm.
func TestPlanCapacityLeavesOutHostsWithoutLanes(t *testing.T) {
	l := newLab(t)
	b, err := os.ReadFile(l.root.path("hosts.yaml"))
	must(t, err)
	drained := "  drained: {ssh: nowhere, isolation: one-way-ok, staging: /tmp/drained, max_builds: 1, max_load5: 1, kubeconfigs: /tmp/drained/kube, evidence: /tmp/drained/ev, clusters: []}\n"
	must(t, os.WriteFile(l.root.path("hosts.yaml"), []byte(strings.Replace(string(b), "checkouts:", drained+"checkouts:", 1)), 0o644))
	must(t, writeJSON(l.root.path("hosts.status.json"), map[string]HostStatus{
		"h1": {Load5: 50}, "drained": {Load5: 900},
	}))
	c, err := (&PlanBackend{Root: l.root}).Capacity(context.Background())
	must(t, err)
	var names []string
	for _, h := range c.Hosts {
		names = append(names, h.Name)
	}
	if len(names) != 1 || names[0] != "h1" {
		t.Fatalf("capacity hosts %v, want only h1 (drained has no lanes)", names)
	}
}

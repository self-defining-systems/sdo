//go:build integration

package bench

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A kind host with no cluster_up never makes a cluster: its lanes must already exist (their kubeconfig is there). A spec
// that would land on a lane with no cluster is refused at add with a message that says what to set, not later at
// recover with "context kind-<lane> does not exist". Behavior is unchanged for hosts whose clusters exist and for hosts
// that have cluster_up.

// adoptClusters gives a fixture host the clusters it lists: a kind host with no cluster_up adopts clusters that exist,
// shown by their kubeconfig files. Fixtures that model a valid host call it with the host's kubeconfigs directory.
func adoptClusters(t *testing.T, kubeconfigs string, clusters ...string) {
	t.Helper()
	must(t, os.MkdirAll(kubeconfigs, 0o755))
	for _, c := range clusters {
		must(t, os.WriteFile(filepath.Join(kubeconfigs, c), []byte("apiVersion: v1\n"), 0o644))
	}
}

func clusterSourceInv(t *testing.T, kube string, extra string) Inventory {
	t.Helper()
	inv, err := loadInv(t, `hosts:
  h:
    local: true
    staging: /s
    kubeconfigs: `+kube+`
    evidence: /e
    max_builds: 1
    max_load5: 10
    clusters: [c1, c2]
`+extra+`checkouts:
  fuzzops: {abcdef0: {h: /fz}}
`)
	must(t, err)
	return inv
}

func TestHostWithoutClusterUpFailsEarlyForALaneWithoutACluster(t *testing.T) {
	kube := t.TempDir()
	must(t, os.WriteFile(filepath.Join(kube, "c1"), []byte("apiVersion: v1\n"), 0o644)) // c1 exists, c2 does not
	inv := clusterSourceInv(t, kube, "")
	s := Spec{Fuzzops: "abcdef0", Hosts: []string{"h"}}
	got := inv.ClusterSourceProblems(context.Background(), s, localRunner)
	want := "host h has no cluster_up and lane c2 has no cluster: set cluster_up (e.g. $LANES_INSTALL/deploy/kind-lane.sh up)"
	if len(got) != 1 || got[0] != want {
		t.Fatalf("problems = %q, want [%q]", got, want)
	}
	must(t, os.WriteFile(filepath.Join(kube, "c2"), []byte("apiVersion: v1\n"), 0o644))
	if got := inv.ClusterSourceProblems(context.Background(), s, localRunner); len(got) != 0 {
		t.Fatalf("every lane has its cluster (the adopt case): %q", got)
	}
}

func TestHostWithClusterUpCreatesItsClustersSoNothingIsMissing(t *testing.T) {
	inv := clusterSourceInv(t, t.TempDir(), "    cluster_up: 'up {cluster} {kubeconfig} {evidence}'\n    cluster_down: 'down {cluster}'\n")
	if got := inv.ClusterSourceProblems(context.Background(), Spec{Fuzzops: "abcdef0", Hosts: []string{"h"}}, localRunner); len(got) != 0 {
		t.Fatalf("a host with cluster_up makes them: %q", got)
	}
}

func TestOnlyLanesTheSpecMayTakeAreChecked(t *testing.T) {
	kube := t.TempDir()
	inv, err := loadInv(t, `hosts:
  h:
    local: true
    staging: /s
    kubeconfigs: `+kube+`
    evidence: /e
    max_builds: 1
    max_load5: 10
    clusters: [c1, other]
    lane_labels: {other: {workload: x}}
checkouts:
  fuzzops: {abcdef0: {h: /fz}}
`)
	must(t, err)
	must(t, os.WriteFile(filepath.Join(kube, "c1"), []byte("x"), 0o644))
	got := inv.ClusterSourceProblems(context.Background(), Spec{Fuzzops: "abcdef0", Hosts: []string{"h"}}, localRunner)
	if len(got) != 0 {
		t.Fatalf("lane other is labeled for another workload; it is not this spec's to check: %q", got)
	}
	if !strings.Contains(strings.Join(inv.ClusterSourceProblems(context.Background(), Spec{Fuzzops: "abcdef0", Hosts: []string{"h"}, Selector: map[string]string{"workload": "x"}}, localRunner), ""), "lane other") {
		t.Fatal("the lane the selector reaches is checked")
	}
}

// add and host status use the check: a spec for a host whose lane has no cluster is refused by ValidateRendered.
func TestValidateRenderedRefusesALaneWithoutACluster(t *testing.T) {
	inv := clusterSourceInv(t, t.TempDir(), "")
	old := addHostRunner
	addHostRunner = func(Inventory) runFn { return localRunner }
	t.Cleanup(func() { addHostRunner = old })
	s := Spec{Name: "e", App: "a", Fuzzops: "abcdef0", Namespace: "n", Hosts: []string{"h"}, Faults: Faults{IDs: []string{"healthy"}}, Sequences: [][]int{{1}},
		ReadyFor: "1s", ReadyTimeout: "1s", Cap: "1s", Lanes: 1, Operator: Operator{Builtin: "noop"}}
	err := ValidateRendered(context.Background(), inv, s, NewRenderedChecks(nil))
	if err == nil || !strings.Contains(err.Error(), "host h has no cluster_up and lane c1 has no cluster") {
		t.Fatalf("want the early refusal, got %v", err)
	}
}

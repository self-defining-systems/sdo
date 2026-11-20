//go:build integration

package bench

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestCPUListRoundTrip(t *testing.T) {
	for in, want := range map[string][]int{
		"4-7":         {4, 5, 6, 7},
		"0-3,8,10-11": {0, 1, 2, 3, 8, 10, 11},
		"5":           {5},
		" 2-3 , 9 ":   {2, 3, 9},
	} {
		got, err := ParseCPUList(in)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Errorf("%q: %v %v", in, got, err)
		}
	}
	if got := FormatCPUList([]int{10, 4, 5, 6, 8, 11}); got != "4-6,8,10-11" {
		t.Errorf("format: %q", got)
	}
	for _, bad := range []string{"", "a", "3-1", "1-", "-2", "1,,2", "4-5-6"} {
		if _, err := ParseCPUList(bad); err == nil {
			t.Errorf("%q parsed", bad)
		}
	}
}

func TestParseSizeAcceptsBinarySuffixes(t *testing.T) {
	for in, want := range map[string]int64{"20Gi": 20 << 30, "512Mi": 512 << 20, "1Ti": 1 << 40, "3GiB": 3 << 30} {
		if got, err := ParseSize(in); err != nil || got != want {
			t.Errorf("%q: %d %v", in, got, err)
		}
	}
}

const laneHosts = `
hosts:
  h1:
    local: true
    staging: /s
    max_builds: 1
    max_load5: 40
    kubeconfigs: /k
    clusters: [c1, c2]
    evidence: /e
    lanes: {cpus: "4-55", reserve: true, memory_min: 8G}
`

func loadLanesInv(t *testing.T, body string) (Inventory, error) {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "hosts.yaml"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return LoadInventory(root)
}

func TestHostLanesConfig(t *testing.T) {
	inv, err := loadLanesInv(t, laneHosts)
	if err != nil {
		t.Fatal(err)
	}
	l := inv.Hosts["h1"].Lanes
	if l.CPUs != "4-55" || !l.Reserve || l.MemoryMin != "8G" || !l.Enabled() {
		t.Fatalf("%+v", l)
	}
	if cpus, _ := l.CPUList(); len(cpus) != 52 || cpus[0] != 4 {
		t.Fatalf("cpu list %v", cpus)
	}
	// without a lanes block a host is as before
	plain := strings.Replace(laneHosts, `    lanes: {cpus: "4-55", reserve: true, memory_min: 8G}`+"\n", "", 1)
	inv, err = loadLanesInv(t, plain)
	if err != nil || inv.Hosts["h1"].Lanes.Enabled() {
		t.Fatalf("plain host: %v %+v", err, inv.Hosts["h1"].Lanes)
	}
}

func TestLaneLimitsOfAStep(t *testing.T) {
	inv, err := loadLanesInv(t, laneHosts)
	if err != nil {
		t.Fatal(err)
	}
	h := inv.Hosts["h1"]
	var spec Spec
	spec.App = "examples/shop"
	got := laneLimits(h, spec, "")
	if got == nil || got.Cores != 5 || got.Memory != "12Gi" || got.CPUs != "4-55" || !got.Reserve || got.MemoryMin != "8G" {
		t.Fatalf("a spec without lane gets its app's default: %+v", got)
	}
	if got.State != "" || got.StateShared {
		t.Fatalf("no state configured: the host's step picks its user's default, not the manager (which may be another account): %+v", got)
	}
	spec.Lane = &LaneLimit{CPUs: 6, Memory: "14Gi"}
	if got := laneLimits(h, spec, ""); got.Cores != 6 || got.Memory != "14Gi" {
		t.Fatalf("the spec's lane wins: %+v", got)
	}
	h.Lanes.State, h.Lanes.StateShared, h.Lanes.Memory = "/srv/lanes", true, "150G"
	if got := laneLimits(h, spec, ""); got.State != "/srv/lanes" || !got.StateShared || got.MemBudget != "150G" {
		t.Fatalf("%+v", got)
	}
	h.Lanes = HostLanes{}
	if laneLimits(h, spec, "") != nil {
		t.Fatal("a host without lanes: limits nothing")
	}
	h.Lanes = HostLanes{CPUs: "4-55"}
	h.Provider, h.EntryHost, h.HostKubeconfig = ProviderVcluster, "x", "/k"
	if laneLimits(h, spec, "") != nil {
		t.Fatal("virtual clusters are not kind nodes: nothing to limit")
	}
}

func TestHostLanesValidation(t *testing.T) {
	for name, tc := range map[string]struct{ from, to, want string }{
		"bad cpus":           {`cpus: "4-55"`, `cpus: "55-4"`, "lanes.cpus"},
		"reserve needs cpus": {`{cpus: "4-55", reserve: true, memory_min: 8G}`, `{reserve: true}`, "reserve needs lanes.cpus"},
		"bad memory_min":     {`memory_min: 8G`, `memory_min: lots`, "memory_min"},
		"shared needs state": {`{cpus: "4-55", reserve: true, memory_min: 8G}`, `{cpus: "4-55", state_shared: true}`, "state_shared needs lanes.state"},
	} {
		_, err := loadLanesInv(t, strings.Replace(laneHosts, tc.from, tc.to, 1))
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestSpecLaneBlock(t *testing.T) {
	s, err := LoadSpec(writeSpec(t, t.TempDir(), exampleSpec+"lane: {cpus: 6, memory: 14Gi}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if got := s.LaneLimit(); got != (LaneLimit{CPUs: 6, Memory: "14Gi"}) {
		t.Fatalf("%+v", got)
	}
	for _, bad := range []string{"lane: {cpus: 0, memory: 8G}", "lane: {cpus: 2, memory: lots}", "lane: {cpus: 2}"} {
		if _, err := LoadSpec(writeSpec(t, t.TempDir(), exampleSpec+bad+"\n")); err == nil || !strings.Contains(err.Error(), "lane") {
			t.Errorf("%q: %v", bad, err)
		}
	}
}

// A spec without lane: still loads and gets the workload's footprint for its app, else the platform default.
func TestSpecWithoutLaneGetsTheWorkloadsFootprintOrTheDefault(t *testing.T) {
	s, err := LoadSpec(writeSpec(t, t.TempDir(), exampleSpec))
	if err != nil {
		t.Fatal(err)
	}
	if got := s.LaneLimit(); got != DefaultLaneLimit() || got.CPUs < 4 || got.Memory == "" {
		t.Fatalf("%+v", got)
	}
	// The built-in workload knows its big app: a lane with an operator beside it was OOM-killed at 20Gi.
	big := Spec{App: "examples/gitlab-big"}
	if g := big.LaneLimit(); g.CPUs != 8 {
		t.Fatalf("big app default %+v", g)
	} else if n, err := ParseSize(g.Memory); err != nil || n < 30<<30 {
		t.Fatalf("big app default memory %s (%v): below the 30Gi a lane with an operator needs", g.Memory, err)
	}
	// Lanes itself reads no app names: an adapter executable's lane is the default unless it answers `footprint`.
	other := Spec{App: "examples/gitlab-big", Workload: "./adapter-x"}
	if g := other.LaneLimit(); g != DefaultLaneLimit() {
		t.Fatalf("an app name must not size a lane outside the built-in workload: %+v", g)
	}
	other.Lane = &LaneLimit{CPUs: 3, Memory: "7Gi"}
	if g := other.LaneLimit(); g.CPUs != 3 {
		t.Fatalf("the spec's own lane wins: %+v", g)
	}
}

// An adapter executable that implements `footprint` sizes its lanes: lanes asks at add and records the answer.
func TestFootprintVerbSizesTheLanesOfAnAdapterWorkload(t *testing.T) {
	f := newFixture(t)
	inv, err := LoadInventory(string(f.root))
	must(t, err)
	adapter := filepath.Join(f.fuzzops, "adapter.sh")
	must(t, os.WriteFile(adapter, []byte("#!/bin/sh\ncase $WORKLOAD_VERB in footprint) echo '{\"cpus\":3,\"memory\":\"7Gi\"}' ;; *) exit 64 ;; esac\n"), 0o755))
	s := Spec{Name: "e", App: "examples/shop", Fuzzops: "0b528ac", Hosts: []string{"h1"}, Workload: "./adapter.sh"}
	got, err := NewRenderedChecks(nil).footprint(context.Background(), inv, s)
	must(t, err)
	if got == nil || got.CPUs != 3 || got.Memory != "7Gi" {
		t.Fatalf("footprint = %+v", got)
	}
	other := s
	other.Workload = "./nothing.sh" // answers nothing, exit 127
	if got, err := NewRenderedChecks(nil).footprint(context.Background(), inv, other); err != nil || got != nil {
		t.Fatalf("a workload with no footprint verb sizes nothing: %+v %v", got, err)
	}
}

// The lane block is infrastructure, not part of what a column is measured by.
func TestLaneLimitIsNotIdentity(t *testing.T) {
	a, _ := LoadSpec(writeSpec(t, t.TempDir(), exampleSpec))
	b, _ := LoadSpec(writeSpec(t, t.TempDir(), exampleSpec+"lane: {cpus: 6, memory: 14Gi}\n"))
	if d := a.IdentityDiff(Lock{Spec: b}); len(d) != 0 {
		t.Fatal(d)
	}
}

//go:build integration

package bench

import (
	"strings"
	"testing"
)

// Lanes carry labels and specs a selector. A spec lands only on a lane whose labels satisfy its selector, and a spec
// with no selector takes a labeled lane only when the lane says it is shared. Unlabeled lanes behave as before.

const labeledHost = `hosts:
  h:
    local: true
    staging: /s
    kubeconfigs: /k
    evidence: /e
    max_builds: 2
    max_load5: 10
    clusters: [plain, sg, open, both]
    lane_labels:
      sg: {workload: sregym}
      open: {workload: sregym, shared: "true"}
      both: {workload: gitlab, pool: a}
checkouts:
  fuzzops: {abc: {h: /fz}}
`

func TestLaneAcceptsASpecOnlyWhenItsLabelsSatisfyTheSelector(t *testing.T) {
	inv, err := loadInv(t, labeledHost)
	must(t, err)
	none := Spec{}
	sregym := Spec{Selector: map[string]string{"workload": "sregym"}}
	gitlab := Spec{Selector: map[string]string{"workload": "gitlab", "pool": "a"}}
	for _, tc := range []struct {
		lane string
		spec Spec
		want bool
	}{
		{"plain", none, true},    // no labels, no selector: as before
		{"plain", sregym, false}, // a selector needs labels to match
		{"sg", none, false},      // a labeled lane is not taken by a spec that did not ask for it
		{"sg", sregym, true},
		{"sg", gitlab, false},
		{"open", none, true}, // shared: any spec may take it
		{"open", sregym, true},
		{"open", gitlab, false},
		{"both", gitlab, true},
		{"both", Spec{Selector: map[string]string{"workload": "gitlab"}}, true}, // a selector may name some of the labels
		{"both", sregym, false},
	} {
		if got := inv.LaneAccepts("h", tc.lane, tc.spec); got != tc.want {
			t.Errorf("lane %s, selector %v: accepts = %v, want %v", tc.lane, tc.spec.Selector, got, tc.want)
		}
	}
}

func TestLaneLabelsNameLanesOfTheHost(t *testing.T) {
	_, err := loadInv(t, strings.Replace(labeledHost, "sg: {workload: sregym}", "ghost: {workload: sregym}", 1))
	if err == nil || !strings.Contains(err.Error(), "ghost") {
		t.Fatalf("a label on a lane the host does not list must be refused, got %v", err)
	}
}

// Placement honors it: a free labeled lane is not offered to a spec that did not ask for it, the matching one is.
func TestFreeLaneIsOfferedOnlyToSpecsThatMatchIt(t *testing.T) {
	inv, err := loadInv(t, labeledHost)
	must(t, err)
	m := &Manager{inv: inv, lanes: map[string]*LaneFile{
		"sg":    {Lane: "sg", Host: "h", State: LaneFree},
		"plain": {Lane: "plain", Host: "h", State: LaneFree},
	}}
	if l := m.freeLaneOn("h", false, Spec{}); l == nil || l.Lane != "plain" {
		t.Fatalf("a spec with no selector must skip the labeled lane, got %+v", l)
	}
	if l := m.freeLaneOn("h", false, Spec{Selector: map[string]string{"workload": "sregym"}}); l == nil || l.Lane != "sg" {
		t.Fatalf("the sregym spec must get its lane, got %+v", l)
	}
	delete(m.lanes, "plain")
	if l := m.freeLaneOn("h", false, Spec{}); l != nil {
		t.Fatalf("nothing but a labeled lane is free: a spec without selector gets none, got %+v", l)
	}
}

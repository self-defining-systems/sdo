package plan

import (
	"reflect"
	"strings"
	"testing"
)

const twopass = `
plan: tp
pins: {workload: fuzzops@4652656, subject: sdo@9b2cadb7f}
arms:
  - {name: p1, spec: tpl-plain.yaml}
  - {name: cold, spec: tpl-plain.yaml}
  - {name: p2, spec: tpl.yaml}
items: [a, b]
reps: 2
parallel: 2
artifacts: {memory: {path: "${run}/operator/learned-baseline", verify: subject}}
edges: [{from: p1, to: p2, artifact: memory}]
`

func TestParseRejectsBadPlans(t *testing.T) {
	cases := map[string]string{
		"no arms":     "plan: tp\npins: {workload: f@1234567, subject: s@1234567}\nitems: [a]\n",
		"bad pin":     strings.Replace(twopass, "fuzzops@4652656", "fuzzops", 1),
		"bad order":   twopass + "order: random\n",
		"cycle":       twopass + "edges2: x\n",
		"edge arm":    strings.Replace(twopass, "to: p2", "to: p9", 1),
		"no artifact": strings.Replace(twopass, "artifact: memory", "artifact: nope", 1),
		"bad verify":  strings.Replace(twopass, "artifact: memory}", "artifact: memory, verify: maybe}", 1),
	}
	cases["cycle"] = strings.Replace(twopass, "edges: [{from: p1, to: p2, artifact: memory}]",
		"edges: [{from: p1, to: p2}, {from: p2, to: p1}]", 1)
	for name, text := range cases {
		if _, err := Parse([]byte(text)); err == nil {
			t.Errorf("%s: plan accepted", name)
		}
	}
	if _, err := Parse([]byte(twopass)); err != nil {
		t.Fatalf("good plan refused: %v", err)
	}
}

func TestExpandInterleavesArmsPerItemAndOrdersRepMajor(t *testing.T) {
	p, err := Parse([]byte(twopass))
	if err != nil {
		t.Fatal(err)
	}
	items := []Item{{ID: "a"}, {ID: "b"}}
	nodes, err := p.Expand(items)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, n := range nodes {
		got = append(got, n.ID)
	}
	want := []string{
		"p1/a/r1", "cold/a/r1", "p2/a/r1", "p1/b/r1", "cold/b/r1", "p2/b/r1",
		"p1/a/r2", "cold/a/r2", "p2/a/r2", "p1/b/r2", "cold/b/r2", "p2/b/r2",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rep-major order\n got %v\nwant %v", got, want)
	}
	p.Order = ItemMajor
	nodes, _ = p.Expand(items)
	if nodes[0].ID != "p1/a/r1" || nodes[3].ID != "p1/a/r2" {
		t.Fatalf("item-major order: %v", nodes[:4])
	}
}

func TestArmOrderPutsSourcesBeforeDependentsNotByName(t *testing.T) {
	// "a-late" is declared first but takes from "z-src": the source goes first; unrelated arms keep declaration order.
	p, err := Parse([]byte(`
plan: tp
pins: {workload: f@1234567, subject: s@1234567}
arms: [{name: a-late, spec: t}, {name: z-src, spec: t}, {name: m-free, spec: t}]
items: [i]
edges: [{from: z-src, to: a-late}]
`))
	if err != nil {
		t.Fatal(err)
	}
	order, _ := p.armOrder()
	if want := []string{"z-src", "a-late", "m-free"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("arm order %v want %v", order, want)
	}
}

func TestExpNameFitsTheManagersLimitAndIsUnique(t *testing.T) {
	seen := map[string]string{}
	for _, id := range []string{"sdo.p1/gmem/r1", "sdo-p1/gmem/r1", "sdo.p1/a-very-long-family-name-that-overflows/r1", "sdo.p1/a-very-long-family-name-that-overflows/r2"} {
		for _, attempt := range []int{1, 2, 3} {
			n := ExpName("twopass-g", id, attempt)
			if len(n) > 40 || !nameRE40.MatchString(n) {
				t.Errorf("%s attempt %d: bad name %q", id, attempt, n)
			}
			if prev, ok := seen[n]; ok {
				t.Errorf("%q names both %s and %s", n, prev, id)
			}
			seen[n] = id
		}
	}
	if got := ExpName("tp", "p1/a/r1", 1); got != "tp-p1-a-r1" {
		t.Errorf("plain name = %q", got)
	}
	if got := ExpName("tp", "p1/a/r1", 2); got != "tp-p1-a-r1-x1" {
		t.Errorf("retry suffix = %q", got)
	}
}

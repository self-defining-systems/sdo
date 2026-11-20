package plan

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

var errBadCPUs = errors.New("lane.cpus is not a number")

const grouped = `
plan: tp
pins: {workload: fuzzops@4652656, subject: sdo@9b2cadb7f}
arms: [{name: a, spec: tpl-plain.yaml}]
groups:
  big:   {spec: {namespace: ns1, lane: {cpus: 8, memory: 30Gi}, hosts: [h1, h2]}}
  small: {vars: {flavor: s}, spec: {namespace: ns2, lane: {cpus: 4, memory: 12Gi}, hosts: [h3]}}
items:
  - {id: g1, group: big}
  - {id: s1, group: small, spec: {lane: {cpus: 2}}}
  - {id: s2, group: small, spec: {claim: "pool-${item.flavor}", hosts: null}}
`

func TestItemAndGroupSpecOverridesAreTypedAndMerged(t *testing.T) {
	l := newLab(t)
	e := l.add(grouped)
	l.tick(e)
	specs := map[string]map[string]any{}
	for _, j := range l.fb.subs {
		var s map[string]any
		if err := json.Unmarshal(mustRead(t, j.SpecPath), &s); err != nil {
			t.Fatal(err)
		}
		specs[j.Node] = s
	}
	g1, s1, s2 := specs["a/g1/r1"], specs["a/s1/r1"], specs["a/s2/r1"]
	if g1 == nil || s1 == nil || s2 == nil {
		t.Fatalf("not all items released: %v", l.fb.nodes())
	}
	if !reflect.DeepEqual(g1["lane"], map[string]any{"cpus": 8.0, "memory": "30Gi"}) || !reflect.DeepEqual(g1["hosts"], []any{"h1", "h2"}) {
		t.Errorf("group spec not typed in g1: %v", g1)
	}
	if !reflect.DeepEqual(s1["lane"], map[string]any{"cpus": 2.0, "memory": "12Gi"}) || s1["namespace"] != "ns2" {
		t.Errorf("item override must merge over its group: %v", s1)
	}
	if _, has := s2["hosts"]; has || s2["claim"] != "pool-s" {
		t.Errorf("null removes a field and group vars reach ${item.<var>}: %v", s2)
	}
}

func TestSpecOverridesRefuseEngineOwnedFieldsAndUnknownGroups(t *testing.T) {
	for name, edit := range map[string][2]string{
		"owned":   {"namespace: ns1", "fuzzops: deadbeef1"},
		"retries": {"namespace: ns1", "retries: 3"},
		"group":   {"group: big}", "group: nope}"},
	} {
		p := strings.Replace(grouped, edit[0], edit[1], 1)
		l := newLab(t)
		l.write("tpl-plain.yaml", `{"app": "x"}`)
		file := l.write("plan.yaml", p)
		if _, err := Add(l.root+"/plans", file, l.clock, nil); err == nil {
			t.Errorf("%s: plan accepted", name)
		}
	}
}

// A wrong type in an override is caught by the platform's spec validation at add time, for every item.
func TestEveryItemsSpecIsValidatedAtAdd(t *testing.T) {
	l := newLab(t)
	l.write("tpl-plain.yaml", `{"app": "x"}`)
	file := l.write("plan.yaml", strings.Replace(grouped, "cpus: 2", "cpus: two", 1))
	var seen []string
	validate := func(b []byte) error {
		var s struct {
			Lane struct {
				CPUs any `json:"cpus"`
			} `json:"lane"`
		}
		_ = json.Unmarshal(b, &s)
		if _, ok := s.Lane.CPUs.(float64); !ok {
			seen = append(seen, "bad")
			return errBadCPUs
		}
		return nil
	}
	if _, err := Add(l.root+"/plans", file, l.clock, validate); err == nil {
		t.Fatalf("a string in lane.cpus of one item passed (validated: %v)", seen)
	}
}

package plan

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestDryRunExpandsAndValidatesWithoutTouchingState(t *testing.T) {
	l := newLab(t)
	l.write("tpl.yaml", `{"name": "x", "app": "examples/${item}", "operator": {"hooks": "h", "seed": "${artifact.memory}"}, "faults": {"instances": "inst.json", "ids": ["${item}"]}}`)
	l.write("tpl-plain.yaml", `{"name": "x", "app": "examples/${item}", "operator": {"hooks": "h"}, "faults": {"ids": ["${item}"]}}`)
	file := l.write("plan.yaml", twopass)
	plans := filepath.Join(l.root, "plans")

	var validated int
	pv, err := DryRun(plans, file, l.clock, func([]byte) error { validated++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(plans); !os.IsNotExist(err) {
		t.Fatalf("dry run touched the plans root: %v", err)
	}
	if validated == 0 {
		t.Error("dry run did not validate any spec")
	}
	if pv.Plan != "tp" || len(pv.Nodes) != 12 || !reflect.DeepEqual(pv.Items, []string{"a", "b"}) {
		t.Fatalf("preview: %+v", pv)
	}
	if want := []string{"p1", "cold", "p2"}; !reflect.DeepEqual(pv.Arms, want) {
		t.Errorf("arms %v want %v", pv.Arms, want)
	}
	if pv.Nodes[0].ID != "p1/a/r1" || pv.Nodes[2].Name != "tp-p2-a-r1" {
		t.Errorf("order: %+v", pv.Nodes[:3])
	}
	if len(pv.Edges) != 1 || pv.Edges[0] != "p1 -> p2 (memory)" {
		t.Errorf("edges %v", pv.Edges)
	}
	var out strings.Builder
	pv.Render(&out)
	for _, want := range []string{"12 nodes", "p1/a/r1", "p1 -> p2 (memory)"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("render lacks %q:\n%s", want, out.String())
		}
	}

	// A taken name is refused, as `plan add` refuses it.
	if _, err := Add(plans, file, l.clock, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := DryRun(plans, file, l.clock, nil); err == nil || !strings.Contains(err.Error(), "exists") {
		t.Errorf("dry run of an existing plan: %v", err)
	}
	entries, _ := os.ReadDir(plans)
	if len(entries) != 1 {
		t.Errorf("dry run left directories behind: %v", entries)
	}
}

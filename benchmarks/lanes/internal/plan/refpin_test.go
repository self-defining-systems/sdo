package plan

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

var errNoRef = errors.New("no such ref")

const refPlan = `
plan: tp
pins: {workload: fuzzops@HEAD, subject: "sdo@origin/main:/src/sdo"}
arms: [{name: a, spec: tpl-plain.yaml}]
items: [x]
`

const (
	shaHead = "0123456789abcdef0123456789abcdef01234567"
	shaMain = "fedcba9876543210fedcba9876543210fedcba98"
)

// A pin may name a ref: it is resolved to a full revision once, at add time, and the stored plan, the lock and every
// rendered spec carry the revision.
func TestRefPinsAreResolvedToFullRevisionsAtAdd(t *testing.T) {
	l := newLab(t)
	l.write("tpl-plain.yaml", `{"app": "examples/${item}", "operator": {"hooks": "h"}, "faults": {"ids": ["${item}"]}}`)
	file := l.write("plan.yaml", refPlan)
	type call struct{ dir, ref string }
	var calls []call
	resolve := func(dir, ref string) (string, error) {
		calls = append(calls, call{dir, ref})
		if ref == "HEAD" {
			return shaHead, nil
		}
		return shaMain, nil
	}
	lock, err := Add(filepath.Join(l.root, "plans"), file, l.clock, nil, WithResolver(resolve))
	if err != nil {
		t.Fatal(err)
	}
	if lock.Pins.Workload.Rev != shaHead || lock.Pins.Subject.Rev != shaMain || lock.Pins.Workload.Ref != "" {
		t.Fatalf("lock pins %+v", lock.Pins)
	}
	// HEAD resolves in the plan file's own directory (the checkout the plan lives in); a pin with a path in that one.
	if len(calls) != 2 || calls[0] != (call{l.root, "HEAD"}) || calls[1] != (call{"/src/sdo", "origin/main"}) {
		t.Fatalf("resolver calls %v", calls)
	}
	e := l.open()
	l.tick(e)
	var spec map[string]any
	if err := json.Unmarshal(mustRead(t, l.fb.subs[0].SpecPath), &spec); err != nil {
		t.Fatal(err)
	}
	if spec["fuzzops"] != shaHead || spec["operator"].(map[string]any)["revision"] != shaMain {
		t.Fatalf("rendered pins %v %v", spec["fuzzops"], spec["operator"])
	}
	if strings.Contains(string(mustRead(t, filepath.Join(l.dir(), "plan.json"))), "HEAD") {
		t.Error("the stored plan still holds the ref")
	}
}

func TestRefPinWithoutAResolverOrWithAFailingOneIsRefused(t *testing.T) {
	l := newLab(t)
	l.write("tpl-plain.yaml", `{"app": "x"}`)
	file := l.write("plan.yaml", refPlan)
	if _, err := Add(filepath.Join(l.root, "plans"), file, l.clock, nil); err == nil || !strings.Contains(err.Error(), "HEAD") {
		t.Errorf("no resolver: %v", err)
	}
	bad := func(dir, ref string) (string, error) { return "", errNoRef }
	if _, err := Add(filepath.Join(l.root, "plans"), file, l.clock, nil, WithResolver(bad)); err == nil || !strings.Contains(err.Error(), "no such ref") {
		t.Errorf("failing resolver: %v", err)
	}
	notSHA := func(dir, ref string) (string, error) { return "main", nil }
	if _, err := Add(filepath.Join(l.root, "plans"), file, l.clock, nil, WithResolver(notSHA)); err == nil {
		t.Error("a resolver answer that is not a revision was accepted")
	}
}

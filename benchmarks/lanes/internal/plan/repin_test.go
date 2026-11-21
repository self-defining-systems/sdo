package plan

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

const newSubject = "abcdef0123456789abcdef0123456789abcdef01"

func repinResolve(_, ref string) (string, error) {
	if ref == "fixed" {
		return newSubject, nil
	}
	return "", errors.New("no such ref")
}

func specOf(t *testing.T, l *lab, exp string) string {
	t.Helper()
	for _, s := range l.fb.subs {
		if s.Exp == exp {
			return string(mustRead(t, s.SpecPath))
		}
	}
	t.Fatalf("no submit of %s", exp)
	return ""
}

// A plan's pins are locked, so after a subject fix the operator removed the plan and added it again. `plan retry
// --repin` re-pins the retried nodes only: done nodes keep their pin in results, the change is in the state.
func TestRetryRepinAppliesTheNewPinToRetriedNodesOnly(t *testing.T) {
	l := newLab(t)
	e := l.add(retryPlan)
	parkA(l, e)
	l.fb.finish(l.fb.expOf("cold/b/r1"), PhaseDone, "")
	l.tick(e)

	var validated []string
	validate := func(b []byte) error { validated = append(validated, string(b)); return nil }
	if err := RetryPlanRepin(l.dir(), "a", false, []string{"subject=fixed"}, validate, WithResolver(repinResolve)); err != nil {
		t.Fatal(err)
	}
	if len(validated) == 0 || !strings.Contains(validated[0], newSubject) {
		t.Fatalf("the new pin was not validated through the rendered spec: %v", validated)
	}
	l.tick(l.open())

	exp := l.fb.expOf("p1/a/r1")
	if got := specOf(t, l, exp); !strings.Contains(got, newSubject) || strings.Contains(got, "9b2cadb7f") {
		t.Fatalf("retried node spec keeps the old pin:\n%s", got)
	}
	st := l.state()
	if len(st.PinChanges) != 1 || st.PinChanges[0].Role != "subject" || st.PinChanges[0].To != "sdo@"+newSubject ||
		!contains(st.PinChanges[0].Nodes, "p1/a/r1") || contains(st.PinChanges[0].Nodes, "cold/b/r1") {
		t.Fatalf("pin change not recorded: %+v", st.PinChanges)
	}
	if st.node("cold/b/r1").Pins != nil {
		t.Fatalf("a done node was re-pinned: %+v", st.node("cold/b/r1").Pins)
	}

	// The re-pinned run is on the new subject: no source mismatch, and its row names the new pin.
	rd := RunData{Run: "r", WorkloadObserved: "4652656aa4f8ad4c3", SubjectObserved: newSubject[:13], Isolation: Isolation{Mode: "two-way"},
		Grade: []byte(`{"valid": true}`)}
	l.fb.rundata[exp] = rd
	l.fb.finish(exp, PhaseDone, "")
	l.tick(l.open())
	if n := l.state().node("p1/a/r1"); n.Status != NodeDone {
		t.Fatalf("re-pinned node %+v", n)
	}
	rows, _, err := Rows(l.dir())
	if err != nil {
		t.Fatal(err)
	}
	pinned := map[string]string{}
	for _, r := range rows {
		if r.Status == "done" {
			pinned[r.Node] = r.Source.SubjectPinned
		}
	}
	if pinned["cold/b/r1"] != "sdo@9b2cadb7f" || pinned["p1/a/r1"] != "sdo@"+newSubject {
		t.Fatalf("row pins %v", pinned)
	}
}

func TestRetryRepinRefusesWhatItCannotResolveOrValidateAndLeavesNoRequest(t *testing.T) {
	l := newLab(t)
	e := l.add(retryPlan)
	parkA(l, e)
	dir := l.dir()
	cases := []struct {
		name     string
		repin    []string
		validate func([]byte) error
		want     string
	}{
		{"unknown ref", []string{"subject=nope"}, nil, "no such ref"},
		{"unknown role", []string{"app=fixed"}, nil, "subject or workload"},
		{"not role=rev", []string{"fixed"}, nil, "role=revision"},
		{"spec refused", []string{"subject=fixed"}, func([]byte) error { return fmt.Errorf("hooks not checked out") }, "hooks not checked out"},
	}
	for _, c := range cases {
		err := RetryPlanRepin(dir, "a", false, c.repin, c.validate, WithResolver(repinResolve))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err %v, want %q", c.name, err, c.want)
		}
	}
	if ctl, _ := LoadControl(dir); len(ctl.Retries) != 0 {
		t.Fatalf("a refused repin left a request: %+v", ctl.Retries)
	}
}

package plan

import (
	"errors"
	"strings"
	"testing"
)

// A submit that keeps failing must not loop silently: after a bounded number of passes it raises an alarm and fails
// the node as a reason ("submit"); the same failure again parks the item.
func TestSubmitFailingForeverRaisesAnAlarmAndParksTheItem(t *testing.T) {
	l := newLab(t)
	e := l.add(twopass)
	l.fb.submitErr = errors.New("fault IDs not in the instances file: nope")
	for i := 0; i < 2*SubmitPasses+2; i++ {
		l.tick(e)
	}
	st := l.state()
	if st.Parked["a"] != "submit" {
		t.Fatalf("parked %v; node p1/a/r1 is %s %s", st.Parked, st.node("p1/a/r1").Status, st.node("p1/a/r1").Reason)
	}
	found := false
	for _, k := range l.fb.kinds() {
		if strings.HasPrefix(k, AlarmSubmit+" ") {
			found = true
		}
	}
	if !found {
		t.Fatalf("no submit alarm: %v", l.fb.kinds())
	}
	if !strings.Contains(strings.Join(st.node("p1/a/r1").attemptDetails(), "|"), "fault IDs not in the instances file") {
		t.Errorf("the reason lacks the submit error: %+v", st.node("p1/a/r1").Attempts)
	}
}

func (n *NodeState) attemptDetails() []string {
	var out []string
	for _, a := range n.Attempts {
		out = append(out, a.Detail)
	}
	return out
}

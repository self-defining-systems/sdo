package plan

import (
	"strings"
	"testing"
	"time"
)

func TestStatusCountsItemsByStateWithParkedReasonsAndNextItems(t *testing.T) {
	l := newLab(t)
	e := l.add(twopass)
	l.tick(e) // p1/a/r1 and cold/a/r1 in flight

	// Item a's two runs fail the same way twice: parked. Item b has nothing released yet.
	for i := 0; i < 2; i++ {
		l.fb.finish(l.fb.expOf("p1/a/r1"), PhaseFailed, "oom")
		l.fb.finish(l.fb.expOf("cold/a/r1"), PhaseFailed, "oom")
		l.tick(e)
	}
	if err := SkipItem(l.dir(), "b", false); err != nil {
		t.Fatal(err)
	}
	if err := PausePlan(l.dir()); err != nil {
		t.Fatal(err)
	}
	l.tick(e)

	s, err := e.Status()
	if err != nil {
		t.Fatal(err)
	}
	if s.Items.Total != 2 || s.Items.Parked != 1 || s.Items.Skipped != 1 || s.Items.Done != 0 || s.Items.Inflight != 0 {
		t.Fatalf("item counts %+v", s.Items)
	}
	if len(s.ItemRows) != 2 || s.ItemRows[0].Item != "a" || s.ItemRows[0].State != "parked" || !strings.Contains(s.ItemRows[0].Reason, "oom") {
		t.Fatalf("item rows %+v", s.ItemRows)
	}
	var out strings.Builder
	s.Render(&out)
	for _, want := range []string{"paused", "items: 2", "1 parked", "1 skipped", "item a parked: oom"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("render lacks %q:\n%s", want, out.String())
		}
	}
}

func TestStatusNamesTheNextItemsAndTheAlarmsStandingSince(t *testing.T) {
	l := newLab(t)
	e := l.add(twopass)
	l.tick(e)
	s, err := e.Status()
	if err != nil {
		t.Fatal(err)
	}
	if s.Items.Inflight != 1 || s.Items.Queued != 1 {
		t.Fatalf("item counts %+v", s.Items)
	}
	if len(s.NextItems) == 0 || s.NextItems[0] != "b" {
		t.Fatalf("next items %v", s.NextItems)
	}
	st := l.state()
	st.Alarms.Active = map[string]time.Time{"stage:p1/a/r1:pre-inject": l.clock.Now()}
	if err := writeJSONFile(stateFile(l.dir()), st); err != nil {
		t.Fatal(err)
	}
	s, _ = e.Status()
	if len(s.AlarmSince) != 1 {
		t.Fatalf("alarms %v", s.AlarmSince)
	}
	var out strings.Builder
	s.Render(&out)
	if !strings.Contains(out.String(), "alarm:   stage:p1/a/r1:pre-inject (since 08:00Z)") || !strings.Contains(out.String(), "next items: b") {
		t.Fatalf("render:\n%s", out.String())
	}
}

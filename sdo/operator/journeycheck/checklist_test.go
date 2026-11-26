package journeycheck

import (
	"math"
	"strings"
	"testing"

	"sdo.dev/operator/memory/appmap"
)

// checklistFixture is a synthetic goal of seven journeys and a map that leaves
// each in a different state: served and passing, unmapped, served by an
// operation no probe exercises, failing, not selected by --only, a write only
// a call probe exercises, and a probe no health-probe workload schedules.
func checklistFixture() ([]appmap.GivenJourney, appmap.Map, Report) {
	given := []appmap.GivenJourney{
		{ID: "sign-in", What: "signing in"},
		{ID: "browse", What: "browse items"},
		{ID: "share", What: "share an item"},
		{ID: "edit", What: "edit an item"},
		{ID: "export", What: "export items"},
		{ID: "upload", What: "upload a file"},
		{ID: "notify", What: "notifications"},
	}
	m := appmap.Map{
		Operations: []appmap.Operation{
			{ID: "login", Journey: "sign-in"},
			{ID: "login-form", Journey: "sign-in"},
			{ID: "share-item", Journey: "share", Writes: true},
			{ID: "edit-item", Journey: "edit"},
			{ID: "export-items", Journey: "export"},
			{ID: "upload-file", Journey: "upload", Writes: true},
			{ID: "send-note", Journey: "notify"},
			{ID: "stray", Journey: "not-in-goal"},
		},
		Probes: []appmap.Probe{
			{Scenario: "s-login", Kind: appmap.KindCall, Operations: []string{"login"}},
			{Scenario: "s-login-fast", Kind: appmap.KindCall, Operations: []string{"login"}},
			{Scenario: "s-form", Kind: appmap.KindCall, Operations: []string{"login-form"}},
			{Scenario: "s-edit", Kind: appmap.KindCall, Operations: []string{"edit-item"}},
			{Scenario: "s-export", Kind: appmap.KindCall, Operations: []string{"export-items"}},
			{Scenario: "s-upload", Kind: appmap.KindCall, Operations: []string{"upload-file"}},
			{Scenario: "s-note", Kind: appmap.KindCall, Operations: []string{"send-note"}},
		},
	}
	r := Report{Cap: 3, PlannedRate: 1.2,
		Results: []Result{
			{ItemBudget, "all", Green, "1.20 of 3.0"},
			{ItemPasses, "s-login", Green, "3 runs passed"},
			{ItemPasses, "s-login-fast", Green, "3 runs passed"},
			{ItemPasses, "s-form", Green, "3 runs passed"},
			{ItemAltered, "s-login", Green, "noticed 4 of 4"},
			{ItemPasses, "s-edit", Red, "run 1 on the healthy application: error at step put"},
			{ItemPasses, "s-upload", Green, "3 runs passed"},
		},
		Schedules: []Schedule{
			{Scenario: "s-login", Workloads: []string{"probe-call"}, Requests: 2, Cost: 0.05, Every: 40, Typical: 26, Worst: 52},
			{Scenario: "s-login-fast", Workloads: []string{"probe-fast-call"}, Requests: 1, Cost: 0.1, Every: 10, Typical: 10.5, Worst: 21},
			{Scenario: "s-form", Workloads: []string{"probe-call"}, Requests: 1, Cost: 0.025, Every: 40, Typical: 25.5, Worst: 51},
			{Scenario: "s-edit", Workloads: []string{"probe-call"}, Requests: 3, Cost: 0.075, Every: 40, Typical: 26, Worst: 52},
			{Scenario: "s-export", Workloads: []string{"probe-call"}, Requests: 1, Cost: 0.025, Every: 40, Typical: 25.5, Worst: 51},
			{Scenario: "s-upload", Workloads: []string{"probe-call"}, Requests: 2, Cost: 0.05, Every: 40, Typical: 26, Worst: 52},
		},
	}
	return given, m, r
}

func TestChecklistFollowsTheGoalAndSaysWhatEachJourneyLacks(t *testing.T) {
	given, m, r := checklistFixture()
	r.AddChecklist(given, m)
	want := []struct{ id, status, says string }{
		{"sign-in", JourneyPassed, "noticed"},
		{"browse", JourneyMissingMap, "journey: browse"},
		{"share", JourneyMissingProbe, "share-item"},
		{"edit", JourneyFailing, "s-edit"},
		{"export", JourneyUnchecked, "s-export"},
		{"upload", JourneyMissingProbe, "write-read"},
		{"notify", JourneyMissingProbe, "s-note"},
	}
	if len(r.Journeys) != len(want) {
		t.Fatalf("journeys = %+v", r.Journeys)
	}
	for i, w := range want {
		j := r.Journeys[i]
		if j.ID != w.id || j.Status != w.status || !strings.Contains(j.Detail, w.says) {
			t.Errorf("row %d = %s %s %q, want %s %s mentioning %q", i+1, j.ID, j.Status, j.Detail, w.id, w.status, w.says)
		}
	}
	if !hasResult(r, ItemGivenJourney, "stray", Red) {
		t.Errorf("an operation naming a journey the goal does not list is red: %+v", r.Results)
	}
	if got := r.JourneyCounts(); got[JourneyPassed] != 1 || got[JourneyMissingProbe] != 3 || got[JourneyUnchecked] != 1 {
		t.Errorf("counts = %v", got)
	}
	if !r.Red() {
		t.Error("a run with missing or failing journeys is red")
	}
}

// The journey is noticed as late as its least-covered operation: each
// operation by its fastest probe, the journey by the slowest operation.
func TestChecklistDetectionIsBoundByTheSlowestOperationAndShowsTheBudget(t *testing.T) {
	given, m, r := checklistFixture()
	r.AddChecklist(given, m)
	signIn := r.Journeys[0]
	if signIn.Typical != 25.5 || signIn.Worst != 51 || signIn.Bottleneck != "login-form" {
		t.Fatalf("sign-in detection = %+v; login is noticed by s-login-fast (10.5/21), login-form only by s-form (25.5/51)", signIn)
	}
	if math.Abs(signIn.Cost-0.175) > 1e-9 {
		t.Fatalf("sign-in cost = %v, want the three scenarios' 0.175 req/s", signIn.Cost)
	}
	edit := r.Journeys[3]
	if edit.Typical != 26 || !strings.Contains(edit.Detail, "10 s") {
		t.Fatalf("edit says what sampling at the floor would cost: %+v", edit)
	}
	text := r.Text()
	first := strings.SplitN(text, "\n", 2)[0]
	if !strings.HasPrefix(first, "journeys") {
		t.Fatalf("the checklist comes first:\n%s", text)
	}
	for _, id := range []string{"sign-in", "browse", "share", "edit", "export", "upload", "notify"} {
		if !strings.Contains(text, id) {
			t.Fatalf("text lacks %s:\n%s", id, text)
		}
	}
	if strings.Index(text, " browse") > strings.Index(text, " notify") {
		t.Fatalf("rows keep the goal's order:\n%s", text)
	}
	if !strings.Contains(text, "1 passed") || !strings.Contains(text, "1 unchecked") || !strings.Contains(text, "1.80 left") {
		t.Fatalf("summary counts and budget left:\n%s", text)
	}
}

// A run that judges every scenario cannot pass with a journey left out: the
// unmapped journey alone makes it red. A --only run reports what it did not
// check instead of calling it passed.
func TestAnOmittedJourneyIsNeverSilentlyPassed(t *testing.T) {
	given := []appmap.GivenJourney{{ID: "sign-in", What: "signing in"}, {ID: "browse", What: "browse"}}
	m := appmap.Map{Operations: []appmap.Operation{{ID: "login", Journey: "sign-in"}}, Probes: []appmap.Probe{{Scenario: "s-login", Kind: appmap.KindCall, Operations: []string{"login"}}}}
	r := Report{Cap: 3, Results: []Result{{ItemBudget, "all", Green, ""}, {ItemPasses, "s-login", Green, ""}}, Schedules: []Schedule{{Scenario: "s-login", Workloads: []string{"w"}, Requests: 1, Cost: 0.1, Every: 10, Typical: 7, Worst: 20}}}
	r.AddChecklist(given, m)
	if !r.Red() || r.Reds() != 0 {
		t.Fatalf("no item is red, yet the unmapped journey makes the run red: red=%v reds=%d", r.Red(), r.Reds())
	}
	r = Report{Cap: 3, Results: []Result{{ItemBudget, "all", Green, ""}}, Schedules: r.Schedules}
	m.Operations = append(m.Operations, appmap.Operation{ID: "list", Journey: "browse"})
	m.Probes = append(m.Probes, appmap.Probe{Scenario: "s-list", Kind: appmap.KindCall, Operations: []string{"list"}})
	r.Schedules = append(r.Schedules, Schedule{Scenario: "s-list", Workloads: []string{"w"}, Requests: 1, Cost: 0.1, Every: 10, Typical: 7, Worst: 20})
	r.AddChecklist(given, m)
	if r.Red() || r.JourneyCounts()[JourneyUnchecked] != 2 || !strings.Contains(r.Text(), "2 unchecked") {
		t.Fatalf("nothing judged: unchecked, not passed and not red:\n%s", r.Text())
	}
	r = Report{Cap: 3}
	r.AddChecklist(nil, m)
	if !strings.Contains(r.Text(), "goal lists no journeys") {
		t.Fatalf("a goal without journeys is said:\n%s", r.Text())
	}
}

func hasResult(r Report, item, subject, status string) bool {
	for _, x := range r.Results {
		if x.Item == item && x.Subject == subject && x.Status == status {
			return true
		}
	}
	return false
}

package journeycheck

import (
	"fmt"
	"strings"

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/sdk/traffic"
)

// ItemGivenJourney is red for an operation that names a journey the goal does
// not list.
const ItemGivenJourney = "given-journey"

// Journey statuses, from the furthest from done.
const (
	// JourneyMissingMap: no operation of the map serves the journey.
	JourneyMissingMap = "missing-map"
	// JourneyMissingProbe: an operation that serves it has no scheduled probe
	// that checks its result.
	JourneyMissingProbe = "missing-probe"
	// JourneyFailing: a probe of it is red in this run.
	JourneyFailing = "failing"
	// JourneyUnchecked: a probe of it was not judged in this run (--only or
	// --plan); nothing of it is red.
	JourneyUnchecked = "unchecked"
	// JourneyPassed: every probe of it was judged and none is red.
	JourneyPassed = "passed"
)

var journeyOrder = []string{JourneyPassed, JourneyFailing, JourneyMissingProbe, JourneyMissingMap, JourneyUnchecked}

// JourneyItem is one given journey's row of the checklist. Typical and Worst
// are the seconds until a full failure of the journey is noticed: each
// operation by its soonest probe, the journey by its latest operation
// (Bottleneck). Zero when an operation has no probe that notices.
type JourneyItem struct {
	ID         string   `json:"id"`
	What       string   `json:"what"`
	Status     string   `json:"status"`
	Detail     string   `json:"detail"`
	Operations []string `json:"operations,omitempty"`
	Scenarios  []string `json:"scenarios,omitempty"`
	Typical    float64  `json:"typical_s,omitempty"`
	Worst      float64  `json:"worst_s,omitempty"`
	Bottleneck string   `json:"bottleneck,omitempty"`
	// Cost is the requests per second of its scenarios.
	Cost float64 `json:"requests_per_second,omitempty"`
}

// AddChecklist turns the goal's journeys, in the goal's order, into the
// report's checklist, from the map's operations and probes and this run's
// results and schedules.
func (r *Report) AddChecklist(given []appmap.GivenJourney, m appmap.Map) {
	r.Journeys, r.Unlisted = nil, len(given) == 0
	if len(given) == 0 {
		return
	}
	listed := map[string]bool{}
	for _, g := range given {
		listed[g.ID] = true
	}
	for _, op := range m.Operations {
		if op.Journey != "" && !listed[op.Journey] {
			r.Results = append([]Result{{ItemGivenJourney, op.ID, Red, fmt.Sprintf("names journey %q, which the goal does not list: use a listed journey id, or drop the field", op.Journey)}}, r.Results...)
		}
	}
	schedules := map[string]Schedule{}
	for _, s := range r.Schedules {
		schedules[s.Scenario] = s
	}
	for _, g := range given {
		r.Journeys = append(r.Journeys, r.journey(g, m, schedules))
	}
}

func (r *Report) journey(g appmap.GivenJourney, m appmap.Map, schedules map[string]Schedule) JourneyItem {
	item := JourneyItem{ID: g.ID, What: g.What}
	var lacking, failing, unchecked, notRun []string
	var bottleneck Schedule
	seen := map[string]bool{}
	complete := true
	for _, op := range m.Operations {
		if op.Journey != g.ID {
			continue
		}
		item.Operations = append(item.Operations, op.ID)
		var named, scheduled []string
		reads := false
		var best Schedule
		for _, p := range m.Probes {
			if !contains(p.Operations, op.ID) {
				continue
			}
			named = append(named, p.Scenario)
			s, ok := schedules[p.Scenario]
			if !ok || s.KnownBroken {
				continue
			}
			scheduled = append(scheduled, p.Scenario)
			reads = reads || p.Kind == appmap.KindWriteRead || p.Kind == appmap.KindReadLater
			if best.Scenario == "" || s.Typical < best.Typical {
				best = s
			}
			if !seen[p.Scenario] {
				seen[p.Scenario] = true
				item.Scenarios = append(item.Scenarios, p.Scenario)
				item.Cost += s.Cost
				switch why := r.redOf(p.Scenario); {
				case why != "":
					failing = append(failing, p.Scenario+": "+why)
				case !r.judged(p.Scenario):
					unchecked = append(unchecked, p.Scenario)
				default:
					notRun = append(notRun, r.notRun(p.Scenario)...)
				}
			}
		}
		switch {
		case len(named) == 0:
			lacking = append(lacking, op.ID+" has no probe: add one whose steps call it and check the result")
		case len(scheduled) == 0:
			lacking = append(lacking, fmt.Sprintf("%s: its probes %s are not scheduled by a health-probe workload (or are known broken), so they notice nothing", op.ID, strings.Join(named, ", ")))
		case len(op.Triggers) > 0 && !reads:
			lacking = append(lacking, fmt.Sprintf("%s triggers background work but only call probes exercise it (%s): add a write-read or read-later probe that reads its effect back", op.ID, strings.Join(scheduled, ", ")))
		}
		if best.Scenario == "" {
			complete = false
			continue
		}
		if best.Typical > item.Typical || bottleneck.Scenario == "" {
			item.Typical, item.Worst, item.Bottleneck, bottleneck = best.Typical, best.Worst, op.ID, best
		}
	}
	if !complete {
		item.Typical, item.Worst, item.Bottleneck = 0, 0, ""
	}
	detection := ""
	if item.Bottleneck != "" {
		detection = fmt.Sprintf("noticed in about %s, at worst %s (%s via %s every %s); %.2f req/s", seconds(item.Typical), seconds(item.Worst), item.Bottleneck, bottleneck.Scenario, seconds(bottleneck.Every), item.Cost)
		if floor := traffic.MinSampleInterval.Seconds(); bottleneck.Every > floor+0.5 {
			extra := float64(bottleneck.Requests)/floor - float64(bottleneck.Requests)/bottleneck.Every
			detection += fmt.Sprintf("; sampling %s every %d s instead costs +%.2f req/s (%.2f left)", bottleneck.Scenario, int(floor), extra, r.Cap-r.PlannedRate)
		}
	}
	switch {
	case len(item.Operations) == 0:
		item.Status = JourneyMissingMap
		item.Detail = fmt.Sprintf("no operation serves it (%s): map the operations that do it with journey: %s", g.What, g.ID)
	case len(lacking) > 0:
		item.Status = JourneyMissingProbe
		item.Detail = strings.Join(lacking, "; ")
	case len(failing) > 0:
		item.Status = JourneyFailing
		item.Detail = strings.Join(failing, "; ")
	case len(unchecked) > 0:
		item.Status = JourneyUnchecked
		item.Detail = "not judged in this run: " + strings.Join(unchecked, ", ")
	default:
		item.Status = JourneyPassed
		item.Detail = "passed"
		if len(notRun) > 0 {
			item.Detail += "; not run: " + strings.Join(inOrder(notRun, notRunItems), ", ")
		}
	}
	if detection != "" {
		item.Detail += "; " + detection
	}
	return item
}

// redOf is the first red result of the scenario in this run ("" when none).
func (r *Report) redOf(scenario string) string {
	for _, x := range r.Results {
		if x.Status == Red && (x.Subject == scenario || strings.HasSuffix(x.Subject, "/"+scenario)) {
			return x.Item + ": " + clip(x.Detail, 160)
		}
	}
	return ""
}

// judged reports whether this run judged the scenario on the application.
func (r *Report) judged(scenario string) bool {
	for _, x := range r.Results {
		if x.Subject == scenario && x.Item == ItemPasses {
			return true
		}
	}
	return false
}

// notRun names the optional items this run did not run for the scenario.
func (r *Report) notRun(scenario string) []string {
	altered, soaked := false, false
	for _, x := range r.Results {
		altered = altered || (x.Item == ItemAltered && x.Subject == scenario)
		soaked = soaked || (x.Item == ItemSoak && strings.HasSuffix(x.Subject, "/"+scenario))
	}
	var out []string
	if !altered {
		out = append(out, notRunItems[0])
	}
	if !soaked {
		out = append(out, notRunItems[1])
	}
	return out
}

// JourneyCounts counts the checklist's rows by status.
func (r Report) JourneyCounts() map[string]int {
	counts := map[string]int{}
	for _, j := range r.Journeys {
		counts[j.Status]++
	}
	return counts
}

// Red reports whether anything needs fixing: a red item, or a journey that is
// unmapped, without a probe or failing.
func (r Report) Red() bool {
	counts := r.JourneyCounts()
	return r.Reds() > 0 || counts[JourneyMissingMap]+counts[JourneyMissingProbe]+counts[JourneyFailing] > 0
}

func (r Report) checklistText() string {
	var b strings.Builder
	if r.Unlisted {
		b.WriteString("journeys: the goal lists no journeys; the probes below are the checklist\n")
		return b.String()
	}
	if len(r.Journeys) == 0 {
		return ""
	}
	b.WriteString("journeys (the goal's order; noticed = a full failure, about / at worst, from the schedule):\n")
	for i, j := range r.Journeys {
		fmt.Fprintf(&b, "  %d %s %s: %s\n", i+1, j.Status, j.ID, j.Detail)
	}
	counts := r.JourneyCounts()
	var parts []string
	for _, status := range journeyOrder {
		if counts[status] > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", counts[status], status))
		}
	}
	fmt.Fprintf(&b, "journeys: %s of %d; budget %.2f of %.1f requests/s, %.2f left\n", strings.Join(parts, ", "), len(r.Journeys), r.PlannedRate, r.Cap, r.Cap-r.PlannedRate)
	return b.String()
}

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

var notRunItems = []string{"altered-answers (--alter)", "healthy-run (--soak)"}

// inOrder is the members of order that items holds, in order's order.
func inOrder(items, order []string) []string {
	var out []string
	for _, x := range order {
		if contains(items, x) {
			out = append(out, x)
		}
	}
	return out
}

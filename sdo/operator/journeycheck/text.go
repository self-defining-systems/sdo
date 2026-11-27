package journeycheck

import (
	"fmt"
	"sort"
	"strings"
)

// Text lists the checklist and one line per scenario, red scenarios first.
func (r Report) Text() string {
	if r.Once {
		return r.VerboseText()
	}
	type row struct {
		schedule *Schedule
		failures []string
		passed   bool
	}
	rows := map[string]*row{}
	get := func(id string) *row {
		if rows[id] == nil {
			rows[id] = &row{}
		}
		return rows[id]
	}
	if r.Plan {
		for _, schedule := range r.Schedules {
			get(schedule.Scenario).schedule = &schedule
		}
	}
	var b strings.Builder
	b.WriteString(r.checklistText())
	for _, result := range r.Results {
		if result.Subject == "all" && (result.Item == ItemBudget || result.Item == "application-map" || result.Item == ItemMemoryCheck) {
			if result.Status == Red {
				fmt.Fprintf(&b, "%s %s: %s\n", Red, result.Item, strings.ReplaceAll(result.Detail, "\n", " "))
			}
			continue
		}
		id := result.Subject
		item := result.Item
		if result.Item == ItemSoak {
			workload, scenario, ok := strings.Cut(id, "/")
			if ok {
				id = scenario
				item += " " + workload
			}
		}
		line := get(id)
		if result.Status == Red {
			line.failures = append(line.failures, item+": "+strings.ReplaceAll(result.Detail, "\n", " "))
		} else if result.Item == ItemPasses {
			line.passed = true
		}
	}
	ids := make([]string, 0, len(rows))
	for id := range rows {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if line := rows[id]; len(line.failures) > 0 {
			fmt.Fprintf(&b, "red %s: %s\n", id, strings.Join(line.failures, "; "))
		}
	}
	for _, id := range ids {
		line := rows[id]
		switch {
		case len(line.failures) > 0:
		case line.schedule != nil && line.schedule.KnownBroken:
			fmt.Fprintf(&b, "planned %s (known broken)\n", id)
		case line.schedule != nil:
			s := line.schedule
			fmt.Fprintf(&b, "planned %s every %s, notice in %s / %s\n", id, seconds(s.Every), seconds(s.Typical), seconds(s.Worst))
		case line.passed:
			fmt.Fprintf(&b, "green %s\n", id)
		}
	}
	b.WriteString(r.memoryText(false))
	b.WriteString(r.summary())
	return b.String()
}

package traffic

import (
	"fmt"
	"regexp"

	"sdo.dev/operator/sdk/logs"
)

// FailureMatch is one recent failing sample whose Error matched a pattern.
type FailureMatch struct {
	ScenarioID string
	PatternID  string
	Sample     Sample
}

// MatchFailures returns the recent failing samples of window's qualified
// scenarios whose Error matches a pattern, scenario by scenario, oldest first,
// one match per sample and pattern. It sees the same recent samples Judge does
// (usable, within the SLO's age and window), so a cause-specific detector keys
// on the exact failure text instead of searching a finding's Evidence.
// Patterns are RE2 and use the same ID and size rules as log patterns; a
// malformed pattern is an error, never a non-match.
func MatchFailures(window Window, patterns ...logs.Pattern) ([]FailureMatch, error) {
	if err := (logs.Query{ID: "traffic-failures", Patterns: patterns}).Validate(); err != nil {
		return nil, fmt.Errorf("traffic failure patterns: %w", err)
	}
	compiled := make([]*regexp.Regexp, len(patterns))
	for i, p := range patterns {
		compiled[i] = regexp.MustCompile(p.RE2) // validated above
	}
	var matches []FailureMatch
	for _, observed := range window.Scenarios {
		if !observed.Qualified {
			continue
		}
		id := observed.Scenario.ID
		slo := window.Workload.ScenarioSLO(id)
		var recent []Sample
		for _, sample := range observed.Samples {
			if sample.Outcome != OutcomeInvalid && window.ObservedAt.Sub(sample.At) <= slo.MaxAge.Duration() {
				recent = append(recent, sample)
			}
		}
		if len(recent) > slo.Window {
			recent = recent[len(recent)-slo.Window:]
		}
		for _, sample := range recent {
			if sample.Outcome == OutcomeOK {
				continue
			}
			for i, re := range compiled {
				if re.MatchString(sample.Error) {
					matches = append(matches, FailureMatch{ScenarioID: id, PatternID: patterns[i].ID, Sample: sample})
				}
			}
		}
	}
	return matches, nil
}

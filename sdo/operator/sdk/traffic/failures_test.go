package traffic

import (
	"testing"
	"time"

	"sdo.dev/operator/sdk/logs"
)

func failureWindow(now time.Time) Window {
	sample := func(age time.Duration, outcome Outcome, msg string) Sample {
		return Sample{At: now.Add(-age), Outcome: outcome, Error: msg}
	}
	return Window{
		ObservedAt: now,
		Scenarios: []ScenarioObservations{
			{Scenario: Descriptor{ID: "browse"}, Qualified: true, Samples: []Sample{
				sample(28*time.Second, OutcomeError, "acme-cache: READONLY replica refuses writes"),
				sample(24*time.Second, OutcomeOK, ""),
				sample(20*time.Second, OutcomeTimeout, "context deadline exceeded"),
				sample(10*time.Second, OutcomeError, "acme-cache: READONLY replica refuses writes"),
			}},
			{Scenario: Descriptor{ID: "checkout"}, Qualified: true, Samples: []Sample{
				sample(5*time.Second, OutcomeError, "status 503"),
				sample(4*time.Second, OutcomeInvalid, "READONLY in a generator bug"),
				sample(48*time.Hour, OutcomeError, "READONLY long ago"),
			}},
			{Scenario: Descriptor{ID: "unproven"}, Samples: []Sample{
				sample(time.Second, OutcomeError, "READONLY but never succeeded"),
			}},
		},
	}
}

func TestMatchFailuresReturnsOnlyRecentFailuresWhoseErrorMatches(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	got, err := MatchFailures(failureWindow(now),
		logs.Pattern{ID: "readonly", RE2: `READONLY\b`},
		logs.Pattern{ID: "deadline", RE2: `deadline exceeded`})
	if err != nil {
		t.Fatal(err)
	}
	type key struct{ scenario, pattern string }
	var keys []key
	for _, m := range got {
		keys = append(keys, key{m.ScenarioID, m.PatternID})
	}
	want := []key{{"browse", "readonly"}, {"browse", "deadline"}, {"browse", "readonly"}}
	if len(keys) != len(want) {
		t.Fatalf("matches = %v, want %v", keys, want)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Fatalf("matches = %v, want %v", keys, want)
		}
	}
	if !got[0].Sample.At.Before(got[2].Sample.At) {
		t.Fatal("matches must keep oldest-first order")
	}
}

func TestMatchFailuresNearMissAndBadPatterns(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	got, err := MatchFailures(failureWindow(now), logs.Pattern{ID: "other", RE2: `connection refused`})
	if err != nil || len(got) != 0 {
		t.Fatalf("near miss = %v, %v; want no matches", got, err)
	}
	for _, bad := range []logs.Pattern{{ID: "x", RE2: `(`}, {ID: "", RE2: `a`}, {ID: "x", RE2: ""}} {
		if _, err := MatchFailures(failureWindow(now), bad); err == nil {
			t.Errorf("pattern %+v: want an error", bad)
		}
	}
}

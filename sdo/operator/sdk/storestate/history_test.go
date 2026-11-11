package storestate_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/storestate"
)

// promotedFindings runs a Go-installed (promoted-baseline) store-state
// detector of engine whose healthy history is the given reads, oldest first
// (the last one is the promoted baseline), against current.
func promotedFindings(t *testing.T, engine storestate.Engine, current string, healthy ...string) []sdk.Finding {
	t.Helper()
	c := config(t)
	c.Engine, c.GapWithoutBaseline = engine, true
	d := detector(t, c)
	at := func(i int) podread.Reading {
		r := reading("")
		r.ObservedAt = time.Unix(int64(100+30*i), 0).UTC()
		return r
	}
	var history []podread.Reading
	for i, out := range healthy {
		r := at(i)
		r.Stdout = out
		history = append(history, r)
	}
	now := at(len(healthy) + 10)
	now.Stdout = current
	q := d.(podread.Consumer).PodReads()[0]
	result := podread.Result{Query: q, Readings: []podread.Reading{now}, Baseline: history[len(history)-1:], History: history}
	findings, err := d.Detect(context.Background(), snapshotOf(q.ID, result))
	if err != nil {
		t.Fatal(err)
	}
	return findings
}

func snapshotOf(id string, result podread.Result) sdk.DetectionContext {
	return sdktest.Snapshot{PodReads: map[string]podread.Result{id: result}}
}

func redisKeys(patterns ...any) string {
	var b strings.Builder
	b.WriteString(healthyRedis + "== keys\ncomplete\n")
	for i := 0; i+1 < len(patterns); i += 2 {
		fmt.Fprintf(&b, "%s\t%d\n", patterns[i], patterns[i+1])
	}
	return b.String()
}

// A key pattern the healthy application itself shrinks (job leases and queue
// entries it deletes) is churn: its drop or disappearance is never a
// namespace finding. A pattern that only held or grew while healthy is
// still judged.
func TestNamespaceIsJudgedAgainstItsHealthyHistory(t *testing.T) {
	healthy := []string{
		redisKeys("lease:*:*", 13, "user:*", 40),
		redisKeys("lease:*:*", 11, "user:*", 40),
		redisKeys("lease:*:*", 13, "user:*", 41),
	}
	for name, test := range map[string]struct {
		current string
		want    int
		in      string
	}{
		"healthy":                  {redisKeys("lease:*:*", 12, "user:*", 41), 0, ""},
		"churning pattern gone":    {redisKeys("user:*", 41), 0, ""},
		"churning pattern dropped": {redisKeys("lease:*:*", 1, "user:*", 41), 0, ""},
		"stable pattern gone":      {redisKeys("lease:*:*", 12), 1, "user:* disappeared"},
		"stable pattern dropped":   {redisKeys("lease:*:*", 12, "user:*", 3), 1, "dropped from 41 to 3"},
	} {
		t.Run(name, func(t *testing.T) {
			findings := promotedFindings(t, storestate.EngineRedis, test.current, healthy...)
			if got := ruleSet(findings)[storestate.RuleNamespaceChanged]; got != test.want {
				t.Fatalf("namespace findings = %d, want %d: %+v", got, test.want, findings)
			}
			if test.in != "" && !strings.Contains(findings[0].Summary, test.in) {
				t.Fatalf("finding = %q, want %q", findings[0].Summary, test.in)
			}
		})
	}
}

// The same rule holds for every engine's tables: a table the healthy
// application creates and drops itself is neither judged when it goes nor
// are its indexes, while a table present in every healthy reading is.
func TestTableMissingFromAHealthyReadingIsNotJudged(t *testing.T) {
	tmp := "shop\tscratch\t50\n"
	tmpIndex := "shop\tscratch\tPRIMARY\tunique 00aa11bb\n"
	with := strings.Replace(healthyMySQL, "== tables\n", tmpIndex+"== tables\n", 1) + tmp
	healthy := []string{with, healthyMySQL, with}
	if got := promotedFindings(t, storestate.EngineMySQL, strings.Replace(with, "shop\titems\tPRIMARY\tunique b80bb774\n", "", 1), healthy...); ruleSet(got)[storestate.RuleCatalogChanged] != 1 {
		t.Fatalf("an index dropped from a stable table: %+v", got)
	}
	withoutIndex := strings.Replace(with, tmpIndex, "", 1)
	if got := promotedFindings(t, storestate.EngineMySQL, withoutIndex, healthy...); len(got) != 0 {
		t.Fatalf("an index change on a table the application drops itself: %+v", got)
	}
	if got := promotedFindings(t, storestate.EngineMySQL, healthyMySQL, healthy...); len(got) != 0 {
		t.Fatalf("a table the application drops itself: %+v", got)
	}
	if got := promotedFindings(t, storestate.EngineMySQL, strings.Replace(with, "shop\torders\t80\n", "", 1), healthy...); ruleSet(got)[storestate.RuleNamespaceChanged] != 1 {
		t.Fatalf("a stable table disappeared: %+v", got)
	}
}

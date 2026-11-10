package storestate_test

import (
	"context"
	"strings"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/storestate"
)

// confirmationReason is the runtime's view of a detector's coverage note.
func confirmationReason(t *testing.T, d sdk.Detector, view sdk.DetectionContext) string {
	t.Helper()
	reasoner, ok := d.(interface {
		ConfirmationReason(sdk.DetectionContext) string
	})
	if !ok {
		t.Fatal("the store-state detector explains no coverage gap")
	}
	return reasoner.ConfirmationReason(view)
}

// gapConfig is a reader Go installs: no designer vetted it against the store
// image, so it has no configured baseline and an unobservable store is a gap.
func gapConfig(t *testing.T) storestate.Config {
	c := config(t)
	c.GapWithoutBaseline = true
	return c
}

func noAuthRedis() string {
	var lines []string
	for _, line := range strings.Split(healthyRedis, "\n") {
		if strings.HasPrefix(line, "== ") {
			lines = append(lines, line, "NOAUTH Authentication required.")
		}
	}
	return strings.Join(lines, "\n") + "\n"
}

func unreachableRedis() string {
	out := ""
	for _, section := range []string{"info", "dbsize", "role", "config", "acl"} {
		out += "== " + section + "\nCould not connect to Redis at 127.0.0.1:6379: Connection refused\n"
	}
	return out
}

// unobservable lists the reads a Go-installed reader can meet on a store it
// was never able to read: the image lacks the CLI, the store needs a password
// the container does not expose, the server is unreachable, or no read ran.
func unobservable(t *testing.T, d sdk.Detector) map[string]sdktest.Snapshot {
	id := d.(podread.Consumer).PodReads()[0].ID
	return map[string]sdktest.Snapshot{
		"no cli":        snapshot(t, d, reading("sh: redis-cli: not found\n")),
		"auth required": snapshot(t, d, reading(noAuthRedis())),
		"unreachable":   snapshot(t, d, reading(unreachableRedis())),
		"exec failed":   snapshot(t, d, podread.Reading{Namespace: "shop", Pod: "cache-0", Error: "exec: container has no shell"}),
		"read failed":   {PodReads: map[string]podread.Result{id: {Query: podread.Query{ID: id}, Error: "pod reads unavailable: no exec credentials"}}},
		"no read":       {},
	}
}

// A store Go reads but never observed healthy is a coverage gap: the detector
// neither errors nor fires, so it cannot stall deployment acceptance or an
// incident's recovery, and it says why the store is not covered.
func TestGoInstalledReaderTreatsAnUnobservedStoreAsACoverageGap(t *testing.T) {
	d := detector(t, gapConfig(t))
	for name, view := range unobservable(t, d) {
		t.Run(name, func(t *testing.T) {
			findings, err := d.Detect(context.Background(), view)
			if err != nil || len(findings) != 0 {
				t.Fatalf("an unobserved store gated health: %+v, %v", findings, err)
			}
			reason := confirmationReason(t, d, view)
			if !strings.Contains(reason, "cache") || !strings.Contains(reason, "not observed") {
				t.Fatalf("coverage reason = %q", reason)
			}
		})
	}
	healthy := snapshot(t, d, reading(healthyRedis))
	if reason := confirmationReason(t, d, healthy); reason != "" {
		t.Fatalf("an observed store reports a gap: %q", reason)
	}
}

// Once the store was read healthy, the same reads are real findings: a
// refusal fires and an unavailable read is a detector error, as for a
// designer's reader.
func TestGoInstalledReaderJudgesAStoreItObservedHealthy(t *testing.T) {
	d := detector(t, gapConfig(t))
	for name, view := range unobservable(t, d) {
		if name == "no read" {
			continue
		}
		t.Run(name, func(t *testing.T) {
			view = withBaseline(view, reading(healthyRedis))
			findings, err := d.Detect(context.Background(), view)
			if name == "auth required" {
				if err != nil || len(findings) == 0 || findings[0].RuleID != storestate.RuleRefused {
					t.Fatalf("a refusal after a healthy baseline = %+v, %v", findings, err)
				}
			} else if err == nil {
				t.Fatalf("an unavailable read after a healthy baseline cleared: %+v", findings)
			}
			if reason := confirmationReason(t, d, view); reason != "" {
				t.Fatalf("an observed store reports a gap: %q", reason)
			}
		})
	}
}

// A readable store is judged on its absolute facts before any baseline: a
// write block is a finding, never a gap.
func TestGoInstalledReaderStillReportsAbsoluteFactsWithoutABaseline(t *testing.T) {
	d := detector(t, gapConfig(t))
	blocked := strings.Replace(healthyRedis, "min-replicas-to-write\n0", "min-replicas-to-write\n1", 1)
	findings, err := d.Detect(context.Background(), snapshot(t, d, reading(blocked)))
	if err != nil || len(findings) == 0 || findings[0].RuleID != storestate.RuleWriteBlocked {
		t.Fatalf("write block without a baseline = %+v, %v", findings, err)
	}
}

// A designer's reader keeps its contract: without a baseline an unavailable
// read is an error and a refusal fires.
func TestDesignerReaderKeepsUnavailableReadsAsErrors(t *testing.T) {
	d := detector(t, config(t))
	if _, err := d.Detect(context.Background(), snapshot(t, d, reading("sh: redis-cli: not found\n"))); err == nil {
		t.Fatal("a designer's reader cleared an unavailable read")
	}
	findings, err := d.Detect(context.Background(), snapshot(t, d, reading(noAuthRedis())))
	if err != nil || len(findings) == 0 || findings[0].RuleID != storestate.RuleRefused {
		t.Fatalf("refusal = %+v, %v", findings, err)
	}
}

// Only a read that could be a healthy reference may become the baseline: a
// refused, unavailable or blocked store read is refused, and a pod read that
// is not a store read is left to the generic rules.
func TestUsableAsBaselineRefusesRefusedReads(t *testing.T) {
	d := detector(t, gapConfig(t))
	query := d.(podread.Consumer).PodReads()[0]
	result := func(out string) podread.Result {
		return podread.Result{Query: query, Readings: []podread.Reading{reading(out)}}
	}
	if err := storestate.UsableAsBaseline(query.ID, result(healthyRedis)); err != nil {
		t.Fatalf("healthy read refused: %v", err)
	}
	for name, out := range map[string]string{
		"auth required": noAuthRedis(),
		"no permission": strings.Replace(healthyRedis, "== dbsize\n12", "== dbsize\nNOPERM User default has no permissions to run the 'dbsize' command", 1),
		"no cli":        "sh: redis-cli: not found\n",
		"blocked":       strings.Replace(healthyRedis, "min-replicas-to-write\n0", "min-replicas-to-write\n1", 1),
	} {
		if err := storestate.UsableAsBaseline(query.ID, result(out)); err == nil {
			t.Errorf("%s read accepted as a baseline", name)
		}
	}
	mixed := result(healthyRedis)
	mixed.Readings = append(mixed.Readings, podread.Reading{Namespace: "shop", Pod: "cache-1", Stdout: noAuthRedis()})
	if err := storestate.UsableAsBaseline(query.ID, mixed); err == nil {
		t.Error("a read with one refused Pod accepted as a baseline")
	}
	if err := storestate.UsableAsBaseline("policy", podread.Result{Readings: []podread.Reading{{Stdout: "NOAUTH"}}}); err != nil {
		t.Errorf("a foreign pod read was judged: %v", err)
	}
}

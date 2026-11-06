package lifecycle_test

import (
	"context"
	"strings"
	"testing"

	"sdo.dev/operator/memory/validate"
)

func distinctFailures(rules ...string) []error {
	errs := make([]error, 0, len(rules))
	for _, rule := range rules {
		errs = append(errs, &validate.Failure{Diagnostics: []validate.Diagnostic{{Rule: rule, Path: ".sdo/" + rule, Message: "message of " + rule, Hint: "hint of " + rule}}})
	}
	return errs
}

// A retry is a fresh session on a rolled-back checkout: it must see every
// distinct rejection so far, newest first, not only the latest.
func TestJudgeRetryCarriesEveryDistinctRejection(t *testing.T) {
	h, fake := gateHarness(t, clear)
	fake.stageErrors = distinctFailures("rule.one", "rule.two", "rule.three", "rule.two")
	if _, err := h.coordinator.Run(context.Background(), config()); err != nil {
		t.Fatal(err)
	}
	last := h.workers.judgeRequests[len(h.workers.judgeRequests)-1].GetPreviousDiagnostics()
	var rules []string
	for _, d := range last {
		rules = append(rules, d.GetRule())
	}
	if got := strings.Join(rules, ","); got != "rule.two,rule.three,rule.one" {
		t.Fatalf("previous diagnostics rules = %q, want each distinct rule once, newest first", got)
	}
}

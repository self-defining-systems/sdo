package operability

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestSummarizeStatesEachItemAndItsFirstFailure(t *testing.T) {
	r := report(t, StageComplete,
		chk("O1.deploy", StatusPass, ""),
		chk("O2.stamps", StatusPass, ""),
		chk("O3.ready", StatusNote, ""),
		Check{ID: "O4.detectors", Phase: PhaseLive, Status: StatusFail, Cause: CauseDetectorFault, Detail: "synthetic"},
		Check{ID: "O4.later", Phase: PhaseLive, Status: StatusFail, Cause: CauseSourceFault},
		chk("O5.memory", StatusPending, CauseProbeUnavailable),
		chk("O6.budget", StatusSkipped, ""),
	)
	got, err := Summarize(r, time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	digest, _ := r.Digest()
	if got.Digest != digest || got.AcceptedCommit != "c0" || got.Stage != StageComplete || got.ObservedAt != "2026-11-06T12:00:00Z" {
		t.Fatalf("envelope = %+v", got)
	}
	want := []ItemSummary{
		{ID: O1, Status: "pass"},
		{ID: O2, Status: "pass"},
		{ID: O3, Status: "note"},
		{ID: O4, Status: "fail", FirstFailedCheck: "O4.detectors", Cause: CauseDetectorFault, Owner: OwnerDesigner},
		{ID: O5, Status: "pending", FirstFailedCheck: "O5.memory", Cause: CauseProbeUnavailable, Owner: OwnerDesigner},
		{ID: O6, Status: "skipped"},
	}
	for i := range want {
		if got.Items[i] != want[i] {
			t.Errorf("item %d = %+v, want %+v", i, got.Items[i], want[i])
		}
	}
	missing, err := Summarize(report(t, StageDeployment, chk("O1.deploy", StatusPass, "")), time.Unix(0, 0))
	if err != nil || missing.Items[2].Status != "missing" {
		t.Fatalf("an item with no checks = %+v, %v; want missing", missing.Items[2], err)
	}
}

// Time lives in the envelope only: the digest of the same state does not move.
func TestSummaryDigestIgnoresObservationTime(t *testing.T) {
	r := allPass(t)
	a, _ := Summarize(r, time.Unix(1, 0))
	b, _ := Summarize(r, time.Unix(2, 0))
	if a.Digest != b.Digest || a.ObservedAt == b.ObservedAt {
		t.Fatalf("digests %s %s, times %s %s", a.Digest, b.Digest, a.ObservedAt, b.ObservedAt)
	}
}

// A report over the size cap keeps every failing and pending check and the full
// digest, and says it was truncated.
func TestSummaryDataTruncatesLargeReports(t *testing.T) {
	var cs []Check
	for i := 0; i < 2000; i++ {
		cs = append(cs, Check{ID: "O1.check-" + strconv.Itoa(i), Phase: PhaseLive, Status: StatusPass, Detail: strings.Repeat("d", 450), Paths: []string{"a/b"}})
	}
	cs = append(cs, Check{ID: "O2.bad", Phase: PhaseLive, Status: StatusFail, Cause: CauseAttribution, Detail: "kept"})
	r := report(t, StageDeployment, cs...)
	full, err := r.Encode()
	if err != nil || len(full) <= MaxSummaryReportBytes {
		t.Fatalf("test report is %d bytes (%v), want over the cap", len(full), err)
	}
	data, err := SummaryData(r, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	var summary Summary
	if err := json.Unmarshal([]byte(data["summary.json"]), &summary); err != nil {
		t.Fatal(err)
	}
	digest, _ := r.Digest()
	if !summary.ReportTruncated || summary.Digest != digest || len(data["report.json"]) > MaxSummaryReportBytes {
		t.Fatalf("truncated=%v digest ok=%v size=%d", summary.ReportTruncated, summary.Digest == digest, len(data["report.json"]))
	}
	if !strings.Contains(data["report.json"], `"O2.bad"`) {
		t.Fatal("a failing check was dropped from the truncated report")
	}
}

// The summary names no benchmark, fault, verdict or application.
func TestSummaryDataHasNoBenchmarkVocabulary(t *testing.T) {
	r := allPass(t)
	r, _ = r.With(Check{ID: "O1.render", Phase: PhaseOffline, Status: StatusNote, Detail: Clip("template rendered")})
	data, err := SummaryData(r, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range data {
		for _, word := range []string{"verdict", "sregym", "benchmark", "fault_injection", "grade", "secret"} {
			if strings.Contains(strings.ToLower(value), word) {
				t.Errorf("%s contains %q", key, word)
			}
		}
	}
}

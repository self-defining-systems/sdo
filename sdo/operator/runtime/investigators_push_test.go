package runtime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAReportArrivalIsShownOnceOnTheNextVerifyAndTry(t *testing.T) {
	board := &InvestigationBoard{}
	board.start("i-inv-1", "db", candidateOnset)
	board.start("i-inv-2", "cache", candidateOnset)
	loop := tryLoop(tryProber(time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)))
	loop.Investigations = board
	loop.VerifyClosure = func(context.Context) ToolResponse { return ToolResponse{Command: "verify", Summary: "not recovered"} }
	before := loop.Handle(context.Background(), ToolRequest{Command: "try"})
	if strings.Contains(before.Summary, "investigator") {
		t.Fatalf("nothing arrived yet: %q", before.Summary)
	}
	board.finish("i-inv-1", investigatorDone, "Verdict: refuted - the extra row is routine\nbody")
	first := loop.Handle(context.Background(), ToolRequest{Command: "try"})
	for _, want := range []string{"investigator report (node db): Verdict: refuted - the extra row is routine", "sdo investigations"} {
		if !strings.Contains(first.Summary, want) || !strings.Contains(first.Text, want) {
			t.Fatalf("try lacks %q: %q", want, first.Summary)
		}
	}
	if strings.Contains(first.Summary, "investigator supports") {
		t.Fatalf("a refuted report adds no support line: %q", first.Summary)
	}
	if again := loop.Handle(context.Background(), ToolRequest{Command: "try"}); strings.Contains(again.Summary, "investigator") {
		t.Fatalf("shown once: %q", again.Summary)
	}
	board.finish("i-inv-2", investigatorDone, "Verdict: supported - a trigger truncates titles\nbody")
	verify := loop.Handle(context.Background(), ToolRequest{Command: "verify"})
	for _, want := range []string{"investigator report (node cache): Verdict: supported - a trigger truncates titles",
		"investigator supports: a trigger truncates titles - read `sdo investigations` before acting"} {
		if !strings.Contains(verify.Summary, want) {
			t.Fatalf("verify lacks %q: %q", want, verify.Summary)
		}
	}
	if next := loop.Handle(context.Background(), ToolRequest{Command: "verify"}); strings.Contains(next.Summary, "investigator") {
		t.Fatalf("shown once: %q", next.Summary)
	}
}

func TestAReportWithoutAVerdictLineIsStillPointedTo(t *testing.T) {
	board := &InvestigationBoard{}
	board.start("i-inv-1", "db", candidateOnset)
	board.finish("i-inv-1", investigatorDone, "free text only")
	notes := board.ReportNotes()
	if len(notes) != 1 || !strings.Contains(notes[0], "node db") || !strings.Contains(notes[0], "sdo investigations") {
		t.Fatalf("%q", notes)
	}
	board.start("i-inv-2", "x", candidateOnset)
	board.finish("i-inv-2", investigatorFailed, "")
	if len(board.ReportNotes()) != 0 {
		t.Fatal("a failed investigator has no report to push")
	}
}

func TestAFinishedReportIsPersistedToTheIncidentEvidenceDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), investigationsDirName)
	board := &InvestigationBoard{Persist: func(id, node, report string) error { return persistInvestigatorReport(dir, id, node, report) }}
	board.start("i-inv-1", "db", candidateOnset)
	board.start("i-inv-2", "cache", candidateOnset)
	board.finish("i-inv-1", investigatorDone, "Verdict: refuted - x\nHow to diagnose: y")
	board.finish("i-inv-2", investigatorFailed, "")
	data, err := os.ReadFile(filepath.Join(dir, "i-inv-1.txt"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"investigator: i-inv-1", "node: db", "Verdict: refuted - x", "How to diagnose: y"} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("report file lacks %q:\n%s", want, data)
		}
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 1 {
		t.Fatalf("only the finished report is stored: %v", entries)
	}
}

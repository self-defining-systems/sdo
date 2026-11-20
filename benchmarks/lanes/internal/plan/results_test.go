package plan

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func finishAll(l *lab, nodes ...string) {
	for _, n := range nodes {
		l.fb.finish(l.fb.expOf(n), PhaseDone, "")
	}
}

func TestEachEndedAttemptHasARowWithGradeSourceStagesAndIsolation(t *testing.T) {
	l := newLab(t)
	e := l.add(retryPlan)
	l.tick(e)
	l.fb.exps[l.fb.expOf("p1/a/r1")] = Observation{Phase: PhaseDone, Stages: map[string]time.Duration{"pre-inject": 90 * time.Second}}
	l.fb.finish(l.fb.expOf("cold/a/r1"), PhaseFailed, "oom")
	l.tick(e)

	rows, skipped, err := Results(e.Dir, RowFilter{})
	if err != nil || skipped != 0 || len(rows) != 2 {
		t.Fatalf("rows %d skipped %d err %v", len(rows), skipped, err)
	}
	r := rows[0]
	if r.Node != "p1/a/r1" || r.Status != "done" || !r.Valid || r.Run != "run-"+r.Exp || r.Isolation.Mode != "two-way" || r.Stages["pre-inject"] != 90 {
		t.Fatalf("row %+v", r)
	}
	if r.Source.WorkloadPinned != "fuzzops@4652656" || r.Source.WorkloadObserved != "4652656aa4f8ad4c3" || r.Source.SubjectPinned != "sdo@9b2cadb7f" || !r.Source.OK || r.Source.App == "" {
		t.Fatalf("source %+v", r.Source)
	}
	if !strings.Contains(string(r.Grade), `"recovered":true`) {
		t.Fatalf("grade not kept as given: %s", r.Grade)
	}
	if f := rows[1]; f.Node != "cold/a/r1" || f.Status != "failed" || f.Reason != "oom" || f.Valid || len(f.Grade) != 0 {
		t.Fatalf("failed row %+v", f)
	}
	// A rerun is a second row for the node; --final keeps the newest attempt.
	l.fb.finish(l.fb.expOf("cold/a/r1"), PhaseDone, "")
	l.tick(e)
	all, _, _ := Results(e.Dir, RowFilter{})
	final, _, _ := Results(e.Dir, RowFilter{Final: true})
	done, _, _ := Results(e.Dir, RowFilter{Done: true})
	if len(all) != 3 || len(final) != 2 || len(done) != 2 {
		t.Fatalf("all %d final %d done %d", len(all), len(final), len(done))
	}
	// No attempt is rowed twice by later passes or a restart.
	l.tick(l.open())
	l.tick(l.open())
	if again, _, _ := Results(e.Dir, RowFilter{}); len(again) != 3 {
		t.Fatalf("rows grew to %d", len(again))
	}
}

func TestGradeWithoutValidIsKeptButInvalid(t *testing.T) {
	l := newLab(t)
	e := l.add(retryPlan)
	l.tick(e)
	exp := l.fb.expOf("p1/a/r1")
	l.fb.rundata[exp] = RunData{Run: "r", Grade: []byte(`{"recovered": true}`), WorkloadObserved: "4652656"}
	l.fb.rundata[l.fb.expOf("cold/a/r1")] = RunData{Run: "r", Grade: []byte(`[1,2]`)}
	l.fb.rundata[l.fb.expOf("p1/b/r1")] = RunData{Run: "r", GradeError: "no graded line for operator exec:x"}
	finishAll(l, "p1/a/r1", "cold/a/r1", "p1/b/r1")
	l.tick(e)
	rows, _, _ := Results(e.Dir, RowFilter{})
	want := map[string]string{"p1/a/r1": `no boolean "valid"`, "cold/a/r1": "not a JSON object", "p1/b/r1": "no graded line"}
	for _, r := range rows {
		if w, ok := want[r.Node]; ok {
			if r.Valid || !strings.Contains(r.GradeError, w) {
				t.Errorf("%s: valid=%v error %q", r.Node, r.Valid, r.GradeError)
			}
		}
	}
	if rows[0].Node != "p1/a/r1" || len(rows[0].Grade) == 0 {
		t.Fatalf("a grade lacking valid is kept as given: %+v", rows[0])
	}
}

func TestPendingGradeDelaysTheRowNotTheNextRun(t *testing.T) {
	l := newLab(t)
	e := l.add(twopass)
	l.tick(e)
	exp := l.fb.expOf("p1/a/r1")
	l.fb.rundata[exp] = RunData{Run: "r", Pending: true, WorkloadObserved: "4652656"}
	l.fb.finish(exp, PhaseDone, "")
	l.tick(e)
	if got := l.fb.nodes(); got[len(got)-1] != "p2/a/r1" {
		t.Fatalf("pass 2 waited for the grade: %v", got)
	}
	if rows, _, _ := Results(e.Dir, RowFilter{}); len(rows) != 0 {
		t.Fatalf("row written before the grade: %+v", rows)
	}
	l.fb.rundata[exp] = RunData{Run: "r", Grade: []byte(`{"valid": true}`), WorkloadObserved: "4652656"}
	l.tick(e)
	if rows, _, _ := Results(e.Dir, RowFilter{}); len(rows) != 1 || !rows[0].Valid {
		t.Fatalf("rows %+v", rows)
	}
	// A grade that never comes is written without one after the wait.
	exp2 := l.fb.expOf("cold/a/r1")
	l.fb.rundata[exp2] = RunData{Run: "r", Pending: true}
	l.fb.finish(exp2, PhaseDone, "")
	l.tick(e)
	l.clock.Add(gradeWait + time.Minute)
	l.tick(e)
	rows, _, _ := Results(e.Dir, RowFilter{})
	if len(rows) != 2 || rows[1].Valid || rows[1].GradeError != "grading did not finish" {
		t.Fatalf("rows %+v", rows)
	}
}

func TestRunOnOtherSourceThanPinnedIsNotAResult(t *testing.T) {
	l := newLab(t)
	e := l.add(retryPlan)
	l.tick(e)
	exp := l.fb.expOf("p1/a/r1")
	l.fb.rundata[exp] = RunData{Run: "r", WorkloadObserved: "deadbeef00", SubjectObserved: "9b2cadb7f", Grade: []byte(`{"valid": true}`)}
	l.fb.finish(exp, PhaseDone, "")
	l.tick(e)
	st := l.state()
	n := st.node("p1/a/r1")
	if n.Status != NodeSubmitted || n.Attempt != 2 || n.Attempts[0].Reason != "source-mismatch" {
		t.Fatalf("node %+v", n)
	}
	rows, _, _ := Results(e.Dir, RowFilter{})
	if len(rows) != 1 || rows[0].Source.OK || rows[0].Status != "failed" || rows[0].Source.WorkloadObserved != "deadbeef00" {
		t.Fatalf("rows %+v", rows)
	}
	// Plans that require two-way isolation refuse a one-way run.
	l2 := newLab(t)
	e2 := l2.add(retryPlan + "isolation: two-way\n")
	l2.tick(e2)
	exp = l2.fb.expOf("p1/a/r1")
	l2.fb.rundata[exp] = RunData{Run: "r", Isolation: Isolation{Mode: "one-way"}, Grade: []byte(`{"valid": true}`)}
	l2.fb.finish(exp, PhaseDone, "")
	l2.tick(e2)
	if got := l2.state().node("p1/a/r1"); got.Attempts[0].Reason != "source-mismatch" || !strings.Contains(got.Attempts[0].Detail, "two-way") {
		t.Fatalf("attempts %+v", got.Attempts)
	}
}

func TestRowsReadsEmptyMissingAndDamagedFiles(t *testing.T) {
	dir := t.TempDir()
	if rows, skipped, err := Rows(dir); err != nil || rows != nil || skipped != 0 {
		t.Fatalf("missing file: %v %d %v", rows, skipped, err)
	}
	write := func(s string) {
		if err := os.WriteFile(filepath.Join(dir, "results.jsonl"), []byte(s), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("")
	if rows, skipped, err := Rows(dir); err != nil || len(rows) != 0 || skipped != 0 {
		t.Fatalf("empty file: %v %d %v", rows, skipped, err)
	}
	write("\n\n")
	if rows, _, err := Rows(dir); err != nil || len(rows) != 0 {
		t.Fatalf("blank lines: %v %v", rows, err)
	}
	good := `{"plan":"tp","node":"p1/a/r1","arm":"p1","item":"a","rep":1,"attempt":1,"exp":"e1","status":"done","valid":true,"grade":{"valid":true,"x":"y"},"source":{"ok":true},"isolation":{}}`
	again := strings.Replace(good, `"status":"done"`, `"status":"failed"`, 1)
	write(good + "\nnot json at all\n{\"node\":\"\"}\n" + again + "\n" + `{"plan":"tp","node":"p1/b/r1","att`)
	rows, skipped, err := Rows(dir)
	if err != nil || len(rows) != 1 || skipped != 3 {
		t.Fatalf("damaged file: rows %d skipped %d err %v", len(rows), skipped, err)
	}
	if rows[0].Status != "failed" {
		t.Fatalf("a repeated attempt keeps the later line: %+v", rows[0])
	}
}

func TestTSVHasAHeaderWithNoRowsAndExpandsGradeFields(t *testing.T) {
	var b bytes.Buffer
	if err := TSV(&b, nil, nil); err != nil {
		t.Fatal(err)
	}
	if lines := strings.Split(strings.TrimSpace(b.String()), "\n"); len(lines) != 1 || !strings.HasPrefix(lines[0], "plan\tnode\tarm") || !strings.HasSuffix(lines[0], "\tgrade") {
		t.Fatalf("empty TSV: %q", b.String())
	}
	l := newLab(t)
	e := l.add(retryPlan)
	l.tick(e)
	l.fb.exps[l.fb.expOf("p1/a/r1")] = Observation{Phase: PhaseDone, Stages: map[string]time.Duration{"pre-inject": 61 * time.Second}}
	l.fb.rundata[l.fb.expOf("cold/a/r1")] = RunData{Run: "r", Grade: []byte("{\"valid\": false, \"note\": \"a\\tb\\nc\", \"n\": 3}")}
	l.fb.finish(l.fb.expOf("cold/a/r1"), PhaseDone, "")
	l.tick(e)
	rows, _, _ := Results(e.Dir, RowFilter{})
	b.Reset()
	if err := TSV(&b, rows, []string{"recovered", "note", "n", "absent"}); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(b.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("lines %d:\n%s", len(lines), b.String())
	}
	head := strings.Split(lines[0], "\t")
	for _, ln := range lines[1:] {
		if got := len(strings.Split(ln, "\t")); got != len(head) {
			t.Fatalf("row has %d cells, header %d:\n%s", got, len(head), b.String())
		}
	}
	col := func(row, name string) string {
		for i, h := range head {
			if h == name {
				return strings.Split(row, "\t")[i]
			}
		}
		t.Fatalf("no column %s in %v", name, head)
		return ""
	}
	if col(lines[1], "stage_pre-inject_s") != "61" || col(lines[1], "grade.recovered") != "true" || col(lines[1], "isolation_mode") != "two-way" ||
		col(lines[2], "grade.note") != "a b c" || col(lines[2], "grade.n") != "3" || col(lines[2], "grade.absent") != "" || col(lines[2], "valid") != "false" {
		t.Fatalf("cells wrong:\n%s", b.String())
	}
}

func TestPlanIsNotCompleteWhileAGradeIsStillToCome(t *testing.T) {
	l := newLab(t)
	e := l.add("plan: tp\npins: {workload: fuzzops@4652656, subject: sdo@9b2cadb7f}\narms: [{name: p1, spec: tpl-plain.yaml}]\nitems: [a]\n")
	l.tick(e)
	exp := l.fb.expOf("p1/a/r1")
	l.fb.rundata[exp] = RunData{Run: "r", Pending: true}
	l.fb.finish(exp, PhaseDone, "")
	l.tick(e)
	if l.state().Complete {
		t.Fatal("complete with a row missing")
	}
	l.fb.rundata[exp] = RunData{Run: "r", Grade: []byte(`{"valid": true}`)}
	l.tick(e)
	if !l.state().Complete {
		t.Fatal("not complete after the row was written")
	}
}

// The TSV is read by scripts: its fixed columns, their order and their names do not change. New columns go after
// these, before the stage and grade columns, only with a docs/plan.md change that says so.
func TestTSVFixedColumnsAreStable(t *testing.T) {
	var b bytes.Buffer
	if err := TSV(&b, nil, nil); err != nil {
		t.Fatal(err)
	}
	const want = "plan\tnode\tarm\titem\trep\tattempt\texp\trun\tstatus\treason\tvalid\tgrade_error\thost\tlane\tisolation_mode\toom_kills" +
		"\tworkload_pinned\tworkload_observed\tsubject_pinned\tsubject_observed\tapp\tsource_ok\tsubmitted\tended\tgrade\n"
	if b.String() != want {
		t.Fatalf("TSV header changed:\n got %q\nwant %q", b.String(), want)
	}
}

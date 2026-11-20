package plan

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"sort"
	"strings"
	"time"
)

func resultsFile(dir string) string { return dir + "/results.jsonl" }

// SourceID is the source identity of a run (invariant 1): what the plan pinned and what the run reports it used.
type SourceID struct {
	WorkloadPinned   string `json:"workload_pinned"`
	WorkloadObserved string `json:"workload_observed,omitempty"`
	SubjectPinned    string `json:"subject_pinned"`
	SubjectObserved  string `json:"subject_observed,omitempty"`
	App              string `json:"app,omitempty"`
	// OK: no observed revision contradicts its pin. A revision the run did not report, or reported in words, is not a
	// contradiction; it is visible in the observed field.
	OK bool `json:"ok"`
}

// Row is one run of the results: one line of <plan dir>/results.jsonl per ended attempt of a node.
type Row struct {
	Plan    string `json:"plan"`
	Node    string `json:"node"`
	Arm     string `json:"arm"`
	Item    string `json:"item"`
	Rep     int    `json:"rep"`
	Attempt int    `json:"attempt"`
	Exp     string `json:"exp"`
	Run     string `json:"run,omitempty"`
	// Position is the run's place (1-based) among the several runs one experiment made on a lane; 0 for a one-run
	// experiment. Such an experiment has one row per run.
	Position int `json:"position,omitempty"`
	// Status: done, or failed (the attempt ended with Reason).
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
	// Valid is the grade's own "valid"; false for a failed attempt and for a grade that lacks it.
	Valid bool `json:"valid"`
	// Grade is the workload's grade, opaque. GradeError says why it is missing or unusable.
	Grade      json.RawMessage `json:"grade,omitempty"`
	GradeError string          `json:"grade_error,omitempty"`
	Source     SourceID        `json:"source"`
	// Isolation is the mode the run was measured under: two-way or one-way (one-way-ok: no reservation).
	Isolation Isolation          `json:"isolation"`
	Stages    map[string]float64 `json:"stages,omitempty"` // seconds
	Host      string             `json:"host,omitempty"`
	Lane      string             `json:"lane,omitempty"`
	Submitted time.Time          `json:"submitted,omitempty"`
	Ended     time.Time          `json:"ended,omitempty"`
	Artifacts map[string]string  `json:"artifacts,omitempty"`
}

// gradeWait is how long a row waits for a grade that is still being made before it is written without one.
const gradeWait = 30 * time.Minute

// sourceMismatch says which observed revision contradicts its pin ("" when none does). Revisions match by prefix
// either way (a short pin against a long revision). An observed value that is not a git revision (an adapter that
// names its version in words) is recorded in the row and cannot be compared.
func (e *Engine) sourceMismatch(rd RunData) string {
	for _, c := range []struct{ role, pin, got string }{
		{"workload", e.Plan.Pins.Workload.Rev, rd.WorkloadObserved},
		{"subject", e.Plan.Pins.Subject.Rev, rd.SubjectObserved},
	} {
		if revRE.MatchString(c.got) && !strings.HasPrefix(c.got, c.pin) && !strings.HasPrefix(c.pin, c.got) {
			return fmt.Sprintf("%s ran %s, the plan pins %s", c.role, c.got, c.pin)
		}
	}
	if m := e.Plan.Isolation; m == "two-way" && rd.Isolation.Mode != "" && rd.Isolation.Mode != "two-way" {
		return fmt.Sprintf("run isolation %s, the plan requires two-way", rd.Isolation.Mode)
	}
	return ""
}

// recordRows writes the row of every ended attempt that has none. A done attempt waits (up to gradeWait) for its
// grade; a failed one is written at once with whatever the backend knows.
func (e *Engine) recordRows(ctx context.Context, st *State) {
	now := e.Clock.Now().UTC()
	for _, n := range st.Nodes {
		for i := range n.Attempts {
			a := &n.Attempts[i]
			if a.Rowed || a.Exp == "" {
				continue
			}
			var rds []RunData
			var err error
			if a.Reason == "" {
				rds, err = e.Backend.RunsData(ctx, a.Exp)
				if err != nil {
					e.logf("run data %s: %v (next pass)", a.Exp, err)
					continue
				}
			}
			if len(rds) == 0 {
				var rd RunData
				rd, err = e.Backend.RunData(ctx, a.Exp)
				if err != nil && a.Reason == "" {
					e.logf("run data %s: %v (next pass)", a.Exp, err)
					continue
				}
				rds = []RunData{rd}
			}
			if a.Reason == "" && now.Sub(a.Ended) < gradeWait && slices.ContainsFunc(rds, func(rd RunData) bool { return rd.Pending }) {
				continue
			}
			wrote := true
			for _, rd := range rds {
				if err := appendRow(e.Dir, e.row(n, a, rd, err)); err != nil {
					e.logf("results.jsonl: %v", err)
					wrote = false
					break
				}
			}
			a.Rowed = wrote
		}
	}
}

func (e *Engine) row(n *NodeState, a *AttemptRecord, rd RunData, rdErr error) Row {
	r := Row{Plan: e.Plan.Name, Node: n.ID, Arm: n.Arm, Item: n.Item, Rep: n.Rep, Attempt: a.Attempt, Exp: a.Exp, Run: rd.Run, Position: rd.Position,
		Status: "done", Reason: a.Reason, Host: rd.Host, Lane: rd.Lane, Isolation: rd.Isolation, Submitted: a.Submitted, Ended: a.Ended,
		Source: SourceID{WorkloadPinned: e.Plan.Pins.Workload.String(), WorkloadObserved: rd.WorkloadObserved,
			SubjectPinned: e.Plan.Pins.Subject.String(), SubjectObserved: rd.SubjectObserved, App: rd.App, OK: e.sourceMismatch(rd) == ""}}
	if a.Reason != "" {
		r.Status = "failed"
	}
	if len(a.Stages) > 0 {
		r.Stages = map[string]float64{}
		for k, d := range a.Stages {
			r.Stages[k] = d.Seconds()
		}
	}
	if a.Reason == "" {
		r.Artifacts = map[string]string{}
		for k, art := range n.Artifacts {
			r.Artifacts[k] = art.Path
		}
	}
	switch {
	case rdErr != nil:
		r.GradeError = rdErr.Error()
	case a.Reason != "":
		// a failed attempt carries no grade
	default:
		r.Grade, r.Valid, r.GradeError = checkGrade(rd)
	}
	return r
}

// checkGrade returns the grade as stored, whether it says the run is valid, and what is wrong with it. A usable grade
// is a JSON object with a boolean "valid"; anything else keeps its text out of the row and invalidates the run.
func checkGrade(rd RunData) (json.RawMessage, bool, string) {
	if rd.GradeError != "" {
		return nil, false, rd.GradeError
	}
	if rd.Pending {
		return nil, false, "grading did not finish"
	}
	if len(rd.Grade) == 0 {
		return nil, false, "no grade"
	}
	var g map[string]json.RawMessage
	if err := json.Unmarshal(rd.Grade, &g); err != nil {
		return nil, false, "grade is not a JSON object"
	}
	var valid bool
	if raw, ok := g["valid"]; !ok || json.Unmarshal(raw, &valid) != nil {
		return rd.Grade, false, `grade has no boolean "valid"`
	}
	return rd.Grade, valid, ""
}

func appendRow(dir string, r Row) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(resultsFile(dir), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(append(b, '\n'))
	return err
}

// Rows reads a plan's results. A missing or empty file is no rows, not an error; a damaged line (a torn last line,
// stray text) is skipped and counted. When an attempt appears twice (a crash between the write and the state) the
// later line wins. Rows are in the plan's order, then by attempt.
func Rows(dir string) (rows []Row, skipped int, err error) {
	f, err := os.Open(resultsFile(dir))
	if os.IsNotExist(err) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	byKey := map[string]int{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var r Row
		if json.Unmarshal([]byte(line), &r) != nil || r.Node == "" {
			skipped++
			continue
		}
		key := fmt.Sprintf("%s#%d#%d", r.Node, r.Attempt, r.Position)
		if i, ok := byKey[key]; ok {
			rows[i] = r
			continue
		}
		byKey[key] = len(rows)
		rows = append(rows, r)
	}
	if err := sc.Err(); err != nil {
		return rows, skipped, err
	}
	return rows, skipped, nil
}

// RowFilter selects rows.
type RowFilter struct {
	// Final keeps the last attempt of each node. Done keeps done rows only.
	Final, Done bool
}

// Select applies the filter and orders the rows by plan order (the state's node index when known).
func Select(rows []Row, f RowFilter, order map[string]int) []Row {
	last := map[string]int{}
	for _, r := range rows {
		if r.Attempt > last[r.Node] {
			last[r.Node] = r.Attempt
		}
	}
	var out []Row
	for _, r := range rows {
		if (f.Final && r.Attempt != last[r.Node]) || (f.Done && r.Status != "done") {
			continue
		}
		out = append(out, r)
	}
	sort.SliceStable(out, func(i, j int) bool {
		oi, oj := order[out[i].Node], order[out[j].Node]
		if oi != oj {
			return oi < oj
		}
		return out[i].Attempt < out[j].Attempt
	})
	return out
}

// TSV writes the rows as tab-separated text with a header. A header is written even with no rows. gradeFields are
// top-level scalar fields of the opaque grade, added as columns (grade.<name>); the whole grade is in `grade`.
func TSV(w io.Writer, rows []Row, gradeFields []string) error {
	stages := map[string]bool{}
	for _, r := range rows {
		for k := range r.Stages {
			stages[k] = true
		}
	}
	stageCols := sortedKeys(stages)
	head := []string{"plan", "node", "arm", "item", "rep", "attempt", "exp", "run", "status", "reason", "valid", "grade_error", "host", "lane",
		"isolation_mode", "oom_kills", "workload_pinned", "workload_observed", "subject_pinned", "subject_observed", "app", "source_ok", "submitted", "ended"}
	// The position column exists only when some row has one, so a plan of one-run experiments keeps its columns.
	withPosition := slices.ContainsFunc(rows, func(r Row) bool { return r.Position > 0 })
	if withPosition {
		head = slices.Insert(head, 8, "position")
	}
	for _, s := range stageCols {
		head = append(head, "stage_"+s+"_s")
	}
	for _, g := range gradeFields {
		head = append(head, "grade."+g)
	}
	head = append(head, "grade")
	bw := bufio.NewWriter(w)
	fmt.Fprintln(bw, strings.Join(head, "\t"))
	for _, r := range rows {
		cols := []string{r.Plan, r.Node, r.Arm, r.Item, fmt.Sprint(r.Rep), fmt.Sprint(r.Attempt), r.Exp, r.Run, r.Status, r.Reason, fmt.Sprint(r.Valid),
			r.GradeError, r.Host, r.Lane, r.Isolation.Mode, fmt.Sprint(r.Isolation.OOMKills), r.Source.WorkloadPinned, r.Source.WorkloadObserved,
			r.Source.SubjectPinned, r.Source.SubjectObserved, r.Source.App, fmt.Sprint(r.Source.OK), stamp(r.Submitted), stamp(r.Ended)}
		if withPosition {
			cols = slices.Insert(cols, 8, fmt.Sprint(r.Position))
		}
		for _, s := range stageCols {
			if d, ok := r.Stages[s]; ok {
				cols = append(cols, fmt.Sprintf("%.0f", d))
			} else {
				cols = append(cols, "")
			}
		}
		var grade map[string]json.RawMessage
		_ = json.Unmarshal(r.Grade, &grade)
		for _, g := range gradeFields {
			cols = append(cols, scalar(grade[g]))
		}
		cols = append(cols, string(r.Grade))
		for i := range cols {
			cols[i] = tsvCell(cols[i])
		}
		fmt.Fprintln(bw, strings.Join(cols, "\t"))
	}
	return bw.Flush()
}

func stamp(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// scalar prints a JSON scalar bare (strings unquoted) and anything structured as compact JSON.
func scalar(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	if string(raw) == "null" {
		return ""
	}
	return string(raw)
}

func tsvCell(s string) string {
	return strings.NewReplacer("\t", " ", "\n", " ", "\r", " ").Replace(s)
}

// Results reads the rows of a plan directory in plan order.
func Results(dir string, f RowFilter) ([]Row, int, error) {
	rows, skipped, err := Rows(dir)
	if err != nil {
		return nil, skipped, err
	}
	order := map[string]int{}
	if st, err := LoadState(dir); err == nil {
		for _, n := range st.Nodes {
			order[n.ID] = n.Index
		}
	}
	return Select(rows, f, order), skipped, nil
}

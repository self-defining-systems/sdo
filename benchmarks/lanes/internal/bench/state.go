package bench

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Run statuses (run.json). A run without a run.json is queued (or running on a lane).
const (
	StatusDone    = "done"
	StatusInvalid = "invalid"
	StatusDropped = "dropped"
)

// RunFile is <root>/<exp>/runs/<run-id>/run.json, written by the manager only.
type RunFile struct {
	ID            string     `json:"id"`
	Experiment    string     `json:"experiment"`
	Fault         string     `json:"fault"`
	Sequence      int        `json:"sequence"`
	Position      int        `json:"position"`
	Seed          int        `json:"seed"`
	Attempt       int        `json:"attempt"`
	Status        string     `json:"status"`
	Reason        string     `json:"reason,omitempty"`
	Lane          string     `json:"lane,omitempty"`
	Host          string     `json:"host,omitempty"`
	OperatorLabel string     `json:"operator_label"`
	Fuzzops       Version    `json:"fuzzops"`
	Started       time.Time  `json:"started"`
	Ended         time.Time  `json:"ended"`
	InjectAt      *time.Time `json:"inject_at,omitempty"`
	SettleEnd     *time.Time `json:"settle_end,omitempty"`
	Capped        bool       `json:"capped,omitempty"`
	// Isolation is what the run was measured under: the lane's cores and memory, whether the host was reserved, and the
	// OOM kills in the lane's nodes during the run (a run with kills is flagged: its measurement is suspect).
	Isolation *Isolation `json:"isolation,omitempty"`
}

// Version is what `fuzzops version` prints.
type Version struct {
	Workload       string `json:"workload,omitempty"` // the workload's own id, when it says (identity verb)
	Revision       string `json:"revision"`
	Modified       bool   `json:"modified,omitempty"`
	MeasureVersion int    `json:"measure_version"`
}

// Lane states shown to the user; everything else is an interval in the lane's timeline.
const (
	LaneFree     = "free"
	LaneBuilding = "building"
	LaneReady    = "ready"
	LaneRunning  = "running"
	LaneDown     = "down"
	// LaneRetired: the lane stayed down through its automatic resets (or left hosts.yaml); nothing uses it any more.
	// The state is the last one in bench.log, so lane-time totals stop at it. Reset by hand to bring it back.
	LaneRetired = "retired"
	// LaneHeld: the run's evidence is incomplete. The lane is neither recycled nor released (its cluster still holds
	// what the evidence lacks) until the manifest lists no problems (a fix or a waiver).
	LaneHeld = "held"
	// LaneUnreachable: the lane's host stopped answering for longer than hostGrace. Its step (Orphan) is given up and
	// its item went back to the queue; the lane is recovered, as every lane after a lost step is, once the host is
	// admitted again.
	LaneUnreachable = "unreachable"
)

// LaneFile is <root>/lanes/<cluster>.json, written by the manager only.
type LaneFile struct {
	Lane       string    `json:"lane"`
	Host       string    `json:"host"`
	State      string    `json:"state"`
	Since      time.Time `json:"since"`
	Reason     string    `json:"reason,omitempty"`
	Experiment string    `json:"experiment,omitempty"`
	Seq        *Seq      `json:"seq,omitempty"`
	Attempt    int       `json:"attempt,omitempty"`
	Position   int       `json:"position,omitempty"` // the run in progress or next
	Step       *StepRef  `json:"step,omitempty"`
	// Orphan is a step given up because its host stopped answering. It may still run there: when the host answers
	// again it is killed, cleaned up and the lane is recovered before anything else uses it.
	Orphan *StepRef `json:"orphan,omitempty"`
	// Prepared is the operator last prepared on the lane (whatever
	// experiment): every build tears it down first.
	Prepared *Prepared `json:"prepared,omitempty"`
	// HeldRun is the run directory whose evidence manifest holds the lane (state held).
	HeldRun       string `json:"held_run,omitempty"`
	BuildFailures int    `json:"build_failures,omitempty"`
	GateFailures  int    `json:"gate_failures,omitempty"`
	// AutoResets counts the times the manager recreated the lane after it went down; a good build clears it.
	AutoResets int `json:"auto_resets,omitempty"`
	// Gone: the lane's cluster was deleted (the next build creates it). Recreate: the next build
	// deletes and creates it (an in-place reset failed or a fault left cluster-scoped residue).
	Gone     bool `json:"gone,omitempty"`
	Recreate bool `json:"recreate,omitempty"`
	// Unplaced: the lane's build waited for cores that were not there and was cancelled so that a free lane that
	// already holds cores could take its slot. The lane is offered last until a build on it succeeds.
	Unplaced bool `json:"unplaced,omitempty"`
	// GateRecreated: two failed gates already made the cluster anew (cleared by a done run).
	GateRecreated bool `json:"gate_recreated,omitempty"`
	// Transcripts is the last failed transcripts check of a teardown on the lane.
	Transcripts string `json:"transcripts,omitempty"`
	// Idle says why the free lane was not bound at the manager's last pass while sequences were queued: each reason
	// with the experiments it held back ("" when free with nothing queued, or busy). IdleLogged is when the log last said it.
	Idle       string    `json:"idle,omitempty"`
	IdleLogged time.Time `json:"idle_logged,omitempty"`
}

// StepRef is the step running (or finished, not yet adopted) on a lane.
type StepRef struct {
	ID      string    `json:"id"`
	Kind    string    `json:"kind"`
	RunID   string    `json:"run_id,omitempty"`
	Run     *RunSpec  `json:"run,omitempty"`
	Started time.Time `json:"started"`
	// Phase is what the step was doing at the manager's last pass (nil: not known).
	Phase *Phase `json:"phase,omitempty"`
	// Bound is how long the step may run before the manager kills it (0: no bound recorded); Killed says why it was.
	Bound         Seconds `json:"bound,omitempty"`
	Killed        string  `json:"killed,omitempty"`
	DeleteCluster bool    `json:"delete_cluster,omitempty"`
}

// Prepared is an operator set up on a lane and how to tear it down, plus the app copy it runs on.
type Prepared struct {
	Experiment string `json:"experiment"`
	Hooks      string `json:"hooks,omitempty"`
	HooksPath  string `json:"hooks_path,omitempty"`
	Command    string `json:"command,omitempty"`
	Operator   string `json:"operator,omitempty"` // the BENCH_OPERATOR file on the host
	Namespace  string `json:"namespace"`
	AppPack    string `json:"app_pack"` // the fuzzops pack (absolute, on the host)
	// Workload and WorkloadRev are the workload adapter and the pinned workload checkout (revision) the lane was prepared
	// with. Steps without an experiment (release, recover, unhold) take both from here: a lane is never handled by
	// a guess among the host's checkouts. WorkloadRev empty: nothing recorded.
	Workload    string `json:"workload,omitempty"`
	WorkloadRev string `json:"workload_rev,omitempty"`
	Pack        string `json:"pack,omitempty"`  // the attached pack prepare returned
	Ready       bool   `json:"ready,omitempty"` // the build finished and passed its check
	// CheckedAt is when the build check passed: a run within gateFreshness of it needs only a short ready gate.
	CheckedAt time.Time `json:"checked_at,omitempty"`
	Fault     string    `json:"fault,omitempty"` // the fault of the sequence this build was made for
	Version   string    `json:"operator_version,omitempty"`
	Memory    string    `json:"memory,omitempty"`
	// EvidenceRun is the operator's sink run directory under the lane's evidence directory.
	EvidenceRun string `json:"evidence_run,omitempty"`
	// Golden: the hooks' snapshot was taken once the build check passed (a held lane), so the lane can return to this
	// state by the hooks' reset. Reset: the lane did, after a claimed run: its operator is prepared and idle but
	// runs the sources of the last claimant; the next build of the same profile takes it by swap.
	Golden bool `json:"golden,omitempty"`
	Reset  bool `json:"reset,omitempty"`
	// Pool is the hold experiment whose golden copy the lane holds: set when the pool builds the lane and carried by
	// every swap, reset and restore, so the lane stays its pool's whatever claimant ran on it last (and whether that
	// claimant still exists). Empty in lane files written before it: the pool is then read from Experiment.
	Pool string `json:"pool,omitempty"`
}

// Experiment is everything on disk about one experiment.
type Experiment struct {
	Name string
	// Spec is the current spec file (nil once removed); SpecErr says why it is not used as is.
	Spec    *Spec
	SpecErr string
	Lock    Lock
	Paused  bool
	Runs    map[string]RunFile
	// Removed is what `remove` recorded (nil: not removed, or removed before removals were recorded).
	Removed *Removal
}

// Removal is <exp>/removed.json: when the experiment was removed and whether its lanes keep their clusters.
type Removal struct {
	At time.Time `json:"at"`
	// KeepLanes: the spec kept its lanes (keep_lanes) and the removal did not ask for a teardown.
	KeepLanes bool `json:"keep_lanes"`
	Teardown  bool `json:"teardown,omitempty"`
	// Cancel: the removal also stops the experiment's runs in flight (`lanes plan remove --cancel`, `plan skip
	// --cancel`); without it, and without Teardown, a run on a lane finishes.
	Cancel bool `json:"cancel,omitempty"`
}

// Root is the results directory.
type Root string

func (r Root) path(p ...string) string        { return filepath.Join(append([]string{string(r)}, p...)...) }
func (r Root) SpecPath(name string) string    { return r.path("specs", name+".yaml") }
func (r Root) PausedPath(name string) string  { return r.path("specs", name+".paused") }
func (r Root) LockPath(name string) string    { return r.path(name, "spec.lock.yaml") }
func (r Root) RemovedPath(name string) string { return r.path(name, "removed.json") }
func (r Root) RunDir(exp, id string) string   { return r.path(exp, "runs", id) }
func (r Root) LanePath(lane string) string    { return r.path("lanes", lane+".json") }
func (r Root) TimelinePath(lane string) string {
	return r.path("lanes", lane+".timeline.jsonl")
}
func (r Root) GatesPath(exp string) string { return r.path(exp, "gates.jsonl") }
func (r Root) WavesPath(exp string) string { return r.path(exp, "waves.json") }

// Experiments lists every experiment with a lock (added), sorted.
func (r Root) Experiments() ([]string, error) {
	ents, err := os.ReadDir(string(r))
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range ents {
		if _, err := os.Stat(r.LockPath(e.Name())); e.IsDir() && err == nil {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out, nil
}

// LoadExperiment reads an experiment's lock, current spec and runs.
func (r Root) LoadExperiment(name string) (*Experiment, error) {
	x := &Experiment{Name: name, Runs: map[string]RunFile{}}
	if err := readJSON(r.LockPath(name), &x.Lock); err != nil {
		return nil, err
	}
	if _, err := os.Stat(r.SpecPath(name)); err == nil {
		s, err := LoadSpec(r.SpecPath(name))
		switch {
		case err != nil:
			x.SpecErr = err.Error()
			x.Spec = &x.Lock.Spec
		case s.Name != name:
			x.SpecErr = fmt.Sprintf("name changed to %q (refused)", s.Name)
			x.Spec = &x.Lock.Spec
		default:
			if d := s.IdentityDiff(x.Lock); len(d) > 0 {
				x.SpecErr = "edit refused (identity): " + strings.Join(d, ", ")
				// The live fields still apply (all of them: keep_lanes, reuse_app, ...); identity stays as locked.
				s = s.withIdentityOf(x.Lock.Spec)
			}
			x.Spec = &s
		}
	}
	_, err := os.Stat(r.PausedPath(name))
	x.Paused = err == nil
	if x.Spec == nil {
		var rm Removal
		if readJSON(r.RemovedPath(name), &rm) == nil {
			x.Removed = &rm
		}
	}
	ents, err := os.ReadDir(r.path(name, "runs"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	for _, e := range ents {
		var rf RunFile
		if err := readJSON(filepath.Join(r.RunDir(name, e.Name()), "run.json"), &rf); err == nil {
			x.Runs[rf.ID] = rf
		}
	}
	return x, nil
}

// Measured is the spec the experiment measures by: the lock's identity with the current live fields.
func (x *Experiment) Measured() Spec {
	if x.Spec != nil {
		return *x.Spec
	}
	return x.Lock.Spec
}

// Sequence states.
const (
	SeqQueued  = "queued"  // no run of the current attempt has ended
	SeqStarted = "started" // some runs of the current attempt are done
	SeqDone    = "done"
	SeqInvalid = "invalid" // an attempt broke and no retry is left
	SeqDropped = "dropped" // removed (its fault or its experiment) before it ended
)

// Progress is where one sequence stands.
type Progress struct {
	Seq     Seq
	State   string
	Attempt int    // the current (or last) attempt
	Next    int    // the next position to run (0 when ended)
	Reason  string // why it is invalid
}

// Ended reports whether nothing more of the sequence will run.
func (p Progress) Ended() bool {
	return p.State == SeqDone || p.State == SeqInvalid || p.State == SeqDropped
}

// Progress works out where q stands from its run files: the queue is never stored.
func (x *Experiment) Progress(q Seq) Progress {
	s := x.Measured()
	seeds := s.Seeds(q.Q)
	last := 1
	byAttempt := map[int]map[int]RunFile{}
	for _, rf := range x.Runs {
		if rf.Fault != q.Fault || rf.Sequence != q.Q {
			continue
		}
		if byAttempt[rf.Attempt] == nil {
			byAttempt[rf.Attempt] = map[int]RunFile{}
		}
		byAttempt[rf.Attempt][rf.Position] = rf
		last = max(last, rf.Attempt)
	}
	p := Progress{Seq: q, Attempt: last}
	runs := byAttempt[last]
	for pos := 1; pos <= len(seeds); pos++ {
		rf, ok := runs[pos]
		switch {
		case !ok:
			p.Next = pos
		case rf.Status == StatusDone:
			continue
		case rf.Status == StatusDropped:
			return Progress{Seq: q, State: SeqDropped, Attempt: last}
		default:
			if last <= s.Retries {
				return Progress{Seq: q, State: SeqQueued, Attempt: last + 1, Next: 1}
			}
			return Progress{Seq: q, State: SeqInvalid, Attempt: last, Reason: rf.Reason}
		}
		break
	}
	switch {
	case p.Next == 0:
		p.State = SeqDone
	case p.Next == 1:
		p.State = SeqQueued
	default:
		p.State = SeqStarted
	}
	if !x.wants(q.Fault) {
		p.State, p.Next = SeqDropped, 0
	}
	return p
}

// wants reports whether the current spec still lists fault.
func (x *Experiment) wants(fault string) bool {
	if x.Spec == nil {
		return false
	}
	for _, f := range x.Spec.Faults.IDs {
		if f == fault {
			return true
		}
	}
	return false
}

// AllSeqs is every sequence the experiment ever implied (the lock's faults
// and the current spec's), so a removed fault's sequences still show.
func (x *Experiment) AllSeqs() []Seq {
	s := x.Measured()
	ids := append([]string(nil), s.Faults.IDs...)
	seen := map[string]bool{}
	for _, f := range ids {
		seen[f] = true
	}
	for _, rf := range x.Runs {
		if !seen[rf.Fault] {
			seen[rf.Fault] = true
			ids = append(ids, rf.Fault)
		}
	}
	for _, f := range x.Lock.Spec.Faults.IDs {
		if !seen[f] {
			seen[f] = true
			ids = append(ids, f)
		}
	}
	var out []Seq
	for _, f := range ids {
		for q := range s.Sequences {
			out = append(out, Seq{f, q + 1})
		}
	}
	return out
}

// Counts are an experiment's runs by status.
type Counts struct {
	Queued, Running, Done, Invalid, Dropped int
	Reasons                                 map[string]int // invalid runs by reason
}

// Count tallies the experiment's runs; running are the runs lanes hold now.
func (x *Experiment) Count(running int) Counts {
	c := Counts{Reasons: map[string]int{}, Running: running}
	for _, rf := range x.Runs {
		switch rf.Status {
		case StatusDone:
			c.Done++
		case StatusInvalid:
			c.Invalid++
			c.Reasons[reasonClass(rf.Reason)]++
		case StatusDropped:
			c.Dropped++
		}
	}
	s := x.Measured()
	for _, q := range x.AllSeqs() {
		p := x.Progress(q)
		left := 0
		if !p.Ended() {
			left = len(s.Seeds(q.Q)) - p.Next + 1
		}
		if p.State == SeqDropped {
			c.Dropped += len(s.Seeds(q.Q)) - x.endedIn(q, p.Attempt)
		}
		c.Queued += left
	}
	c.Queued = max(c.Queued-running, 0)
	return c
}

// endedIn counts the runs of q's attempt that have a run.json.
func (x *Experiment) endedIn(q Seq, attempt int) int {
	n := 0
	for _, rf := range x.Runs {
		if rf.Fault == q.Fault && rf.Sequence == q.Q && rf.Attempt == attempt {
			n++
		}
	}
	return n
}

// reasonClass is the head of a reason ("capped: reflection queued" -> "capped").
func reasonClass(r string) string {
	h, _, _ := strings.Cut(r, ":")
	return strings.TrimSpace(h)
}

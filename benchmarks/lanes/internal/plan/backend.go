package plan

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrExists is what Backend.Submit answers for an experiment that already exists: a restart between the submit and
// the state write finds its own submission and adopts it.
var ErrExists = errors.New("experiment exists")

// Clock is the engine's time. The runner passes the wall clock; tests pass a fake.
type Clock interface{ Now() time.Time }

// Phases of an experiment as the backend sees it. Absent: the backend knows no such experiment.
const (
	PhaseAbsent  = "absent"
	PhaseQueued  = "queued"
	PhaseRunning = "running"
	PhaseDone    = "done"
	PhaseFailed  = "failed"
)

// Job is one experiment to release.
type Job struct {
	Plan     string
	Node     string
	Exp      string
	SpecPath string // the rendered spec file
}

// Observation is how an experiment stands. A failed one carries Reason, a short stable class ("oom", "capped", ...):
// the plan counts failures per (item, reason).
type Observation struct {
	Phase   string
	Reason  string
	Detail  string
	Started time.Time
	Ended   time.Time
	// Stage is the stage a running experiment is in and StageSince when it entered it; Stages are the durations of the
	// stages already passed. Stage names are the backend's; a plan sets a minimum for the ones it watches.
	Stage      string
	StageSince time.Time
	Stages     map[string]time.Duration
}

// Isolation is what a run was measured under.
type Isolation struct {
	Mode     string `json:"mode,omitempty"` // two-way, one-way, one-way-ok; "" when the host has no reservation record
	OOMKills int    `json:"oom_kills,omitempty"`
	Reserved bool   `json:"reserved,omitempty"`
	CPUs     string `json:"cpus,omitempty"`
	Memory   string `json:"memory,omitempty"`
}

// Layout is what an EKS lane ran on.
type Layout struct {
	Nodes        int    `json:"nodes"`
	InstanceType string `json:"instance_type"`
	VCPUs        int    `json:"vcpus"` // all nodes together
	// OneNode: the host put the lane on one node (eks.one_node_lanes).
	OneNode bool `json:"one_node,omitempty"`
}

// String is "2 x m6i.2xlarge" ("" for nil).
func (l *Layout) String() string {
	if l == nil {
		return ""
	}
	return fmt.Sprintf("%d x %s", l.Nodes, l.InstanceType)
}

// RunData is what the backend knows of the newest run of an experiment, for the results row. Identity (Run, the
// observed revisions, Isolation) is there as soon as the run ended; the grade may take longer (Pending).
type RunData struct {
	Run string
	// Position is the run's place (1-based) among the runs of an experiment that ran several on one lane, in order; 0
	// for an experiment that ran one.
	Position int
	Host     string
	Lane     string
	// WorkloadObserved and SubjectObserved are the revisions the run reports having used (empty: not reported); App
	// is the application source identity (pack and image digests or commits, as the adapters name them).
	WorkloadObserved string
	SubjectObserved  string
	App              string
	Isolation        Isolation
	// Layout is what the lane ran on (nil: not an EKS lane).
	Layout *Layout
	// Grade is the workload's grade of the run: opaque JSON, an object with at least a boolean "valid". Nil with
	// GradeError empty and Pending false: the run was not graded (a failed run).
	Grade      []byte
	GradeError string
	Pending    bool
}

// LaneInfo is a lane as the alarms see it.
type LaneInfo struct {
	Host  string
	Lane  string
	Free  bool // no step, not bound to a run, ready for work
	Since time.Time
}

// HostInfo is a host's pressure against its own budget (0 budget: not gated).
type HostInfo struct {
	Name     string
	Load5    float64
	MaxLoad5 float64
	Free     int64 // bytes free on the staging disk
	MinFree  int64 // bytes the host must keep free (0: none)
	Error    string
}

// Capacity is what the alarms read of the platform: its lanes, its hosts and how many runs wait for a lane.
type Capacity struct {
	Lanes   []LaneInfo
	Hosts   []HostInfo
	Waiting int
}

// Source names what an artifact comes from: a node's experiment, or a step instance.
type Source struct {
	ID   string
	Exp  string // the node's experiment; "" for a step
	Step bool
	Dir  string // a step's working directory
}

// Artifact is an exported artifact: where it is.
type Artifact struct {
	Kind string
	Path string
}

// Verdict is the answer of a verify: OK, or the reason it is not.
type Verdict struct {
	OK     bool
	Detail string
}

// StepJob starts a lane-free step. Inputs lists what the step runs over: the done nodes of its After arms with their
// artifact paths. The command gets them as JSON in LANES_INPUTS, its directory in STEP_DIR and the plan's in PLAN_DIR.
type StepJob struct {
	Plan    string
	ID      string
	Name    string
	Run     string
	Dir     string
	Timeout time.Duration
	Inputs  []StepInput
}

// StepInput is one done node a step takes.
type StepInput struct {
	Node      string            `json:"node"`
	Arm       string            `json:"arm"`
	Item      string            `json:"item"`
	Rep       int               `json:"rep"`
	Exp       string            `json:"exp"`
	Artifacts map[string]string `json:"artifacts,omitempty"`
}

// StepObservation: Phase is PhaseRunning, PhaseDone, PhaseFailed or PhaseAbsent (never started).
type StepObservation struct {
	Phase  string
	Detail string
}

// Backend is everything the engine needs from the platform, so the engine is tested against a fake and runs on the
// manager's experiments in production. Every method is idempotent: a restarted engine repeats any of them.
type Backend interface {
	// Submit releases an experiment from a rendered spec.
	Submit(ctx context.Context, j Job) error
	Observe(ctx context.Context, exp string) (Observation, error)
	// Export resolves the artifact of a done source (path template: the plan's artifact definition).
	Export(ctx context.Context, src Source, kind, pathTemplate string) (Artifact, error)
	// Verify asks the mode (exists, subject, cmd:<shell>) whether the artifact is good. A returned error is an
	// infrastructure failure (retried next pass); a not-OK verdict is a verdict.
	Verify(ctx context.Context, src Source, a Artifact, mode string) (Verdict, error)
	StartStep(ctx context.Context, j StepJob) error
	// ObserveStep reads the step started in dir.
	ObserveStep(ctx context.Context, dir string) (StepObservation, error)
	// RunData reads the newest run of an experiment for its results row.
	RunData(ctx context.Context, exp string) (RunData, error)
	// RunsData reads every done run of an experiment, in run order, for its results rows: one element for an
	// experiment that ran once, several (Position set) for one that ran several runs on one lane.
	RunsData(ctx context.Context, exp string) ([]RunData, error)
	// Cancel stops an experiment: its queued runs are dropped and it is not observed again. Cancelling an experiment
	// that is gone is not an error.
	Cancel(ctx context.Context, exp string) error
	// CancelStep stops a running step started in dir.
	CancelStep(ctx context.Context, dir string) error
	// Capacity reads lanes, hosts and the queue for the alarms.
	Capacity(ctx context.Context) (Capacity, error)
}

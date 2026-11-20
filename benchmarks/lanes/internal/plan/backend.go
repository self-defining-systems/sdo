package plan

import (
	"context"
	"errors"
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
}

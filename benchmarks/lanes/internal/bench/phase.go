package bench

import (
	"bytes"
	"path/filepath"
	"strings"
	"time"
)

// Phase is what a running step is doing now: <step dir>/phase.json, rewritten
// whenever it changes. The probe reports it and the manager keeps it in the
// lane file (StepRef.Phase), so `status` and the dashboard can say whether a
// lane is gating, in its window or undoing. Display only: nothing decides on it.
type Phase struct {
	Name   string    `json:"phase"`
	Since  time.Time `json:"since"`
	Detail string    `json:"detail,omitempty"`
}

// Phase names, besides the hook verbs (prepare, teardown, transcripts, collect)
// and the shell commands (cluster-up, cluster-down).
const (
	PhaseRecover   = "recover"
	PhaseImages    = "images"
	PhaseApp       = "app"
	PhaseAppHealth = "app-health"
	PhaseCheck     = "check"
	PhaseGate      = "gate"   // the run's ready gate
	PhaseWindow    = "window" // injected: the operator's window
	PhaseUndo      = "undo"   // the fault's undo, the tail and the verify
	PhaseSettle    = "settle"
	PhaseSafety    = "safety"
	PhaseResidue   = "residue"
	// PhaseLaneWait: the build waits for the lane's cores or memory on its host (detail: who holds what).
	PhaseLaneWait = "lane-wait"
)

const phaseFile = "phase.json"

// phase records that the step entered a phase (best effort).
func (r *stepRunner) phase(name, detail string) {
	_ = writeJSON(filepath.Join(r.dir, phaseFile), Phase{Name: name, Since: time.Now().UTC(), Detail: detail})
}

// runPhase classifies one line of `fuzzops run` output ("[ns] inject ...");
// "" for a line that starts no phase.
func runPhase(line string) string {
	_, rest, ok := strings.Cut(line, "] ")
	if !ok {
		return ""
	}
	switch {
	case strings.HasPrefix(rest, "ready gate"):
		return PhaseGate
	case strings.HasPrefix(rest, "inject "):
		return PhaseWindow
	case strings.HasPrefix(rest, "undo "):
		return PhaseUndo
	}
	return ""
}

// phaseWriter watches `fuzzops run` output for the lines that start a phase.
type phaseWriter struct {
	r    *stepRunner
	part []byte
	last string
}

func (w *phaseWriter) Write(p []byte) (int, error) {
	w.part = append(w.part, p...)
	for {
		i := bytes.IndexByte(w.part, '\n')
		if i < 0 {
			break
		}
		line := string(w.part[:i])
		w.part = w.part[i+1:]
		if ph := runPhase(line); ph != "" && ph != w.last {
			w.last = ph
			w.r.phase(ph, "")
		}
	}
	return len(p), nil
}

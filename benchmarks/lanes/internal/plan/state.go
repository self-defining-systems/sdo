package plan

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// Node statuses. A node is released (submitted) once, then ends done, failed, or skipped (an input never came).
const (
	NodeWaiting   = "waiting"
	NodeSubmitted = "submitted"
	NodeDone      = "done"
	NodeFailed    = "failed"
	NodeSkipped   = "skipped"
	// NodeParked: the item failed twice with the same reason; nothing more of it runs.
	NodeParked = "parked"
)

// Step statuses.
const (
	StepWaiting = "waiting"
	StepRunning = "running"
	StepDone    = "done"
	StepFailed  = "failed"
	StepSkipped = "skipped"
)

// ArtifactState is one artifact of a done source: exported, and whether a verify passed.
type ArtifactState struct {
	Path     string    `json:"path"`
	Verified bool      `json:"verified"`
	Detail   string    `json:"detail,omitempty"`
	At       time.Time `json:"at"`
}

// NodeState is the state of one node.
type NodeState struct {
	ID      string `json:"id"`
	Arm     string `json:"arm"`
	Item    string `json:"item"`
	Rep     int    `json:"rep"`
	Index   int    `json:"index"`
	Status  string `json:"status"`
	Attempt int    `json:"attempt"`
	Exp     string `json:"exp,omitempty"`
	Reason  string `json:"reason,omitempty"`
	// Submitted, Ended: when the current attempt was released and when it ended.
	Submitted time.Time                 `json:"submitted,omitempty"`
	Ended     time.Time                 `json:"ended,omitempty"`
	Artifacts map[string]*ArtifactState `json:"artifacts,omitempty"`
	// Attempts are the ended attempts, oldest first; the current one is in the fields above.
	Attempts []AttemptRecord `json:"attempts,omitempty"`
}

// AttemptRecord is one ended attempt of a node: how it ended and how long each stage took.
type AttemptRecord struct {
	Attempt   int                      `json:"attempt"`
	Exp       string                   `json:"exp"`
	Reason    string                   `json:"reason,omitempty"` // empty: the attempt is done
	Detail    string                   `json:"detail,omitempty"`
	Submitted time.Time                `json:"submitted"`
	Ended     time.Time                `json:"ended"`
	Stages    map[string]time.Duration `json:"stages,omitempty"`
}

// StepState is the state of one step instance.
type StepState struct {
	ID        string                    `json:"id"`
	Name      string                    `json:"name"`
	Rep       int                       `json:"rep,omitempty"`
	Status    string                    `json:"status"`
	Detail    string                    `json:"detail,omitempty"`
	Started   time.Time                 `json:"started,omitempty"`
	Ended     time.Time                 `json:"ended,omitempty"`
	Artifacts map[string]*ArtifactState `json:"artifacts,omitempty"`
}

// State is <plan dir>/state.json. Everything the engine knows lives here or in the experiments; a restart rebuilds
// nothing.
type State struct {
	Version  int          `json:"version"`
	Nodes    []*NodeState `json:"nodes"`
	Steps    []*StepState `json:"steps,omitempty"`
	Complete bool         `json:"complete,omitempty"`
	// Failures counts failed attempts per item and reason ("item|reason"); Parked maps a parked item to its reason.
	Failures map[string]int    `json:"failures,omitempty"`
	Parked   map[string]string `json:"parked,omitempty"`
	Alarms   AlarmState        `json:"alarms,omitempty"`
}

const stateVersion = 1

func (e *Engine) newState(nodes []Node) *State {
	st := &State{Version: stateVersion}
	for _, n := range nodes {
		st.Nodes = append(st.Nodes, &NodeState{ID: n.ID, Arm: n.Arm, Item: n.Item, Rep: n.Rep, Index: n.Index, Status: NodeWaiting})
	}
	for _, si := range e.Plan.StepInstances() {
		st.Steps = append(st.Steps, &StepState{ID: si.ID, Name: si.Name, Rep: si.Rep, Status: StepWaiting})
	}
	return st
}

// LoadState reads a plan directory's state.
func LoadState(dir string) (*State, error) {
	var st State
	if err := readJSONFile(stateFile(dir), &st); err != nil {
		return nil, err
	}
	if st.Version != stateVersion {
		return nil, fmt.Errorf("%s: state version %d (this build reads %d)", stateFile(dir), st.Version, stateVersion)
	}
	return &st, nil
}

func (s *State) node(id string) *NodeState {
	for _, n := range s.Nodes {
		if n.ID == id {
			return n
		}
	}
	return nil
}

func (s *State) step(id string) *StepState {
	for _, n := range s.Steps {
		if n.ID == id {
			return n
		}
	}
	return nil
}

func terminalNode(status string) bool {
	return status == NodeDone || status == NodeFailed || status == NodeSkipped || status == NodeParked
}

// event is one line of events.jsonl.
type event struct {
	At     time.Time `json:"at"`
	Node   string    `json:"node,omitempty"`
	Event  string    `json:"event"`
	Detail string    `json:"detail,omitempty"`
}

func (e *Engine) event(node, ev, detail string, a ...any) {
	b, _ := json.Marshal(event{At: e.Clock.Now().UTC(), Node: node, Event: ev, Detail: fmt.Sprintf(detail, a...)})
	f, err := os.OpenFile(eventsFile(e.Dir), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.Write(append(b, '\n'))
}

// Package evaluation owns the JSON verdict the controller binary prints for
// --evaluate-once. The runtime writes it and the deployment health probe reads
// it; both use these types, so a field cannot be renamed on one side only.
package evaluation

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"sdo.dev/operator/sdk"
)

// Status is the bounded command verdict, not a persisted detector observation.
// Unavailable means this invocation cannot read an incident opener; Pending means a detector can run but its
// observation window is incomplete.
type Status string

const (
	Clear       Status = "clear"
	Firing      Status = "firing"
	Pending     Status = "pending"
	Unavailable Status = "unavailable"
)

// Valid reports whether s is a verdict status this contract defines.
func (s Status) Valid() bool {
	switch s {
	case Clear, Firing, Pending, Unavailable:
		return true
	}
	return false
}

// OutsideClusterReason prefixes the reason of a detector an evaluation run
// outside the cluster (--outside-cluster) leaves out because it needs the
// cluster's own network, such as one that dials Service names.
const OutsideClusterReason = "not evaluable outside the cluster: "

// UnavailableDetector names a detector that could not be evaluated and why.
type UnavailableDetector struct {
	DetectorID string `json:"detector_id"`
	Reason     string `json:"reason"`
	Clue       bool   `json:"clue,omitempty"`
}

// DetectorState is one detector observation that is not a plain clear: pending
// windows and any state carrying a reason. Its keys are Go-cased for
// compatibility with the runtime's persisted detector observations.
type DetectorState struct {
	DetectorID   string    `json:"DetectorID"`
	EvaluatedAt  time.Time `json:"EvaluatedAt"`
	Status       string    `json:"Status"`
	Fingerprints []string  `json:"Fingerprints"`
	Error        string    `json:"Error"`
	Reason       string    `json:"Reason,omitempty"`
	Clue         bool      `json:"Clue,omitempty"`
}

// DetectorProgress says how far one detector has judged the application since
// the evaluation began: how often it judged, how many of those judgements came
// at least its own Horizon after its first, and the Interval and Horizon (how
// long after a condition resolves a fresh judgement may still report it) it
// runs with. A reader that wants "judged N times past its horizon" reads
// PastHorizon.
type DetectorProgress struct {
	DetectorID  string        `json:"detector_id"`
	Interval    time.Duration `json:"interval"`
	Horizon     time.Duration `json:"horizon"`
	Evaluations int           `json:"evaluations"`
	PastHorizon int           `json:"past_horizon"`
	FirstAt     time.Time     `json:"first_at"`
}

// Verdict is the whole --evaluate-once output.
type Verdict struct {
	Findings []sdk.Finding `json:"findings"`
	// ClueFindings indexes active findings that cannot open an incident.
	ClueFindings   []int                 `json:"clue_findings,omitempty"`
	Status         Status                `json:"status"`
	Unavailable    []UnavailableDetector `json:"unavailable,omitempty"`
	DetectorStates []DetectorState       `json:"detector_states,omitempty"`
	// Progress lists every detector that has judged at least once (watching
	// evaluations only).
	Progress []DetectorProgress `json:"progress,omitempty"`
	// ExistingRecordsExercised counts the probe samples in which a step marked
	// as reading existing records returned a record the probe did not write.
	ExistingRecordsExercised int `json:"existing_records_exercised,omitempty"`
	// StartedAt is when a watching evaluation (--watch) began judging this
	// line, so a reader can tell a verdict computed after an event from an
	// earlier one. A one-shot evaluation leaves it zero.
	StartedAt time.Time `json:"started_at,omitzero"`
	// EnvironmentWait is the longest probe-user provisioning run the
	// evaluation's probes are still waiting for; zero when none waits.
	EnvironmentWait time.Duration `json:"environment_wait,omitempty"`
}

// Decode parses the verdict strictly: an unknown field or status means the two
// sides of the contract have drifted and is an error, never silently dropped.
func Decode(stdout []byte) (Verdict, error) {
	var verdict Verdict
	decoder := json.NewDecoder(bytes.NewReader(stdout))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&verdict); err != nil {
		return Verdict{}, err
	}
	if !verdict.Status.Valid() {
		return Verdict{}, fmt.Errorf("unknown evaluation status %q", verdict.Status)
	}
	return verdict, nil
}

// GatingClasses are the detector classes whose verdict gates an upstream
// advance: the health objective and the designer's health detectors judge the
// candidate. Learned detectors (learned by reflection, responder-owned) are
// not part of the gate: they compare live state against a baseline the advance
// legitimately changes, and a real fault after the advance reaches them
// through ordinary detection.
var GatingClasses = []sdk.DetectorClass{sdk.DetectorClassHealth}

// ParseClasses decodes a comma-separated --classes value. The empty string is
// "every class" (nil). An unknown class is an error, never an empty selection.
func ParseClasses(value string) ([]sdk.DetectorClass, error) {
	if value == "" {
		return nil, nil
	}
	var classes []sdk.DetectorClass
	for _, name := range strings.Split(value, ",") {
		class := sdk.DetectorClass(strings.TrimSpace(name))
		if class != sdk.DetectorClassHealth && class != sdk.DetectorClassLearned {
			return nil, fmt.Errorf("unknown detector class %q", name)
		}
		classes = append(classes, class)
	}
	return classes, nil
}

// FormatClasses is ParseClasses' inverse.
func FormatClasses(classes []sdk.DetectorClass) string {
	names := make([]string, len(classes))
	for i, class := range classes {
		names[i] = string(class)
	}
	return strings.Join(names, ",")
}

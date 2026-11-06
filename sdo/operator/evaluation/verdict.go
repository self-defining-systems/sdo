// Package evaluation owns the JSON verdict the controller binary prints for
// --evaluate-once. The runtime writes it and the deployment health probe reads
// it; both use these types, so a field cannot be renamed on one side only.
package evaluation

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"sdo.dev/operator/sdk"
)

// Status is the bounded command verdict, not a persisted detector observation.
// Unavailable means this invocation lacks a required capability; Pending means
// a detector can run but its observation window is incomplete.
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

// UnavailableDetector names a detector that could not be evaluated and why.
type UnavailableDetector struct {
	DetectorID string `json:"detector_id"`
	Reason     string `json:"reason"`
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
}

// Verdict is the whole --evaluate-once output.
type Verdict struct {
	Findings       []sdk.Finding         `json:"findings"`
	Status         Status                `json:"status"`
	Unavailable    []UnavailableDetector `json:"unavailable,omitempty"`
	DetectorStates []DetectorState       `json:"detector_states,omitempty"`
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

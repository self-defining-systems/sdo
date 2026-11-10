package datacheck

import (
	"context"
	"sort"

	"sdo.dev/operator/sdk"
)

// Accepted is a view that carries the accepted data-check files, by check ID:
// a playbook's ReadView. Its text is the designer's YAML, so a program re-judges
// a check without embedding it.
type Accepted interface {
	DataChecks() (map[string]string, error)
}

// CheckVerdict is one accepted data check judged over a view's frozen reads.
// Findings are what the check's detector reports; Error is set when the check
// does not parse or its reads are missing or unusable, and Findings are then
// unknown, never clear.
type CheckVerdict struct {
	ID        string        `json:"id"`
	Component string        `json:"component,omitempty"`
	Lookup    string        `json:"lookup,omitempty"`
	Findings  []sdk.Finding `json:"findings,omitempty"`
	Error     string        `json:"error,omitempty"`
}

// Verdicts judges every accepted data check over the view's frozen pod reads,
// the same way their detectors do, sorted by check ID. It reads only frozen
// results and never fails: a view without accepted checks has no verdicts, and
// a check without a usable read is a verdict with Error.
func Verdicts(view sdk.DetectionContext) []CheckVerdict {
	accepted, ok := view.(Accepted)
	if !ok {
		return nil
	}
	checks, err := accepted.DataChecks()
	if err != nil {
		return []CheckVerdict{{Error: err.Error()}}
	}
	ids := make([]string, 0, len(checks))
	for id := range checks {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]CheckVerdict, 0, len(ids))
	for _, id := range ids {
		verdict := CheckVerdict{ID: id}
		if check, err := Parse([]byte(checks[id])); err == nil {
			verdict.Component = check.Component
			if check.Lookup != nil {
				verdict.Lookup = check.Lookup.Component
			}
		}
		findings, err := NewDetector(sdk.DetectorSpec{ID: "data-" + id}, id, checks[id]).Detect(context.Background(), view)
		if err != nil {
			verdict.Error = err.Error()
		} else {
			verdict.Findings = findings
		}
		out = append(out, verdict)
	}
	return out
}

// Firing reports whether the verdict judged the check violated.
func (v CheckVerdict) Firing() bool { return v.Error == "" && len(v.Findings) > 0 }

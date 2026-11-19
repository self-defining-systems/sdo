package sdktest

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/sdk/program"
)

// OwnIncidentOutcome is what running a new playbook's Diagnose on the incident
// it was written for says about it. There are exactly three: it recognises its
// fault, it provably does not, or the capture cannot say.
type OwnIncidentOutcome string

const (
	// OwnIncidentMatch: Diagnose returned Match on the captured open state.
	OwnIncidentMatch OwnIncidentOutcome = "match"
	// OwnIncidentFailed is a definite failure: Diagnose read everything it
	// asked for and returned NoMatch, so it cannot recognise the fault it was
	// written for. The only outcome that may block publication.
	OwnIncidentFailed OwnIncidentOutcome = "failed"
	// OwnIncidentUnverified: the capture cannot decide. A read it lacks or
	// holds an error for, a binding it cannot supply, or no capture at all.
	OwnIncidentUnverified OwnIncidentOutcome = "unverified"
)

// OwnIncidentResult is one playbook's own-incident replay verdict. Capture
// names the open@* snapshot the outcome comes from; Reason says why a
// failed or unverified outcome is one.
type OwnIncidentResult struct {
	ProgramID string             `json:"program_id"`
	Outcome   OwnIncidentOutcome `json:"outcome"`
	Capture   string             `json:"capture,omitempty"`
	Reason    string             `json:"reason,omitempty"`
}

// DiagnoseOnIncident runs d's Diagnose on the incident's captured open state:
// the controller's open@* snapshots in dir with their recorded reads, through
// the same RunDiagnose and read semantics the live stage uses. A learned
// playbook declares no object role, so it runs with no bindings; one that
// declares roles is unverified, because production binds them from findings the
// capture does not carry. A Match on any capture is a Match. Without one, a
// NoMatch on any capture is a failure; everything else is unverified.
func DiagnoseOnIncident(d program.Diagnoser, dir string) (OwnIncidentResult, error) {
	spec := d.Spec()
	result := OwnIncidentResult{ProgramID: spec.ID, Outcome: OwnIncidentUnverified}
	var roles []string
	for _, param := range spec.Params {
		if param.Kind != "" {
			roles = append(roles, param.Name)
		}
	}
	paths, err := filepath.Glob(filepath.Join(dir, "open@*.json"))
	if err != nil {
		return result, err
	}
	sort.Strings(paths)
	var captures []string
	for _, path := range paths {
		if !strings.HasSuffix(path, "-recordings.json") {
			captures = append(captures, path)
		}
	}
	if len(captures) == 0 {
		result.Reason = "no captured open state"
		return result, nil
	}
	if len(roles) > 0 {
		result.Capture = filepath.Base(captures[0])
		result.Reason = fmt.Sprintf("declares object roles (%s); production binds them from findings the capture does not carry", strings.Join(roles, ", "))
		return result, nil
	}
	var failed, unverified *OwnIncidentResult
	for _, path := range captures {
		raw, err := os.ReadFile(path)
		if err != nil {
			return result, err
		}
		snapshot, err := decodeSnapshotStrict(raw)
		if err != nil {
			return result, fmt.Errorf("incident capture %s: %w", filepath.Base(path), err)
		}
		c := CorpusCase{Open: snapshot}
		if recorded, readErr := os.ReadFile(strings.TrimSuffix(path, ".json") + "-recordings.json"); readErr == nil {
			if err := json.Unmarshal(recorded, &c.Recordings); err != nil {
				return result, fmt.Errorf("incident capture %s recordings: %w", filepath.Base(path), err)
			}
		}
		c.Open.SourceName = filepath.Base(path)
		verdict := program.RunDiagnose(context.Background(), d, &ReplayReadView{Snapshot: c.Open, Calls: c.PhaseRecordings("open")}, program.Bindings{}, 5*time.Second)
		here := OwnIncidentResult{ProgramID: spec.ID, Capture: filepath.Base(path)}
		switch verdict.Result {
		case program.Match:
			here.Outcome = OwnIncidentMatch
			return here, nil
		case program.NoMatch:
			here.Outcome, here.Reason = OwnIncidentFailed, "Diagnose returned NoMatch on the incident it was written for: "+evidenceSummary(verdict)
			if limit := captureLimits(snapshot); limit != "" {
				// What a capture withholds, a live read sees: the NoMatch may be the capture, not the playbook.
				here.Outcome, here.Reason = OwnIncidentUnverified, "Diagnose returned NoMatch, but the capture "+limit+": "+evidenceSummary(verdict)
			}
			if failed == nil {
				failed = &here
			}
		default:
			here.Outcome, here.Reason = OwnIncidentUnverified, fmt.Sprintf("Diagnose returned %s: %s", verdict.Result, evidenceSummary(verdict))
			if unverified == nil {
				unverified = &here
			}
		}
	}
	switch {
	case failed != nil:
		return *failed, nil
	case unverified != nil:
		return *unverified, nil
	}
	return result, nil
}

// evidenceSummary is a verdict's evidence as one short line: the facts Diagnose
// observed, or the error that made it Unknown.
func evidenceSummary(v program.Verdict) string {
	var parts []string
	for _, item := range v.Evidence {
		if len(parts) == 3 {
			break
		}
		parts = append(parts, strings.TrimSpace(fmt.Sprintf("%s: %s (expected %s)", item.Fact, item.Observed, item.Expected)))
	}
	if len(parts) == 0 {
		return "no evidence"
	}
	return strings.Join(parts, "; ")
}

// captureLimits says what a capture withholds that a live read sees, "" when it withholds nothing a Diagnose could
// depend on: ConfigMap values are redacted and objects over the snapshot budget are dropped.
func captureLimits(snapshot Snapshot) string {
	for _, configMap := range snapshot.ConfigMapList {
		if configMap.Annotations["sdo.dev/configmap-values-redacted"] == "true" {
			return "redacts ConfigMap values"
		}
	}
	for _, object := range snapshot.Objects {
		if object.GetKind() == "ConfigMap" && object.GetAnnotations()["sdo.dev/configmap-values-redacted"] == "true" {
			return "redacts ConfigMap values"
		}
	}
	if len(snapshot.TrimmedObjects) > 0 {
		return "dropped objects over its budget"
	}
	return ""
}

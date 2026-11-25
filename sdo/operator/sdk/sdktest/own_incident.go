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

	"sdo.dev/operator/sdk"
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
	Recovered *RecoveredMatch    `json:"recovered_match,omitempty"`
}

// RecoveredMatch records a positive diagnosis on the verified fixed snapshot.
type RecoveredMatch struct {
	Objects []sdk.ObjectRef `json:"objects,omitempty"`
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
		view := &ReplayReadView{Snapshot: c.Open, Calls: c.PhaseRecordings("open")}
		verdict := program.RunDiagnose(context.Background(), d, view, program.Bindings{}, 5*time.Second)
		here := OwnIncidentResult{ProgramID: spec.ID, Capture: filepath.Base(path)}
		switch verdict.Result {
		case program.Match:
			here.Outcome = OwnIncidentMatch
			return here, nil
		case program.NoMatch:
			here.Outcome, here.Reason = OwnIncidentFailed, "Diagnose returned NoMatch on the incident it was written for: "+evidenceSummary(verdict)+
				". A Diagnose must match its own incident before it is saved: fix its conditions, or keep the playbook README-only"
			if limit := captureLimits(snapshot, view.KindsRead()); limit != "" {
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

// DiagnoseOnRecovered replays fixed-boundary observations. Unknown means not evaluated.
func DiagnoseOnRecovered(ctx context.Context, d program.Diagnoser, dir string) (*RecoveredMatch, program.Result) {
	for _, param := range d.Spec().Params {
		if param.Kind != "" {
			return nil, program.Unknown // The capture does not carry role bindings.
		}
	}
	raw, err := os.ReadFile(filepath.Join(dir, "fixed.json"))
	if err != nil {
		return nil, program.Unknown
	}
	snapshot, err := decodeSnapshotStrict(raw)
	if err != nil {
		return nil, program.Unknown
	}
	var calls []PrimitiveCall
	for _, phase := range []string{"detect", "settle", "pre-apply"} {
		raw, err := os.ReadFile(filepath.Join(dir, "open@"+phase+"-recordings.json"))
		var capture CorpusCase
		if err == nil && json.Unmarshal(raw, &capture.Recordings) == nil {
			calls = append(calls, capture.PhaseRecordings("fixed")...)
		}
	}
	view := &ReplayReadView{Snapshot: snapshot, Calls: calls}
	verdict := program.RunDiagnose(ctx, d, view, program.Bindings{}, 500*time.Millisecond)
	if ctx.Err() != nil || view.hadReadFailure() || captureLimits(snapshot, view.KindsRead()) != "" {
		return nil, program.Unknown
	}
	if verdict.Result != program.Match {
		return nil, verdict.Result
	}
	inventory := map[string]sdk.ObjectRef{}
	for _, object := range append(sdk.TypedObjects(snapshot), snapshot.Objects...) {
		ref := sdk.ObjectIdentity(object)
		inventory[recoveredObjectName(ref)] = ref
	}
	note := &RecoveredMatch{}
	for _, evidence := range verdict.Evidence {
		ref := evidence.Resource
		name := recoveredObjectName(ref)
		captured, present := inventory[name]
		if !present || ref.APIVersion != "" && ref.APIVersion != captured.APIVersion ||
			len(name) > 120 || strings.ContainsAny(name, " \t\r\n") {
			continue
		}
		delete(inventory, name)
		note.Objects = append(note.Objects, captured)
		if len(note.Objects) == 12 {
			break
		}
	}
	if len(note.Objects) == 0 {
		return nil, program.Unknown
	}
	return note, program.Match
}

func recoveredObjectName(ref sdk.ObjectRef) string {
	return ref.Kind + "/" + ref.Namespace + "/" + ref.Name
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

// captureLimits says what the capture withholds from the kinds Diagnose read that a live read sees, "" when it
// withholds nothing Diagnose depended on: ConfigMap values are redacted and objects over the snapshot budget are
// dropped. A limit on a kind Diagnose never read cannot be why it returned NoMatch.
func captureLimits(snapshot Snapshot, read map[string]bool) string {
	if read["ConfigMap"] {
		for _, configMap := range snapshot.ConfigMapList {
			if configMap.Annotations["sdo.dev/configmap-values-redacted"] == "true" {
				return "redacts ConfigMap values, which Diagnose read"
			}
		}
		for _, object := range snapshot.Objects {
			if object.GetKind() == "ConfigMap" && object.GetAnnotations()["sdo.dev/configmap-values-redacted"] == "true" {
				return "redacts ConfigMap values, which Diagnose read"
			}
		}
	}
	var dropped []string
	for kind, count := range snapshot.TrimmedObjects {
		if count > 0 && read[kind] {
			dropped = append(dropped, kind)
		}
	}
	if len(dropped) > 0 {
		sort.Strings(dropped)
		return "dropped " + strings.Join(dropped, ", ") + " objects over its budget, which Diagnose read"
	}
	return ""
}

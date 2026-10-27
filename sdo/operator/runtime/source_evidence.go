package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// acceptanceUnavailableReason prefixes a recorded comparison whose acceptance
// record is missing: the declared-field result stays in the recording, the
// additions since acceptance are unknown.
const acceptanceUnavailableReason = "acceptance_record_unavailable"

// recordSourceComparisons is the one owner of accepted-source evidence in the
// incident capture. Every resource a write names, at whichever boundary names
// it, and every resource the agent asks the source of, gets its comparison
// recorded once per phase, so reflection can ground a source-reconcile program
// in it. The comparison is evidence or a typed reason, never absent: an object
// with no acceptance record keeps its declared-field comparison and says why
// the additions are unknown.
func (l *ResponderLoop) recordSourceComparisons(ctx context.Context, phase string, refs ...sdk.ObjectRef) {
	if l.CorpusSessionRoot == "" || l.ReadView == nil || l.Provider == nil {
		return
	}
	var view program.ReadView
	for _, ref := range refs {
		key := phase + "\x00" + fmt.Sprint(ref)
		if !sourceComparable(ref) || l.sourceCompared[key] {
			continue
		}
		if view == nil {
			snapshot, err := l.Provider.Snapshot(ctx)
			if err != nil {
				slog.Warn("source comparison evidence gap", "incident_id", l.Request.IncidentID, "error", err)
				return
			}
			view = l.ReadView(snapshot)
		}
		diff, err := view.DiffAgainstSource(ref)
		call, err := sourceComparisonCall(ref, diff, err)
		if err != nil {
			slog.Warn("source comparison evidence gap", "incident_id", l.Request.IncidentID, "resource", ref.Kind+"/"+ref.Name, "error", err)
			continue
		}
		if err := memory.AppendCapturedRecording(l.CorpusSessionRoot, l.Request.IncidentID, phase, call); err != nil {
			slog.Warn("source comparison evidence gap", "incident_id", l.Request.IncidentID, "resource", ref.Kind+"/"+ref.Name, "error", err)
			continue
		}
		if l.sourceCompared == nil {
			l.sourceCompared = map[string]bool{}
		}
		l.sourceCompared[key] = true
	}
}

// sourceComparable excludes objects accepted source never declares and the
// ones whose contents are not captured.
func sourceComparable(ref sdk.ObjectRef) bool {
	if ref.Name == "" {
		return false
	}
	switch strings.ToLower(ref.Kind) {
	case "", "pod", "pods", "event", "events", "node", "nodes", "secret", "secrets", "namespace", "namespaces":
		return false
	}
	return true
}

func sourceComparisonCall(ref sdk.ObjectRef, diff program.Diff, compareErr error) (sdktest.PrimitiveCall, error) {
	arguments, err := json.Marshal([]any{ref})
	if err != nil {
		return sdktest.PrimitiveCall{}, err
	}
	call := sdktest.PrimitiveCall{Method: "DiffAgainstSource", Arguments: arguments}
	var unknown *AcceptanceUnknownError
	switch {
	case errors.As(compareErr, &unknown):
		diff = unknown.Declared
		state := "match"
		if diff.Changed {
			state = "differ"
		}
		call.Error = fmt.Sprintf("%s: %v; declared fields %s", acceptanceUnavailableReason, ErrAcceptanceRecordUnavailable, state)
		if diff.Text != "" {
			call.Error += ": " + diff.Text
		}
	case compareErr != nil:
		call.Error = compareErr.Error()
	}
	if call.Result, err = json.Marshal(diff); err != nil {
		return sdktest.PrimitiveCall{}, err
	}
	return call, nil
}

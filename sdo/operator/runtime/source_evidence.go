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
//
// snapshot, when set, is the snapshot of the boundary being recorded; without
// one the comparison takes its own.
func (l *ResponderLoop) recordSourceComparisons(ctx context.Context, phase string, snapshot sdk.DetectionContext, refs ...sdk.ObjectRef) {
	if l.CorpusSessionRoot == "" || l.ReadView == nil || snapshot == nil && l.Provider == nil {
		return
	}
	defer l.phase("source_comparison")()
	pending := l.uncompared(phase, refs)
	if len(pending) == 0 {
		return
	}
	if snapshot == nil {
		// Evidence never delays a write for longer than a diagnosis may take.
		snapshotCtx, cancel := context.WithTimeout(ctx, l.deadline())
		endSnapshot := l.phase("source_comparison_snapshot")
		taken, err := l.Provider.Snapshot(snapshotCtx)
		endSnapshot()
		cancel()
		if err != nil {
			slog.Warn("source comparison evidence gap", "incident_id", l.Request.IncidentID, "error", err)
			return
		}
		snapshot = taken
	}
	calls, compared := sourceComparisons(l.Request.IncidentID, l.ReadView(snapshot), pending)
	if err := memory.AppendCapturedRecordings(l.CorpusSessionRoot, l.Request.IncidentID, phase, calls); err != nil {
		slog.Warn("source comparison evidence gap", "incident_id", l.Request.IncidentID, "error", err)
		return
	}
	l.markCompared(phase, compared)
}

// uncompared are the refs of phase whose comparison is not yet recorded.
func (l *ResponderLoop) uncompared(phase string, refs []sdk.ObjectRef) []sdk.ObjectRef {
	var pending []sdk.ObjectRef
	seen := map[string]bool{}
	for _, ref := range refs {
		key := phase + "\x00" + fmt.Sprint(ref)
		if !sourceComparable(ref) || l.sourceCompared[key] || seen[key] {
			continue
		}
		seen[key] = true
		pending = append(pending, ref)
	}
	return pending
}

func (l *ResponderLoop) markCompared(phase string, refs []sdk.ObjectRef) {
	if l.sourceCompared == nil {
		l.sourceCompared = map[string]bool{}
	}
	for _, ref := range refs {
		l.sourceCompared[phase+"\x00"+fmt.Sprint(ref)] = true
	}
}

// sourceComparisons compares each ref with accepted source on view. It reads
// no loop state, so a boundary may run it beside its other reads.
func sourceComparisons(incidentID string, view program.ReadView, refs []sdk.ObjectRef) (calls []sdktest.PrimitiveCall, compared []sdk.ObjectRef) {
	for _, ref := range refs {
		diff, err := view.DiffAgainstSource(ref)
		call, err := sourceComparisonCall(ref, diff, err)
		if err != nil {
			slog.Warn("source comparison evidence gap", "incident_id", incidentID, "resource", ref.Kind+"/"+ref.Name, "error", err)
			continue
		}
		calls = append(calls, call)
		compared = append(compared, ref)
	}
	return calls, compared
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

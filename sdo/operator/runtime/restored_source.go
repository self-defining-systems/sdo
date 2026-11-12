package runtime

import (
	"context"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// sourceRestore is what a plan's writes looked like against accepted source
// just before it applied: each written object and whether it was absent from
// source. ok is false when the plan cannot prove a restore by its own receipt:
// it writes a Secret (Secret values are never diffed), an object an earlier
// SDO write of this incident touched (the departure may be SDO's own), an
// object that did not depart from source, a departure that was not present
// when the incident opened, or anything Go cannot name as one object.
type sourceRestore struct {
	ok      bool
	written map[sdk.ObjectRef]bool
}

// sourceRestoreBefore reads, before plan applies, whether every object it
// writes departs from accepted source with a departure the incident opened
// on: an opening drift finding on the object, or the object in the
// configuration diff captured at open.
func (l *ResponderLoop) sourceRestoreBefore(ctx context.Context, plan program.Plan) sourceRestore {
	restore := sourceRestore{written: map[sdk.ObjectRef]bool{}}
	if len(plan.Actions) == 0 {
		return restore
	}
	var refs []sdk.ObjectRef
	for _, action := range plan.Actions {
		ref, err := planActionRef(action)
		if err != nil || ref.Kind == "Secret" || ref.Name == "" {
			return restore
		}
		refs = append(refs, ref)
	}
	earlier := map[sdk.ObjectRef]bool{}
	for _, entries := range [][]RecordedPlanApplication{l.archivedApplications, l.applicationsLog} {
		for _, entry := range entries {
			for _, effect := range entry.Result.ObjectEffects {
				if effect.State != "not_applied" {
					earlier[objectKey(effect.Ref)] = true
				}
			}
		}
	}
	view, err := l.snapshotView(ctx)
	if err != nil {
		return restore
	}
	for _, ref := range refs {
		if earlier[objectKey(ref)] || !departedAtOpen(l.Request, ref) {
			return restore
		}
		diff, err := view.DiffAgainstSource(ref)
		if err != nil || !diff.Changed {
			return restore
		}
		restore.written[objectKey(ref)] = diff.SourceAbsent
	}
	restore.ok = true
	return restore
}

// restored reports, after the plan applied, that every object it wrote now
// matches accepted source, or, when it was absent from source, is gone.
func (r sourceRestore) restored(ctx context.Context, l *ResponderLoop) bool {
	if !r.ok || len(r.written) == 0 {
		return false
	}
	view, err := l.snapshotView(ctx)
	if err != nil {
		return false
	}
	for ref, absent := range r.written {
		diff, err := view.DiffAgainstSource(ref)
		switch {
		case err != nil:
			return false
		case absent && diff.PresentButNotInSource():
			return false
		case !absent && diff.Changed:
			return false
		}
	}
	return true
}

// departedAtOpen reports a departure of ref the incident opened on: a drift
// finding on it among the request's findings, or the object in the
// configuration diff captured at dispatch.
func departedAtOpen(request IncidentRequest, ref sdk.ObjectRef) bool {
	for _, finding := range request.Findings {
		if len(finding.DetectorID) > len("drift.") && finding.DetectorID[:len("drift.")] == "drift." && sameResource(finding.PrimaryResource, ref) {
			return true
		}
	}
	if request.StateChanges != nil {
		for _, change := range request.StateChanges.Changes {
			if change.Kind == ref.Kind && change.Name == ref.Name && (ref.Namespace == "" || change.Namespace == ref.Namespace) {
				return true
			}
		}
	}
	return false
}

func objectKey(ref sdk.ObjectRef) sdk.ObjectRef {
	return sdk.ObjectRef{Kind: ref.Kind, Namespace: ref.Namespace, Name: ref.Name}
}

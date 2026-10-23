package runtime

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/sdk"
)

// verifyClosureDrift rechecks the owned fields admitted on source-declared
// objects. Health clearance and responder claims cannot resolve a live/source
// mismatch. Admission stays in the drift classifier, including for late drift.
func (c *Controller) verifyClosureDrift(ctx context.Context, now time.Time, snapshot sdk.DetectionContext) bool {
	c.mu.Lock()
	findings := append([]sdk.Finding(nil), c.currentIncidentRequest.Findings...)
	c.mu.Unlock()
	clear := true
	active := map[string][]sdk.Finding{}
	errors := map[string]error{}
	for _, finding := range findings {
		if !strings.HasPrefix(finding.DetectorID, "drift.") {
			continue
		}
		events := driftEventsForFindings(nil, []sdk.Finding{finding})
		// Unowned program matches have no admitted source fields.
		// Learned watch matches retain owned-field evidence when they have it.
		if len(events) != 1 || len(events[0].Fields) == 0 || events[0].Source == "absent from accepted source" {
			continue
		}
		if _, ok := active[finding.DetectorID]; !ok {
			active[finding.DetectorID] = nil
		}
		resolved := false
		err := fmt.Errorf("accepted source view is unavailable")
		if c.drift != nil && c.drift.View != nil {
			if snapshot == nil {
				snapshot, err = c.provider.Snapshot(ctx)
			} else {
				err = nil
			}
			if err == nil {
				diff, diffErr := c.drift.View(snapshot).DiffAgainstSource(finding.PrimaryResource)
				err = diffErr
				resolved = err == nil && (diff.SourceAbsent || driftFieldsRestored(diff, events) || (!diff.Changed && diff.Text == ""))
			}
		}
		if !resolved {
			clear = false
			active[finding.DetectorID] = append(active[finding.DetectorID], finding)
			if err != nil {
				errors[finding.DetectorID] = err
			}
		}
		// Unexplained findings have no scheduled detector to clear their tracker
		// entries after repair. Clear only this binding, preserving unrelated drift.
		if finding.DetectorID == "drift.unexplained" {
			c.tracker.SetPolicy(finding.DetectorID, 1, 1, 0)
			changes := c.tracker.ObserveBinding(now, finding, !resolved)
			c.recordFiring(sdk.DetectorSpec{ID: finding.DetectorID, Class: sdk.DetectorClassIncident}, now, active[finding.DetectorID], changes)
		}
	}
	ids := make([]string, 0, len(active))
	for id := range active {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		unresolved := active[id]
		if err := errors[id]; err != nil {
			c.recordDetectorError(id, now, err)
		} else {
			c.recordEvaluation(id, now, unresolved)
		}
	}
	return clear
}

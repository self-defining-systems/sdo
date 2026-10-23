package replay

import (
	"context"
	"fmt"
	"slices"
	"time"

	operator "sdo.dev/operator/runtime"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// DiffAgainstSource is recomputed with the production comparison. Primitive
// observations are available only in the phase in which they were recorded;
// fixed recordings are never used to prove the effect of a plan.
type readView struct {
	*sdktest.ReplayReadView
	source *source
	ctx    context.Context
}

func newReadView(ctx context.Context, s sdktest.Snapshot, calls []sdktest.PrimitiveCall, source *source) *readView {
	return &readView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: s, Calls: calls}, source: source, ctx: ctx}
}
func (v *readView) DiffAgainstSource(ref sdk.ObjectRef) (program.Diff, error) {
	return (operator.LiveReadView{DetectionContext: v.Snapshot, Context: v.ctx, Source: v.source, SourceCommit: "offline-accepted"}).DiffAgainstSource(ref)
}
func checkDetector(ctx context.Context, d sdk.Detector, c Case) (r Result) {
	start := time.Now()
	r = Result{ProgramID: "detector:" + d.Spec().ID, CaseID: c.ID, Status: "negative", Detectors: map[string][]DetectorResult{}}
	defer func() {
		r.DurationMS = float64(time.Since(start)) / float64(time.Millisecond)
		if v := recover(); v != nil {
			r.Status = "failed"
			r.Errors = append(r.Errors, fmt.Sprintf("detector panic: %v", v))
		}
	}()
	for _, id := range d.Spec().Playbooks {
		r.ExpectedMatch = r.ExpectedMatch || slices.Contains(c.Corpus.RootCauseProgramIDs, id)
	}
	r.Detectors["healthy"] = detect(ctx, []sdk.Detector{d}, c.Corpus.Healthy)
	r.Detectors["faulted"] = detect(ctx, []sdk.Detector{d}, c.Corpus.Open)
	for phase, rows := range r.Detectors {
		for _, row := range rows {
			if row.Error != "" {
				r.Errors = append(r.Errors, phase+": "+row.Error)
			}
			if row.Active > 0 && (phase == "healthy" || !r.ExpectedMatch) {
				r.FalsePositives = append(r.FalsePositives, phase+":detector:"+row.ID)
			}
			if phase == "faulted" && r.ExpectedMatch && row.Active == 0 {
				r.Errors = append(r.Errors, "matching detector did not fire")
			}
		}
	}
	if len(r.Errors) > 0 || len(r.FalsePositives) > 0 {
		r.Status = "failed"
	} else if r.ExpectedMatch {
		r.Status = "detected"
	}
	return r
}

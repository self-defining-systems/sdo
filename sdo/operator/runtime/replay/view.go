package replay

import (
	"context"
	"errors"
	"fmt"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
	source   *source
	ctx      context.Context
	catalog  operator.DatabaseCatalogClient
	external *cluster
}

func newReadView(ctx context.Context, s sdktest.Snapshot, calls []sdktest.PrimitiveCall, source *source) *readView {
	return &readView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: s, Calls: append(append([]sdktest.PrimitiveCall(nil), s.Readings...), calls...)}, source: source, ctx: ctx}
}
func (v *readView) DiffAgainstSource(ref sdk.ObjectRef) (program.Diff, error) {
	// Before execution, a recorded comparison can prove an uncaptured object's
	// observed absence. After execution, only the mutated fake API supplies it.
	if v.external == nil {
		if _, err := v.Snapshot.Object(ref); errors.Is(err, sdk.ErrObjectNotCaptured) {
			return v.ReplayReadView.DiffAgainstSource(ref)
		}
	}
	return (operator.LiveReadView{DetectionContext: v, Context: v.ctx, Source: v.source, SourceCommit: "offline-accepted"}).DiffAgainstSource(ref)
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
	familyMatch := detectorFamilyMatches(d, c)
	r.ExpectedMatch = familyMatch
	if d.Spec().Class == sdk.DetectorClassHealth {
		r.ExpectedMatch = healthImpact(c)
	}
	r.Detectors["healthy"] = detect(ctx, []sdk.Detector{d}, recordedPhase(c.Corpus, "healthy"))
	r.Detectors["faulted"] = detect(ctx, []sdk.Detector{d}, recordedPhase(c.Corpus, "open"))
	for _, phase := range []string{"healthy", "faulted"} {
		for _, row := range r.Detectors[phase] {
			r.scoreDetector(phase, row, familyMatch, c)
		}
	}
	if r.hasFailures() {
		r.Status = "failed"
	} else if r.Detectors["faulted"][0].Active > 0 {
		r.Status = "detected"
	}
	return r
}

func (v *readView) Object(ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if v.external != nil {
		return v.external.Get(v.ctx, ref)
	}
	return v.Snapshot.Object(ref)
}

// Keep checked detector reads on the same measured view as Diagnose.
func (v *readView) ObservedView() (sdk.DetectionContext, func() error) { return program.CheckReads(v) }

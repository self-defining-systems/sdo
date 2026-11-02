package replay

import (
	"context"
	"errors"
	"fmt"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk/inputs"
	"time"

	operator "sdo.dev/operator/runtime"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// Before execution, every read answers from what the controller recorded in
// that phase; fixed recordings are never used to prove the effect of a plan.
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
	// Before execution the controller's recorded comparison is the observation:
	// captured objects are projected (ConfigMap values redacted) and the case's
	// inventory need not be the one the controller compared against. Only an
	// explicit inventory may answer a comparison nobody recorded. After
	// execution, only the mutated fake API supplies the post-state.
	if v.external == nil {
		var diff program.Diff
		err := v.ReplayReadView.Recorded("DiffAgainstSource", []any{ref}, &diff)
		var unrecorded *sdktest.UnrecordedError
		if !errors.As(err, &unrecorded) {
			return diff, err
		}
		if len(v.source.accepted) == 0 {
			return program.Diff{}, err
		}
	}
	return (operator.LiveReadView{Carrier: inputs.New(v), Context: v.ctx, Source: v.source, SourceCommit: "offline-accepted"}).DiffAgainstSource(ref)
}
func checkDetector(ctx context.Context, d sdk.Detector, c Case) (r Result) {
	start := lifecycleio.Now()
	r = Result{ProgramID: "detector:" + d.Spec().ID, CaseID: c.ID, Status: "negative", Detectors: map[string][]DetectorResult{}}
	defer func() {
		r.DurationMS = float64(lifecycleio.Since(start)) / float64(time.Millisecond)
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

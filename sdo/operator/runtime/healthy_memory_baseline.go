package runtime

import (
	"context"
	"fmt"
	"time"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type healthyBaselineBoundaries interface {
	BaselineAt() time.Time
	BaselineCandidateAt() time.Time
}

// captureHealthyMemoryBaseline samples the same clear observations used by the
// configuration baseline. A candidate's recordings are collected at the start
// of settle, then promoted with that candidate, never resampled after a fault.
func (c *Controller) captureHealthyMemoryBaseline(ctx context.Context, now time.Time, snapshot sdk.DetectionContext, healthy bool) {
	now = now.UTC()
	if !healthy {
		c.healthyMemoryCaptured = false
		return
	}
	if c.config.CorpusSessionRoot == "" {
		return
	}
	baselineAt, candidateAt := now, time.Time{}
	if baseline, ok := c.Baseline.(healthyBaselineBoundaries); ok {
		baselineAt, candidateAt = baseline.BaselineAt(), baseline.BaselineCandidateAt()
		if baselineAt.IsZero() {
			return
		}
	} else if c.healthyMemoryCaptured {
		return
	}
	if c.healthyMemoryCandidates == nil {
		c.healthyMemoryCandidates = map[time.Time]string{}
	}
	if baselineAt.Equal(now) || candidateAt.Equal(now) {
		reference := c.config.SourceCommit + "@" + now.UTC().Format(time.RFC3339Nano)
		if err := c.recordHealthyMemoryObservation(ctx, reference, snapshot); err != nil {
			c.reportHealthyCaptureError(err)
		} else {
			c.healthyMemoryCandidates[now] = reference
		}
	}
	if reference := c.healthyMemoryCandidates[baselineAt]; reference != "" {
		c.healthyMemoryCaptured = true
		c.healthyMemoryBaselineAt = baselineAt
		c.healthyMemoryReference = reference
	}
	for at, reference := range c.healthyMemoryCandidates {
		if !at.Equal(baselineAt) && !at.Equal(candidateAt) {
			if err := memory.RemoveHealthyBaseline(c.config.CorpusSessionRoot, reference); err != nil {
				c.reportHealthyCaptureError(err)
				continue
			}
			delete(c.healthyMemoryCandidates, at)
		}
	}
}

// Record all declared Service TCP ports, including intentionally unused ports.
// Missing capabilities remain evidence gaps that reject later publication.
func (c *Controller) recordHealthyMemoryObservation(ctx context.Context, reference string, snapshot sdk.DetectionContext) error {
	captured, err := sdktest.CaptureSnapshot(snapshot, c.config.Namespace)
	if err != nil {
		return err
	}
	var view program.ReadView
	if c.drift != nil && c.drift.View != nil {
		view = c.drift.View(snapshot)
	}
	if live, ok := routingLiveView(view); ok && live.Prober != nil {
		live.DetectionContext = captured
		budget, cancel := context.WithTimeout(ctx, routingCaptureBudget)
		defer cancel()
		var services []sdk.Finding
		for _, service := range captured.Services() {
			services = append(services, sdk.Finding{PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: service.Namespace, Name: service.Name}})
		}
		recorder := &sdktest.RecordingReadView{ReadView: live}
		for index, target := range routingProbeTargets(captured, services) {
			if index >= maxRoutingCaptureProbes || budget.Err() != nil {
				break
			}
			probeCtx, done := context.WithTimeout(budget, routingCaptureProbeTimeout)
			live.Context = probeCtx
			recorder.ReadView = live
			_, _ = recorder.ProbeTCP(target.ref, target.port)
			done()
		}
		captured.Readings = append(captured.Readings, recorder.RecordedReads()...)
	}
	return memory.CaptureHealthyBaseline(c.config.CorpusSessionRoot, reference, captured, c.config.Namespace)
}

func (c *Controller) reportHealthyCaptureError(err error) {
	if c.OnError != nil {
		c.OnError(fmt.Errorf("capture pre-incident healthy memory baseline: %w", err))
	}
}

func (c *Controller) completeHealthyEvaluation() bool {
	latest := map[string]DetectorEvaluation{}
	for _, evaluation := range c.history {
		latest[evaluation.DetectorID] = evaluation
	}
	if len(c.detectors) == 0 {
		return false
	}
	for id := range c.detectors {
		if c.setupDetectorDisabled(id) {
			continue
		}
		evaluation, found := latest[id]
		if !found || evaluation.Status != DetectorEvaluationClear || evaluation.EvaluatedAt.Before(c.startedAt) {
			return false
		}
	}
	return true
}

func (c *Controller) bindIncidentHealthyBaseline(incidentID string) error {
	if c.healthyMemoryReference == "" {
		return fmt.Errorf("controller has no accepted healthy replay reference")
	}
	if baseline, ok := c.Baseline.(interface{ BaselineAt() time.Time }); ok && !baseline.BaselineAt().Equal(c.healthyMemoryBaselineAt) {
		return fmt.Errorf("healthy configuration baseline has no matching replay capture")
	}
	return memory.BindIncidentHealthyBaseline(c.config.CorpusSessionRoot, c.healthyMemoryReference, incidentID)
}

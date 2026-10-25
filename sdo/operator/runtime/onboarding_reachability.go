package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/servicehealth"
)

// Confirmation is an optional detector contract. No findings alone cannot
// establish health when its observation window is still incomplete.
func detectorConfirmed(detector sdk.Detector, snapshot sdk.DetectionContext) bool {
	if d, ok := detector.(interface {
		Confirmed(sdk.DetectionContext) bool
	}); ok {
		return d.Confirmed(snapshot)
	}
	return true
}
func detectorsConfirmed(detectors []sdk.Detector, snapshot sdk.DetectionContext) bool {
	for _, d := range detectors {
		if !detectorConfirmed(d, snapshot) {
			return false
		}
	}
	return true
}

type onboardingReachabilityState struct {
	mu             sync.RWMutex
	reference      *servicehealth.ReachabilityBaseline
	candidate      *servicehealth.ReachabilityBaseline
	configurations map[string]string
	invalid        bool
}

type onboardingSnapshotProvider struct {
	Base  SnapshotProvider
	State *onboardingReachabilityState
}

func (p onboardingSnapshotProvider) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	snapshot, err := p.Base.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	p.State.mu.RLock()
	reference := p.State.reference
	p.State.mu.RUnlock()
	return withOnboardingReference(snapshot, reference), nil
}
func withOnboardingReference(snapshot sdk.DetectionContext, reference *servicehealth.ReachabilityBaseline) sdk.DetectionContext {
	if reference == nil {
		return snapshot
	}
	window, ok := servicehealth.ReachabilityFrom(snapshot)
	if !ok {
		return snapshot
	}
	window.Baseline = reference
	window.SourceInputs = reference.Authorization.SourceInputs
	window.ObjectiveDigest = reference.Authorization.ObjectiveDigest
	if view, ok := snapshot.(program.ReadView); ok {
		return NewCycleReadView(onboardingReadView{ReadView: view, window: window})
	}
	if view, ok := snapshot.(sdktest.Snapshot); ok {
		view.Reachability = &window
		return view
	}
	return NewCycleReadView(onboardingReadView{ReadView: LiveReadView{DetectionContext: snapshot}, window: window})
}

type onboardingReadView struct {
	program.ReadView
	window servicehealth.ReachabilityWindow
}

func (v onboardingReadView) ServiceReachability() (servicehealth.ReachabilityWindow, bool) {
	return v.window, true
}

// The install window is the sole automatic authority for never-served ports.
// Every sampled evaluation must independently satisfy the application's goal;
// configuration changes or any unhealthy observation permanently reject it.
func (c *Controller) prepareOnboardingReachability(ctx context.Context, snapshot sdk.DetectionContext, detectors []sdk.Detector) sdk.DetectionContext {
	state := c.onboardingReachability
	if state == nil || c.pendingOnboarding == nil || !ConsumesServiceReachability(detectors) {
		return snapshot
	}
	state.candidate = nil
	_, ok := servicehealth.ReachabilityFrom(snapshot)
	if !ok {
		return snapshot
	}
	if _, err := healthyBaselineGoal(ctx, c.detectorList(), snapshot); err != nil {
		state.invalid = true
		return snapshot
	}
	current := map[string]string{}
	targets, _ := servicehealth.ReachabilityTargets(snapshot)
	for _, target := range targets {
		config, ok := servicehealth.ReachabilityTargetConfiguration(snapshot, target)
		if !ok {
			state.invalid = true
			return snapshot
		}
		current[target.ID()] = config
	}
	if state.configurations != nil && !reflect.DeepEqual(state.configurations, current) {
		state.invalid = true
	}
	state.configurations = current
	if state.invalid || !detectorsConfirmed(detectors, snapshot) {
		return snapshot
	}
	reference, err := servicehealth.CaptureReachabilityBaseline(snapshot, servicehealth.BaselineAuthorization{SourceInputs: objectiveDigest(c.pendingOnboarding.AcceptedCommit), ObjectiveDigest: c.reachabilityObjectiveDigest(), Reason: "healthy-baseline", GoalHealthy: true})
	if err != nil {
		if c.OnError != nil {
			c.OnError(fmt.Errorf("capture onboarding reachability: %w", err))
		}
		return snapshot
	}
	// Only stable connection refusals establish declared-but-never-served.
	// DNS/timeouts and other uncertainty remain ordinary detector evidence.
	exclusions := reference.Exclusions[:0]
	for _, e := range reference.Exclusions {
		refused := true
		for _, s := range e.Samples {
			refused = refused && s.Class == servicehealth.DialRefused
		}
		if refused {
			exclusions = append(exclusions, e)
		}
	}
	reference.Exclusions = exclusions
	state.candidate = reference
	return withOnboardingReference(snapshot, reference)
}

func (c *Controller) detectorList() []sdk.Detector {
	list := make([]sdk.Detector, 0, len(c.detectors))
	for _, d := range c.detectors {
		list = append(list, d)
	}
	return list
}

func (c *Controller) publishOnboardingReachability(ctx context.Context) {
	state := c.onboardingReachability
	if state == nil {
		return
	}
	state.mu.Lock()
	state.reference = state.candidate
	state.mu.Unlock()
	if state.reference != nil {
		for _, e := range state.reference.Exclusions {
			observation := servicehealth.ReachabilityObservation{Target: e.Target, Status: "declared-but-never-served", ReferenceAt: state.reference.CapturedAt}
			slog.InfoContext(ctx, "service reachability reference observation", "observation", observation, "evidence", fmt.Sprintf("%d confirmed refused probes", len(e.Samples)))
		}
	}
}

func (c *Controller) reachabilityObjectiveDigest() string {
	specs := map[string]sdk.DetectorSpec{}
	for id, spec := range c.detectorSpecs {
		if spec.Class == sdk.DetectorClassHealth {
			specs[id] = spec
		}
	}
	raw, _ := json.Marshal(specs)
	return objectiveDigest(string(raw))
}

// A missing/stale/over-capacity observer must not defer inventory forever.
// Expiry consumes the failed onboarding opportunity, never a clear verdict.
func (c *Controller) onboardingConfirmationExpired(now time.Time, snapshot sdk.DetectionContext) bool {
	if c.pendingOnboarding == nil {
		return false
	}
	interval, timeout, count := servicehealth.DefaultReachabilityInterval, servicehealth.DefaultReachabilityTimeout, servicehealth.DefaultReachabilityFailures
	if w, ok := servicehealth.ReachabilityFrom(snapshot); ok {
		if w.Interval > 0 {
			interval = w.Interval
		}
		if w.Timeout > 0 {
			timeout = w.Timeout
		}
		if w.Failures >= 2 {
			count = w.Failures
		}
	}
	expired := !now.Before(c.startedAt.Add(time.Duration(count)*interval + 4*timeout))
	if expired && c.OnError != nil {
		c.OnError(fmt.Errorf("onboarding confirmation window expired without confirmed detector verdicts"))
	}
	return expired
}

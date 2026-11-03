package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sdo.dev/operator/sdk/inputs"
	"strings"
	"sync"
	"time"

	"sdo.dev/operator/onboarding"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/traffic"
)

// Confirmation is an optional detector contract. No findings alone cannot
// establish health when its observation window is still incomplete.
type confirmedDetector interface {
	Confirmed(sdk.DetectionContext) bool
}

func detectorsNeedConfirmation(detectors []sdk.Detector) bool {
	for _, detector := range detectors {
		if _, ok := detector.(confirmedDetector); ok {
			return true
		}
	}
	return false
}

func detectorConfirmed(detector sdk.Detector, snapshot sdk.DetectionContext) bool {
	if d, ok := detector.(confirmedDetector); ok {
		return d.Confirmed(snapshot)
	}
	return true
}

// detectorConfirmationReason is informational; pending is never an execution error.
func detectorConfirmationReason(detector sdk.Detector, snapshot sdk.DetectionContext) string {
	if d, ok := detector.(interface {
		ConfirmationReason(sdk.DetectionContext) string
	}); ok {
		return d.ConfirmationReason(snapshot)
	}
	if !detectorConfirmed(detector, snapshot) {
		return "observation window is incomplete"
	}
	return ""
}

func pendingEvaluationSummary(states []DetectorEvaluation) string {
	var reasons []string
	for _, state := range states {
		if state.Status == DetectorEvaluationPending {
			reason := state.Reason
			if reason == "" {
				reason = "observation window is incomplete"
			}
			reasons = append(reasons, state.DetectorID+": "+reason)
		}
	}
	if len(reasons) == 0 {
		return ""
	}
	return "; pending: " + strings.Join(reasons, "; ")
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
	invalidReason  string
	startedAt      time.Time
	waitReason     string
	resumedAt      time.Time
	servedTargets  map[string]bool
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
	window.Baseline, window.BaselinePending = reference, false
	window.SourceInputs = reference.Authorization.SourceInputs
	window.ObjectiveDigest = reference.Authorization.ObjectiveDigest
	if view, ok := snapshot.(program.ReadView); ok {
		return NewCycleReadView(onboardingReadView{ReadView: view, window: window})
	}
	if view, ok := snapshot.(sdktest.Snapshot); ok {
		view.Reachability = &window
		return view
	}
	return NewCycleReadView(onboardingReadView{ReadView: LiveReadView{Carrier: inputs.New(snapshot)}, window: window})
}

type onboardingReadView struct {
	program.ReadView
	window servicehealth.ReachabilityWindow
}

// UnderlyingReadView exposes the wrapped view to the memory host edge.
func (v onboardingReadView) UnderlyingReadView() program.ReadView { return v.ReadView }

func (v onboardingReadView) ServiceReachability() (servicehealth.ReachabilityWindow, bool) {
	return v.window, true
}

// The install window is the sole automatic authority for never-served ports.
// Every sampled evaluation must independently satisfy the application's goal;
// configuration changes or any unhealthy observation permanently reject it.
func (c *Controller) prepareOnboardingReachability(ctx context.Context, now time.Time, snapshot sdk.DetectionContext, detectors []sdk.Detector) sdk.DetectionContext {
	state := c.onboardingReachability
	if state == nil || c.pendingOnboarding == nil || !ConsumesServiceReachability(detectors) {
		return snapshot
	}
	if state.startedAt.IsZero() {
		actionCtx, cancel, err := c.actionContext(ctx)
		if err != nil {
			return snapshot
		}
		err = onboarding.MarkWindow(actionCtx, c.onboardingClient, c.onboardingControl)
		cancel()
		if err != nil {
			if c.OnError != nil {
				c.OnError(err)
			}
			return snapshot
		}
		state.startedAt = now
	}
	state.candidate = nil
	verified, err := c.onboardingCommitReady(ctx, c.pendingOnboarding.AcceptedCommit)
	if err != nil {
		c.waitForOnboarding(ctx, "accepted lineage unavailable")
		return snapshot
	}
	if !verified {
		state.invalid = true
		state.invalidReason = "accepted install lineage changed"
		return snapshot
	}
	c.mu.Lock()
	busy := c.incidentOpen || c.currentIncidentRequest != nil || c.closureBlocksNextLocked()
	c.mu.Unlock()
	if busy {
		state.invalid = true
		state.invalidReason = "incident work interrupted install window"
		return snapshot
	}
	window, ok := servicehealth.ReachabilityFrom(snapshot)
	if !ok {
		return snapshot
	}
	if !c.checkOnboardingHealth(ctx, snapshot) {
		return snapshot
	}
	if state.servedTargets == nil {
		state.servedTargets = map[string]bool{}
	}
	for _, observed := range window.Targets {
		served := observed.LastSuccess != nil
		for _, sample := range observed.Samples {
			served = served || sample.OK
		}
		state.servedTargets[observed.Target.ID()] = state.servedTargets[observed.Target.ID()] || served
	}
	current := map[string]string{}
	targets, _ := servicehealth.ReachabilityTargets(snapshot)
	for _, target := range targets {
		config, ok := servicehealth.ReachabilityTargetConfiguration(snapshot, target)
		if !ok {
			c.waitForOnboarding(ctx, "target configuration unavailable: "+target.ID())
			return snapshot
		}
		current[target.ID()] = config
	}
	if state.configurations != nil && !reflect.DeepEqual(state.configurations, current) {
		state.invalid = true
		state.invalidReason = fmt.Sprintf("target configuration changed: before=%v after=%v", state.configurations, current)
	}
	state.configurations = current
	if state.invalid || !detectorsConfirmed(detectors, snapshot) || !onboardingSamplesSince(window, state.resumedAt) {
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
		if refused && !state.servedTargets[e.Target.ID()] {
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
	return c.enabledSetupDetectors(list)
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
	if c.pendingOnboarding == nil || c.onboardingReachability == nil || c.onboardingReachability.startedAt.IsZero() {
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
	budget := time.Duration(count)*interval + 4*timeout
	// Consumers can need longer than the TCP observer to prove their own
	// observation window. Bound missing or stalled consumers at five minutes.
	for _, detector := range c.detectorList() {
		if consumer, ok := detector.(traffic.Consumer); ok {
			for _, name := range consumer.TrafficWorkloads() {
				local := traffic.NewDetector(detector.Spec(), name)
				if window, ok := local.(interface {
					EvaluationWindow(sdk.DetectionContext) time.Duration
				}); ok {
					budget = max(budget, 2*window.EvaluationWindow(snapshot))
				}
			}
		}
		if consumer, ok := detector.(interface {
			EvaluationWindow(sdk.DetectionContext) time.Duration
		}); ok {
			budget = max(budget, 2*consumer.EvaluationWindow(snapshot))
		}
	}
	budget = min(budget, 5*time.Minute)
	expired := !now.Before(c.onboardingReachability.startedAt.Add(budget))
	if expired && c.OnError != nil {
		c.OnError(fmt.Errorf("onboarding confirmation window expired without confirmed detector verdicts"))
	}
	return expired
}

// A trusted maintenance pause has known process continuity. Restart only the
// sampling clock, retaining all identity, configuration and invalidation facts.
// The driver resets observer samples before invoking this entry point.
func (c *Controller) ResumeObservation(ctx context.Context, now time.Time) error {
	unlock := c.lockObservation("resume_observation")
	if c.pendingOnboarding != nil && c.onboardingReachability != nil {
		state := c.onboardingReachability
		state.startedAt = time.Time{}
		state.resumedAt = now
		state.candidate = nil
		slog.InfoContext(ctx, "namespace onboarding sampling resumed", "sample_not_before", now)
	}
	unlock()
	return c.EvaluateAll(ctx, now)
}

func onboardingSamplesSince(window servicehealth.ReachabilityWindow, since time.Time) bool {
	if since.IsZero() {
		return true
	}
	count := window.Failures
	if count < servicehealth.DefaultReachabilityFailures {
		count = servicehealth.DefaultReachabilityFailures
	}
	for _, observed := range window.Targets {
		if len(observed.Samples) < count {
			return false
		}
		for _, sample := range observed.Samples[len(observed.Samples)-count:] {
			if sample.At.Before(since) {
				return false
			}
		}
	}
	return true
}

// Incomplete reads cannot authorize onboarding and cannot contradict it.
func (c *Controller) checkOnboardingHealth(ctx context.Context, snapshot sdk.DetectionContext) bool {
	_, err := healthyBaselineGoal(ctx, c.detectorList(), snapshot)
	if err == nil {
		return true
	}
	var contradiction *baselineContradictionError
	if errors.As(err, &contradiction) {
		c.onboardingReachability.invalid = true
		c.onboardingReachability.invalidReason = err.Error()
	} else {
		c.waitForOnboarding(ctx, err.Error())
	}
	return false
}

func (c *Controller) onboardingRejectionReason(expired bool) string {
	if state := c.onboardingReachability; state != nil && state.invalid {
		if state.invalidReason != "" {
			return "install window contradicted: " + state.invalidReason
		}
		return "install boundary contradicted or interrupted"
	}
	if expired {
		return "install observation window expired; incomplete observations: " + c.onboardingReachability.waitReason
	}
	return ""
}

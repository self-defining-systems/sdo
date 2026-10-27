package runtime

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"sync"
	"time"

	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/servicehealth"
)

// Service reachability splits along the existing controller/prober line.
// The controller already holds the namespace snapshot and the observer RBAC,
// so it derives the targets (servicehealth.ReachabilityTargets) and keeps
// every target's dial history. The credential-free prober pod only dials the
// targets of each round from inside the cluster network and classifies the
// errors; it lists nothing and remembers nothing, so it needs no Kubernetes
// client, no service-account token and no new RBAC, and a replaced prober pod
// loses no history.

// evaluateOnceReachabilityInterval spaces the synchronous rounds of a
// bounded --evaluate-once run: the default three rounds take about two
// seconds plus the dials (each round at most four 1 s dial timeouts).
const evaluateOnceReachabilityInterval = time.Second

// reachabilityCatchUpSpacing spaces the dials that fill a clearing window a
// readiness transition restarted, the same spacing a bounded run uses.
const reachabilityCatchUpSpacing = time.Second

// ReachabilityDialer is the prober capability the observer needs.
type ReachabilityDialer interface {
	DialTargets(ctx context.Context, request prober.DialRequest) (prober.DialResponse, error)
}

// DialTargets runs one round of reachability dials in the prober.
func (c HTTPProberClient) DialTargets(ctx context.Context, request prober.DialRequest) (prober.DialResponse, error) {
	var response prober.DialResponse
	err := c.do(ctx, http.MethodPost, prober.PathDials, request, &response)
	return response, err
}

// ConsumesServiceReachability reports whether any detector needs dial observations.
func ConsumesServiceReachability(detectors []sdk.Detector) bool {
	for _, detector := range detectors {
		if consumer, ok := sdk.As[servicehealth.ReachabilityConsumer](detector); ok && consumer.ConsumesServiceReachability() {
			return true
		}
	}
	return false
}

// ServiceReachabilityConfig configures a ServiceReachabilityObserver.
type ServiceReachabilityConfig struct {
	Prober ReachabilityDialer
	// Snapshots is the namespace view targets are derived from: the informer
	// cache itself, never a provider that wraps this observer.
	Snapshots SnapshotProvider
	// Notify wakes reachability detectors after a round with a recent failure.
	Notify func()
	// Identity is where the dials come from, for NetworkPolicy admission.
	Identity servicehealth.ProberIdentity
	Interval time.Duration
	Timeout  time.Duration
	Failures int
	// Capacity bounds the samples kept per target.
	Capacity int
	// CatchUpSpacing spaces the extra dials of targets whose clearing window
	// a readiness transition restarted.
	CatchUpSpacing time.Duration
	// Now is the observer's clock; nil selects time.Now.
	Now func() time.Time
}

type reachabilityHistory struct {
	target      servicehealth.ReachabilityTarget
	samples     []servicehealth.DialSample
	lastSuccess *time.Time
	// holdUntil defers a target whose catch-up dial failed to the first full
	// round one interval later, so a failure run keeps the normal spacing.
	holdUntil time.Time
	// catchUps counts catch-up dials since the last full round or Pod
	// change. At most Failures run in between, so a Ready time the dials
	// never pass (clock skew) cannot keep the prober dialing.
	catchUps int
}

// ServiceReachabilityObserver dials every derived Service port once per
// interval through the prober and keeps the recent samples. Between rounds
// it dials, CatchUpSpacing apart, only the targets whose clearing window a
// readiness transition restarted (servicehealth.ReachabilityCatchingUp), so
// a repaired Service proves reachability in seconds with the same number of
// dials.
type ServiceReachabilityObserver struct {
	config ServiceReachabilityConfig
	wake   chan struct{}

	mu       sync.Mutex
	history  map[string]*reachabilityHistory
	omitted  int
	failure  string
	observed time.Time
	cancel   context.CancelFunc
	wait     sync.WaitGroup
}

// NewServiceReachabilityObserver returns nil without a prober or snapshots.
func NewServiceReachabilityObserver(config ServiceReachabilityConfig) *ServiceReachabilityObserver {
	if config.Prober == nil || config.Snapshots == nil {
		return nil
	}
	if config.Interval <= 0 {
		config.Interval = servicehealth.DefaultReachabilityInterval
	}
	if config.Timeout <= 0 {
		config.Timeout = servicehealth.DefaultReachabilityTimeout
	}
	if config.Failures < 2 {
		config.Failures = servicehealth.DefaultReachabilityFailures
	}
	if config.Capacity < config.Failures+1 {
		config.Capacity = 2 * config.Failures
	}
	if config.CatchUpSpacing <= 0 {
		config.CatchUpSpacing = reachabilityCatchUpSpacing
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return &ServiceReachabilityObserver{config: config, wake: make(chan struct{}, 1), history: map[string]*reachabilityHistory{}}
}

// Cycle runs one round: derive the targets from the current snapshot, dial
// them, and record the samples. It reports whether any target failed within
// its last Failures+1 dials, so detectors are woken both while a port fails
// and on the round that recovers it.
func (o *ServiceReachabilityObserver) Cycle(ctx context.Context) bool {
	notify, _ := o.round(ctx, false)
	return notify
}

// CatchUp dials only the targets still catching up after a readiness
// transition. It reports whether it dialed any, which wakes the detectors so
// a window that just filled clears in the same evaluation, and whether any
// target still needs more dials.
func (o *ServiceReachabilityObserver) CatchUp(ctx context.Context) (dialed bool, pending bool) {
	return o.round(ctx, true)
}

// Wake asks the running observer to check for targets to catch up, for
// example after a Pod changed. Wakes coalesce and never dial more often than
// CatchUpSpacing.
func (o *ServiceReachabilityObserver) Wake() {
	select {
	case o.wake <- struct{}{}:
	default:
	}
}

func (o *ServiceReachabilityObserver) renewCatchUps() {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, state := range o.history {
		state.catchUps = 0
	}
}

func (o *ServiceReachabilityObserver) catchingUp(snapshot sdk.DetectionContext) []servicehealth.ReachabilityTarget {
	window, _ := o.Window()
	pending := servicehealth.ReachabilityCatchingUp(snapshot, window)
	o.mu.Lock()
	defer o.mu.Unlock()
	return slices.DeleteFunc(pending, func(target servicehealth.ReachabilityTarget) bool {
		state := o.history[target.ID()]
		return state != nil && state.catchUps >= o.config.Failures
	})
}

func (o *ServiceReachabilityObserver) round(ctx context.Context, catchUp bool) (bool, bool) {
	snapshot, err := o.config.Snapshots.Snapshot(ctx)
	if err != nil {
		return false, false // the controller's own evaluation reports an unreadable namespace
	}
	targets, omitted := servicehealth.ReachabilityTargets(snapshot)
	current := make(map[string]bool, len(targets))
	for _, target := range targets {
		current[target.ID()] = true
	}
	dial := targets
	if catchUp {
		dial = o.catchingUp(snapshot)
	} else {
		dial = o.unheld(targets)
	}
	if catchUp && len(dial) == 0 {
		return false, false
	}
	var response prober.DialResponse
	if len(dial) > 0 {
		// A round lasts at most four dial timeouts in the prober; bound the
		// HTTP exchange so a hung prober cannot stall rounds.
		roundCtx, cancel := context.WithTimeout(ctx, o.config.Interval+4*o.config.Timeout)
		response, err = o.config.Prober.DialTargets(roundCtx, prober.DialRequest{Targets: dial, Timeout: o.config.Timeout})
		cancel()
	}
	o.mu.Lock()
	if err != nil {
		if ctx.Err() == nil {
			o.failure = err.Error()
		}
		o.mu.Unlock()
		return false, false
	}
	notify := o.record(response, current, omitted, catchUp)
	o.mu.Unlock()
	if catchUp {
		return true, len(o.catchingUp(snapshot)) > 0
	}
	return notify, len(o.catchingUp(snapshot)) > 0
}

func (o *ServiceReachabilityObserver) unheld(targets []servicehealth.ReachabilityTarget) []servicehealth.ReachabilityTarget {
	now := o.config.Now()
	o.mu.Lock()
	defer o.mu.Unlock()
	dial := make([]servicehealth.ReachabilityTarget, 0, len(targets))
	for _, target := range targets {
		if state := o.history[target.ID()]; state != nil && now.Before(state.holdUntil) {
			continue
		}
		dial = append(dial, target)
	}
	return dial
}

// record stores a round's samples; o.mu is held.
func (o *ServiceReachabilityObserver) record(response prober.DialResponse, current map[string]bool, omitted int, catchUp bool) bool {
	o.failure, o.omitted, o.observed = "", omitted, o.config.Now().UTC()
	for id := range o.history {
		if !current[id] {
			delete(o.history, id)
		}
	}
	for _, result := range response.Results {
		if !current[result.Target.ID()] {
			continue
		}
		state := o.history[result.Target.ID()]
		if state == nil {
			state = &reachabilityHistory{}
			o.history[result.Target.ID()] = state
		}
		state.target = result.Target
		if catchUp {
			state.catchUps++
		} else {
			state.catchUps = 0
		}
		state.samples = append(state.samples, result.Sample)
		if len(state.samples) > o.config.Capacity {
			state.samples = append([]servicehealth.DialSample(nil), state.samples[len(state.samples)-o.config.Capacity:]...)
		}
		if result.Sample.OK {
			at := result.Sample.At
			state.lastSuccess = &at
		} else if catchUp {
			state.holdUntil = o.observed.Add(o.config.Interval)
		}
	}
	for _, state := range o.history {
		recent := state.samples[max(0, len(state.samples)-o.config.Failures-1):]
		for _, sample := range recent {
			if !sample.OK {
				return true
			}
		}
	}
	return false
}

// Collect runs Failures rounds synchronously, Interval apart, so a bounded
// evaluation judges the same gate as the long-running controller from a
// history it gathered itself. It stops early when the prober cannot be read,
// leaving the load error in the window.
func (o *ServiceReachabilityObserver) Collect(ctx context.Context) error {
	return o.collect(ctx, nil)
}

func (o *ServiceReachabilityObserver) collect(ctx context.Context, check func() error) error {
	for round := 0; round < o.config.Failures; round++ {
		if round > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(o.config.Interval):
			}
		}
		o.Cycle(ctx)
		if check != nil {
			if err := check(); err != nil {
				return err
			}
		}
		o.mu.Lock()
		failed := o.failure != ""
		o.mu.Unlock()
		if failed {
			return nil
		}
	}
	return ctx.Err()
}

// Start runs a round every interval, and catch-up rounds between them, until
// Stop; on a running observer it is a no-op.
func (o *ServiceReachabilityObserver) Start(ctx context.Context) {
	o.mu.Lock()
	if o.cancel != nil {
		o.mu.Unlock()
		return
	}
	running, cancel := context.WithCancel(ctx)
	o.cancel = cancel
	o.mu.Unlock()
	o.wait.Add(1)
	go func() {
		defer o.wait.Done()
		ticker := time.NewTicker(o.config.Interval)
		defer ticker.Stop()
		catchUp := false
		var last time.Time
		for {
			notify, pending := o.round(running, catchUp)
			if catchUp {
				last = o.config.Now()
			}
			if notify && o.config.Notify != nil {
				o.config.Notify()
			}
			var next <-chan time.Time
			if pending {
				next = time.After(max(0, last.Add(o.config.CatchUpSpacing).Sub(o.config.Now())))
			}
			for waiting := true; waiting; {
				select {
				case <-running.Done():
					return
				case <-ticker.C:
					catchUp, waiting = false, false
				case <-next:
					catchUp, waiting = true, false
				case <-o.wake:
					o.renewCatchUps()
					if next == nil {
						next = time.After(max(0, last.Add(o.config.CatchUpSpacing).Sub(o.config.Now())))
					}
				}
			}
		}
	}()
}

// Stop ends the rounds and waits for the running one.
func (o *ServiceReachabilityObserver) Stop() {
	o.mu.Lock()
	cancel := o.cancel
	o.cancel = nil
	o.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	o.wait.Wait()
}

// Reset forgets every sample, for example after a maintenance pause that may
// have redeployed the application.
func (o *ServiceReachabilityObserver) Reset() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.history, o.failure, o.omitted = map[string]*reachabilityHistory{}, "", 0
}

// Window returns the current observations, sorted by target.
func (o *ServiceReachabilityObserver) Window() (servicehealth.ReachabilityWindow, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	window := servicehealth.ReachabilityWindow{
		ObservedAt: o.config.Now().UTC(), Interval: o.config.Interval, Timeout: o.config.Timeout, Failures: o.config.Failures,
		Omitted: o.omitted, Prober: o.config.Identity, Targets: make([]servicehealth.TargetObservations, 0, len(o.history)),
	}
	if o.failure != "" {
		window.LoadError = "prober unavailable: " + o.failure
	}
	for _, state := range o.history {
		observed := servicehealth.TargetObservations{
			Target: state.target, Samples: append([]servicehealth.DialSample(nil), state.samples...),
		}
		if state.lastSuccess != nil {
			at := *state.lastSuccess
			observed.LastSuccess = &at
		}
		window.Targets = append(window.Targets, observed)
	}
	sort.Slice(window.Targets, func(left, right int) bool {
		a, b := window.Targets[left].Target, window.Targets[right].Target
		if a.Service != b.Service {
			return a.Service < b.Service
		}
		return a.Port < b.Port
	})
	return window, true
}

// Summary describes the observer for the controller log.
func (o *ServiceReachabilityObserver) Summary() map[string]any {
	window, _ := o.Window()
	failing := make([]string, 0)
	for _, observed := range window.Targets {
		if count := len(observed.Samples); count > 0 && !observed.Samples[count-1].OK {
			failing = append(failing, fmt.Sprintf("%s (%s)", observed.Target.ID(), observed.Samples[count-1].Class))
		}
	}
	summary := map[string]any{
		"targets": len(window.Targets), "omitted": window.Omitted, "failing": failing,
		"interval": window.Interval.String(), "timeout": window.Timeout.String(),
	}
	if window.LoadError != "" {
		summary["error"] = window.LoadError
	}
	return summary
}

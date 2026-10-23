package runtime

import (
	"context"
	"fmt"
	"net/http"
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
		if consumer, ok := detector.(servicehealth.ReachabilityConsumer); ok && consumer.ConsumesServiceReachability() {
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
}

type reachabilityHistory struct {
	target      servicehealth.ReachabilityTarget
	samples     []servicehealth.DialSample
	lastSuccess *time.Time
}

// ServiceReachabilityObserver dials every derived Service port once per
// interval through the prober and keeps the recent samples.
type ServiceReachabilityObserver struct {
	config ServiceReachabilityConfig

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
	return &ServiceReachabilityObserver{config: config, history: map[string]*reachabilityHistory{}}
}

// Cycle runs one round: derive the targets from the current snapshot, dial
// them, and record the samples. It reports whether any target failed within
// its last Failures+1 dials, so detectors are woken both while a port fails
// and on the round that recovers it.
func (o *ServiceReachabilityObserver) Cycle(ctx context.Context) bool {
	snapshot, err := o.config.Snapshots.Snapshot(ctx)
	if err != nil {
		return false // the controller's own evaluation reports an unreadable namespace
	}
	targets, omitted := servicehealth.ReachabilityTargets(snapshot)
	var response prober.DialResponse
	if len(targets) > 0 {
		// A round lasts at most four dial timeouts in the prober; bound the
		// HTTP exchange by the interval so a hung prober cannot stall rounds.
		roundCtx, cancel := context.WithTimeout(ctx, o.config.Interval)
		response, err = o.config.Prober.DialTargets(roundCtx, prober.DialRequest{Targets: targets, Timeout: o.config.Timeout})
		cancel()
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if err != nil {
		if ctx.Err() == nil {
			o.failure = err.Error()
		}
		return false
	}
	o.failure, o.omitted, o.observed = "", omitted, time.Now().UTC()
	current := make(map[string]bool, len(targets))
	for _, target := range targets {
		current[target.ID()] = true
	}
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
		state.samples = append(state.samples, result.Sample)
		if len(state.samples) > o.config.Capacity {
			state.samples = append([]servicehealth.DialSample(nil), state.samples[len(state.samples)-o.config.Capacity:]...)
		}
		if result.Sample.OK {
			at := result.Sample.At
			state.lastSuccess = &at
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

// Start runs a round every interval until Stop; on a running observer it is a no-op.
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
		for {
			if o.Cycle(running) && o.config.Notify != nil {
				o.config.Notify()
			}
			select {
			case <-running.Done():
				return
			case <-ticker.C:
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
		ObservedAt: time.Now().UTC(), Interval: o.config.Interval, Timeout: o.config.Timeout, Failures: o.config.Failures,
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

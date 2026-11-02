package runtime

import (
	"context"
	kclock "k8s.io/utils/clock"
	"encoding/json"
	"fmt"
	"io"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"net/http"
	"sort"
	"sync"
	"time"

	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/traffic"
)

// TrafficWorkloadNames returns the workloads the detectors consume, sorted.
func TrafficWorkloadNames(detectors []sdk.Detector) []string {
	seen := make(map[string]bool)
	for _, detector := range detectors {
		consumer, ok := detector.(traffic.Consumer)
		if !ok {
			continue
		}
		for _, name := range consumer.TrafficWorkloads() {
			seen[name] = true
		}
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// ProberAPI is the controller's view of the isolated prober.
type ProberAPI interface {
	Windows(ctx context.Context) (map[string]traffic.Window, error)
	Burst(ctx context.Context, request prober.BurstRequest) (prober.BurstResult, error)
	Reset(ctx context.Context) error
}

// DefaultTrafficPollInterval is how often the controller reads the prober.
const DefaultTrafficPollInterval = 500 * time.Millisecond

// TrafficObserver keeps the latest prober observations of the workloads the
// detectors consume and wakes the controller while any of them holds a
// failed iteration, so detection and clearing do not wait for detector
// intervals. A healthy application causes no extra evaluations.
type TrafficObserver struct {
	// Clock controls polling and warmup; set it before Start. Nil uses production timers.
	Clock     kclock.WithTicker
	api       ProberAPI
	workloads []string
	notify    func()
	interval  time.Duration

	mu      sync.Mutex
	windows map[string]traffic.Window
	failure string
	cancel  context.CancelFunc
	wait    sync.WaitGroup
}

// NewTrafficObserver returns nil when no detector consumes a workload.
func NewTrafficObserver(api ProberAPI, workloads []string, notify func(), interval time.Duration) *TrafficObserver {
	if api == nil || len(workloads) == 0 {
		return nil
	}
	if interval <= 0 {
		interval = DefaultTrafficPollInterval
	}
	return &TrafficObserver{api: api, workloads: append([]string(nil), workloads...), notify: notify, interval: interval}
}

// Reconfigure replaces the accepted workload catalog and drops retired windows.
func (o *TrafficObserver) Reconfigure(workloads []string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.workloads = append([]string(nil), workloads...)
	o.windows = nil
	o.failure = ""
}

// Poll reads the prober once and reports whether any consumed workload holds
// a failed iteration within its SLO window.
func (o *TrafficObserver) Poll(ctx context.Context) bool {
	windows, err := o.api.Windows(ctx)
	o.mu.Lock()
	defer o.mu.Unlock()
	if err != nil {
		if ctx.Err() == nil {
			o.failure = err.Error()
		}
		return false
	}
	o.failure = ""
	o.windows = windows
	for _, name := range o.workloads {
		window, ok := windows[name]
		if !ok {
			continue
		}
		if windowHasRecentFailure(window) {
			return true
		}
	}
	return false
}

func windowHasRecentFailure(window traffic.Window) bool {
	for _, observed := range window.Links {
		// A link that never connected is no signal (see LinkObservations).
		if !observed.Qualified {
			continue
		}
		samples := observed.Samples
		if size := window.Workload.Failures; size > 0 && len(samples) > size {
			samples = samples[len(samples)-size:]
		}
		for _, sample := range samples {
			if !sample.OK {
				return true
			}
		}
	}
	for _, observed := range window.Scenarios {
		samples := observed.Samples
		size := window.Workload.ScenarioSLO(observed.Scenario.ID).Window
		if len(samples) > size {
			samples = samples[len(samples)-size:]
		}
		for _, sample := range samples {
			if sample.Outcome == traffic.OutcomeError || sample.Outcome == traffic.OutcomeTimeout {
				return true
			}
		}
	}
	return false
}

// Start polls until Stop; calling it on a running observer is a no-op.
func (o *TrafficObserver) Start(ctx context.Context) {
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
		ticker := timerClock(o.Clock).NewTicker(o.interval)
		defer ticker.Stop()
		for {
			if o.Poll(running) && o.notify != nil {
				o.notify()
			}
			select {
			case <-running.Done():
				return
			case <-ticker.C():
			}
		}
	}()
}

// Stop ends polling and waits for the poller.
func (o *TrafficObserver) Stop() {
	o.mu.Lock()
	cancel := o.cancel
	o.cancel = nil
	o.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	o.wait.Wait()
}

// Reset asks the prober to forget its observations and drops the local copy.
func (o *TrafficObserver) Reset(ctx context.Context) error {
	o.mu.Lock()
	o.windows = nil
	o.mu.Unlock()
	return o.api.Reset(ctx)
}

// WaitWarm polls until every scenario of every consumed workload has a
// sample, so the next evaluation, which may be the all-clear that precedes a
// fault, judges observed traffic. It reports whether that happened in time.
func (o *TrafficObserver) WaitWarm(ctx context.Context, timeout time.Duration) bool {
	deadline := timerClock(o.Clock).NewTimer(timeout)
	defer deadline.Stop()
	ticker := timerClock(o.Clock).NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		o.Poll(ctx)
		if o.warm() {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-deadline.C():
			return o.warm()
		case <-ticker.C():
		}
	}
}

func (o *TrafficObserver) warm() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, name := range o.workloads {
		window, ok := o.windows[name]
		if !ok || len(window.Scenarios)+len(window.Links) == 0 {
			return false
		}
		for _, observed := range window.Links {
			if len(observed.Samples) == 0 {
				return false
			}
		}
		for _, observed := range window.Scenarios {
			if len(observed.Samples) == 0 {
				return false
			}
		}
	}
	return true
}

// Windows returns the consumed workloads' windows. When the prober cannot be
// read, or does not run a consumed workload, the window carries a LoadError
// so the detector reports an error: a health verdict that cannot be observed
// must not count as clear.
func (o *TrafficObserver) Windows() map[string]traffic.Window {
	o.mu.Lock()
	defer o.mu.Unlock()
	windows := make(map[string]traffic.Window, len(o.workloads))
	for _, name := range o.workloads {
		window, ok := o.windows[name]
		switch {
		case o.failure != "":
			windows[name] = traffic.Window{LoadError: "prober unavailable: " + o.failure}
		case !ok && o.windows != nil:
			windows[name] = traffic.Window{LoadError: "prober does not run health-probe workload " + name}
		case ok:
			windows[name] = window
		}
	}
	return windows
}

// Summary describes each consumed workload for the controller log.
func (o *TrafficObserver) Summary() map[string]any {
	summary := make(map[string]any)
	for name, window := range o.Windows() {
		if window.LoadError != "" {
			summary[name] = map[string]any{"error": window.LoadError}
			continue
		}
		scenarios := make(map[string]any, len(window.Scenarios))
		for _, observed := range window.Scenarios {
			scenarios[observed.Scenario.ID] = map[string]any{
				"samples": len(observed.Samples), "qualified": observed.Qualified, "invalid": observed.Invalid,
				"target": observed.Scenario.Target.Service, "depends_on": observed.Scenario.DependsOn,
			}
		}
		if len(window.Links) > 0 {
			links := make(map[string]any, len(window.Links))
			for _, observed := range window.Links {
				links[observed.Link.String()] = map[string]any{"samples": len(observed.Samples), "qualified": observed.Qualified}
			}
			summary[name] = map[string]any{"interval": window.Workload.Interval.Duration().String(), "failures": window.Workload.Failures, "links": links}
			continue
		}
		summary[name] = map[string]any{
			"rate_per_second": window.Workload.RatePerSecond, "seed": window.Workload.Seed, "scenarios": scenarios,
		}
	}
	return summary
}

// TrafficSnapshotProvider adds synthetic-traffic windows and Service
// reachability observations to another provider's snapshots.
type TrafficSnapshotProvider struct {
	Base         SnapshotProvider
	Observer     *TrafficObserver
	Reachability *ServiceReachabilityObserver
	// Reference is this cluster's reachability baseline; nil means no record.
	Reference *ReachabilityReference
}

func (p TrafficSnapshotProvider) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	base, err := p.Base.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	windows := map[string]traffic.Window{}
	if p.Observer != nil {
		windows = p.Observer.Windows()
	}
	var reachability *servicehealth.ReachabilityWindow
	if p.Reachability != nil {
		window, _ := p.Reachability.Window()
		p.Reference.bind(&window)
		reachability = &window
	}
	carrier := inputs.New(base)
	if p.Observer != nil {
		carrier = carrier.WithTraffic(trafficWindows(windows))
	}
	if reachability != nil {
		carrier = carrier.WithReachability(reachabilityWindow{reachability})
	}
	return trafficContext{Carrier: carrier, ScopedObservationSource: sdk.ScopedSource(base)}, nil
}

// trafficContext keeps the carrier's inputs and the base snapshot's scoped reads visible.
type trafficContext struct {
	inputs.Carrier
	sdk.ScopedObservationSource
}

type trafficWindows map[string]traffic.Window

func (w trafficWindows) TrafficWindow(id string) (traffic.Window, bool) { v, ok := w[id]; return v, ok }

type reachabilityWindow struct {
	window *servicehealth.ReachabilityWindow
}

func (w reachabilityWindow) ServiceReachability() (servicehealth.ReachabilityWindow, bool) {
	return *w.window, true
}

func scopedSource(view sdk.DetectionContext) sdk.ScopedObservationSource {
	if source, ok := view.(sdk.ScopedObservationSource); ok {
		return source
	}
	return sdk.ScopedSnapshot{}
}

func (v trafficContext) CaptureObjects(refs []sdk.ObjectRef) ([]unstructured.Unstructured, error) {
	if source, ok := v.DetectionContext.(sdk.ObjectCaptureSource); ok {
		return source.CaptureObjects(refs)
	}
	return sdk.TypedObjects(v.DetectionContext), nil
}

func (v trafficContext) Object(ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if source, ok := v.DetectionContext.(sdk.ObjectReader); ok {
		return source.Object(ref)
	}
	return sdk.FindObject(sdk.TypedObjects(v.DetectionContext), ref)
}

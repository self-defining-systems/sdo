package storestate

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
)

// Rule IDs of store-state findings.
const (
	RuleWriteBlocked         = "store-state.write-blocked"
	RuleRoleChanged          = "store-state.role-changed"
	RulePrincipalChanged     = "store-state.principal-changed"
	RuleCatalogChanged       = "store-state.catalog-changed"
	RuleRefused              = "store-state.read-refused"
	RuleConnectionsNearLimit = "store-state.connections-near-limit"
	RuleMemoryNearLimit      = "store-state.memory-near-limit"
)

// NearLimit is the fraction of a cap at which use is reported.
const NearLimit = 0.95

// Config declares one store component to watch.
type Config struct {
	// Spec is the detector's spec; a v1 Pod watch is added when absent.
	Spec sdk.DetectorSpec
	// Component is the application-map component the store belongs to.
	Component string
	// Engine selects the adapter; it must have one (AdapterFor).
	Engine Engine
	// Target selects the store's Pods and container by role.
	Target podread.Target
	// Port is where the store listens in its container; 0 is the engine default.
	Port int
	// ReadInterval is the pod-read interval; 0 is podread.MinInterval.
	ReadInterval time.Duration
	// Baselines are facts from a healthy moment, one per store Pod name, or a
	// single baseline every Pod is compared with. Without a baseline only
	// absolute facts are judged: write blocks, refusals and near-cap use.
	Baselines []Facts
}

type detector struct {
	spec      sdk.DetectorSpec
	config    Config
	adapter   Adapter
	query     podread.Query
	baselines map[string]Facts
}

// NewDetector returns a deterministic detector of the store's facts. It
// reports a write block, a role, principal or catalog change against the
// baseline, a refusal of its own read, and connection or memory use at
// NearLimit of a cap (memory only under a noeviction policy). Each finding
// names the component, the Pod and the changed item. A read that gives no
// complete answer is a detector error, never a clear.
func NewDetector(c Config) (sdk.Detector, error) {
	if c.Spec.ID == "" {
		return nil, fmt.Errorf("store-state detector requires a spec ID")
	}
	if c.Component == "" {
		return nil, fmt.Errorf("store-state detector %s requires a component", c.Spec.ID)
	}
	adapter, ok := AdapterFor(c.Engine)
	if !ok {
		return nil, fmt.Errorf("store-state detector %s: engine %q has no reader", c.Spec.ID, c.Engine)
	}
	if c.Port == 0 {
		c.Port = adapter.DefaultPort()
	}
	if c.Port < 1 || c.Port > 65535 {
		return nil, fmt.Errorf("store-state detector %s: port %d is out of range", c.Spec.ID, c.Port)
	}
	if c.ReadInterval == 0 {
		c.ReadInterval = podread.MinInterval
	}
	query := podread.Query{ID: "store-state-" + c.Component, Target: c.Target, Args: adapter.ReadCommand(Endpoint{Port: c.Port}), Interval: c.ReadInterval}
	if err := query.Validate(); err != nil {
		return nil, fmt.Errorf("store-state detector %s: %w", c.Spec.ID, err)
	}
	baselines := map[string]Facts{}
	for _, b := range c.Baselines {
		if err := validBaseline(c, b); err != nil {
			return nil, fmt.Errorf("store-state detector %s: %w", c.Spec.ID, err)
		}
		if _, dup := baselines[b.Pod.Name]; dup {
			return nil, fmt.Errorf("store-state detector %s: two baselines for Pod %q", c.Spec.ID, b.Pod.Name)
		}
		baselines[b.Pod.Name] = b
	}
	spec := c.Spec
	if !slices.ContainsFunc(spec.Watches, func(w sdk.WatchKind) bool { return w.APIVersion == "v1" && w.Kind == "Pod" }) {
		spec.Watches = append(slices.Clone(spec.Watches), sdk.WatchKind{APIVersion: "v1", Kind: "Pod"})
	}
	return detector{spec: spec, config: c, adapter: adapter, query: query, baselines: baselines}, nil
}

func validBaseline(c Config, b Facts) error {
	switch {
	case b.Error != "":
		return fmt.Errorf("baseline of Pod %q is unavailable: %s", b.Pod.Name, b.Error)
	case b.Engine != c.Engine:
		return fmt.Errorf("baseline of Pod %q is engine %q, not %q", b.Pod.Name, b.Engine, c.Engine)
	case b.Component != c.Component:
		return fmt.Errorf("baseline of Pod %q is component %q, not %q", b.Pod.Name, b.Component, c.Component)
	}
	for _, it := range b.Items {
		if it.Kind == KindWriteBlock || (it.Kind == KindRefusal && it.Value != "unsupported") {
			return fmt.Errorf("baseline of Pod %q is not healthy: %s %s", b.Pod.Name, it.Key(), it.Value)
		}
	}
	return b.Validate()
}

func (d detector) Spec() sdk.DetectorSpec { return d.spec }

// PodReads implements podread.Consumer.
func (d detector) PodReads() []podread.Query { return []podread.Query{d.query} }

// Observe returns the facts of every store Pod the read reached, for a
// baseline or a diagnosis. Any missing, failed or unparsable read is an error.
func Observe(ctx sdk.DetectionContext, d sdk.Detector) ([]Facts, error) {
	s, ok := d.(detector)
	if !ok {
		return nil, fmt.Errorf("not a store-state detector")
	}
	return s.observe(ctx)
}

func (d detector) observe(ctx sdk.DetectionContext) ([]Facts, error) {
	result, err := podread.Require(ctx, d.query.ID)
	if err != nil {
		return nil, fmt.Errorf("store %s: %w", d.config.Component, err)
	}
	out := make([]Facts, 0, len(result.Readings))
	for _, reading := range result.Readings {
		f, err := d.adapter.Parse(reading.Stdout)
		if err != nil {
			return nil, fmt.Errorf("store %s Pod %s: %w", d.config.Component, reading.Pod, err)
		}
		f.Component, f.Pod, f.At = d.config.Component, reading.Ref(), reading.ObservedAt
		out = append(out, f)
	}
	return out, nil
}

func (d detector) baselineFor(pod string) (Facts, bool) {
	if b, ok := d.baselines[pod]; ok {
		return b, true
	}
	if len(d.baselines) == 1 {
		for _, b := range d.baselines {
			return b, true
		}
	}
	return Facts{}, false
}

func (d detector) Detect(_ context.Context, ctx sdk.DetectionContext) ([]sdk.Finding, error) {
	facts, err := d.observe(ctx)
	if err != nil {
		return nil, err
	}
	var findings []sdk.Finding
	for _, f := range facts {
		baseline, hasBaseline := d.baselineFor(f.Pod.Name)
		var changes []Change
		if hasBaseline {
			changes = Diff(baseline, f)
		} else {
			changes = Diff(Facts{}, f)
		}
		cites := map[ItemKind][]Change{}
		for _, c := range changes {
			if c.After != nil && c.After.Kind == KindWriteBlock {
				findings = append(findings, d.finding(f, RuleWriteBlocked, c, fmt.Sprintf("store %s (%s) refuses writes: %s", d.config.Component, f.Engine, c.After.Name)))
			}
			cites[c.Key().Kind] = append(cites[c.Key().Kind], c)
		}
		for _, c := range changes {
			key := c.Key()
			switch {
			case key.Kind == KindRefusal && c.After != nil && (c.After.Value != "unsupported" || hasBaseline):
				findings = append(findings, d.finding(f, RuleRefused, c, fmt.Sprintf("store %s (%s) refuses its %s read: %s", d.config.Component, f.Engine, key.Name, c.After.Value)))
			case !hasBaseline:
			case key.Kind == KindRole:
				findings = append(findings, d.finding(f, RuleRoleChanged, c, fmt.Sprintf("store %s (%s) %s changed since the healthy baseline", d.config.Component, f.Engine, key.Name)))
			case key.Kind == KindPrincipal:
				findings = append(findings, d.finding(f, RulePrincipalChanged, c, fmt.Sprintf("store %s (%s) principal %s changed since the healthy baseline", d.config.Component, f.Engine, key.Name)))
			case key.Kind == KindIndex || key.Kind == KindValidator || key.Kind == KindLock:
				findings = append(findings, d.finding(f, RuleCatalogChanged, c, fmt.Sprintf("store %s (%s) %s changed since the healthy baseline", d.config.Component, f.Engine, key)))
			}
		}
		// A blocked write cites the setting changes that explain it.
		for i := range findings {
			if findings[i].RuleID == RuleWriteBlocked && findings[i].PrimaryResource == f.Pod && len(cites[KindSetting]) > 0 {
				findings[i].Evidence += "; settings changed since the healthy baseline: " + render(cites[KindSetting])
			}
		}
		if f.Connections != nil && f.Connections.Near(NearLimit) {
			findings = append(findings, d.gaugeFinding(f, RuleConnectionsNearLimit, "connections", *f.Connections))
		}
		if f.Memory != nil && f.Memory.Near(NearLimit) && d.noEviction(f) {
			findings = append(findings, d.gaugeFinding(f, RuleMemoryNearLimit, "memory", *f.Memory))
		}
	}
	sort.SliceStable(findings, func(i, j int) bool { return findings[i].Fingerprint < findings[j].Fingerprint })
	return findings, nil
}

func (d detector) noEviction(f Facts) bool {
	policy, ok := f.Lookup(Key{Kind: KindSetting, Name: "maxmemory-policy"})
	return !ok || policy.Value == "noeviction"
}

func render(changes []Change) string {
	parts := make([]string, 0, len(changes))
	for _, c := range changes {
		before, after := "(absent)", "(absent)"
		if c.Before != nil {
			before = c.Before.Value
		}
		if c.After != nil {
			after = c.After.Value
		}
		parts = append(parts, fmt.Sprintf("%s %s -> %s", c.Key().Name, before, after))
	}
	return strings.Join(parts, ", ")
}

func (d detector) finding(f Facts, rule string, c Change, summary string) sdk.Finding {
	return sdk.Finding{
		DetectorID: d.spec.ID, RuleID: rule, Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
		Summary:         summary,
		Evidence:        fmt.Sprintf("Pod %s: %s", f.Pod.Name, c),
		PrimaryResource: f.Pod,
		Playbooks:       slices.Clone(d.spec.Playbooks),
		Fingerprint:     fmt.Sprintf("%s/%s/%s/%s/%s", d.spec.ID, rule, f.Pod.Namespace, f.Pod.Name, c.Key()),
		Metadata:        map[string]any{"component": d.config.Component, "engine": string(f.Engine), "change": c},
	}
}

func (d detector) gaugeFinding(f Facts, rule, what string, g Gauge) sdk.Finding {
	return sdk.Finding{
		DetectorID: d.spec.ID, RuleID: rule, Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
		Summary:         fmt.Sprintf("store %s (%s) %s at %d of %d", d.config.Component, f.Engine, what, g.Used, g.Max),
		Evidence:        fmt.Sprintf("Pod %s: %s used %d, cap %d (report at %.0f%%)", f.Pod.Name, what, g.Used, g.Max, NearLimit*100),
		PrimaryResource: f.Pod,
		Playbooks:       slices.Clone(d.spec.Playbooks),
		Fingerprint:     fmt.Sprintf("%s/%s/%s/%s", d.spec.ID, rule, f.Pod.Namespace, f.Pod.Name),
		Metadata:        map[string]any{"component": d.config.Component, "engine": string(f.Engine), what: g},
	}
}

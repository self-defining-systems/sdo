package storestate

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strconv"
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
	RuleSequenceNearLimit    = "store-state.sequence-near-limit"
	RuleNamespaceChanged     = "store-state.namespace-changed"
	RuleRegistryChanged      = "store-state.registry-changed"
	RuleLockWaiters          = "store-state.lock-waiters"
)

// Namespace judgement: a judged namespace (a table, collection, KV prefix or
// Namespace judgement: a judged namespace (a table, collection, KV prefix or
// key pattern) is reported when it disappears or when its count drops below
// half of a baseline of at least MinJudgedCount. Growth is not judged: an
// application writes its tables, so a count grows with time and traffic past
// any multiple of a healthy reading. A namespace the healthy application
// itself shrinks (smaller in a healthy reading than in the one before, or
// gone from one) is not judged at all: removing its entries is the
// application's own churn (a queue drained, a lease or lock released, a
// temporary table dropped).
const MinJudgedCount = 10

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
	// single baseline every Pod is compared with. Without them the detector
	// compares with the controller's promoted healthy readings of its pod
	// read (podread.Result.Baseline), and without those only absolute facts
	// are judged: write blocks, refusals and near-cap use.
	Baselines []Facts
	// GapWithoutBaseline makes a store Pod the detector has no healthy
	// baseline for a coverage gap instead of a finding or an error: a read
	// that is unavailable, unparsable or refused clears, and
	// ConfirmationReason says why the store is not observed. Go sets it on
	// the readers it installs from the application map, which no designer
	// checked against the store's image or credentials, so a store SDO cannot
	// read never stalls deployment acceptance or an incident's recovery. Once
	// the Pod was read healthy, the same reads are a refusal finding or a
	// detector error as usual. Absolute facts of a readable store (write
	// blocks, near-cap use) are judged either way.
	GapWithoutBaseline bool
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
// baseline, a refusal of its own read, connection or memory use at NearLimit
// of a cap (memory only under a noeviction policy), and a session holding a
// lock others have waited on for LockWaitThreshold. Each finding
// names the component, the Pod and the changed item. A read that gives no
// complete answer is a detector error, never a clear, except a store a
// GapWithoutBaseline reader has never read healthy, which is a coverage gap.
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
	query := podread.Query{ID: QueryID(c.Engine, c.Component), Target: c.Target, Args: adapter.ReadCommand(Endpoint{Port: c.Port}), Interval: c.ReadInterval}
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
	facts, _, err := s.observe(ctx)
	return facts, err
}

// observe parses the current readings and returns the baselines to compare
// them with: the configured ones, or else the controller's promoted healthy
// readings carried with the result.
func (d detector) observe(ctx sdk.DetectionContext) ([]Facts, reference, error) {
	result, err := podread.Require(ctx, d.query.ID)
	if err != nil {
		return nil, reference{}, fmt.Errorf("store %s: %w", d.config.Component, err)
	}
	out := make([]Facts, 0, len(result.Readings))
	for _, reading := range result.Readings {
		f, err := parseReading(d.adapter, d.config.Component, reading)
		if err != nil {
			return nil, reference{}, fmt.Errorf("store %s Pod %s: %w", d.config.Component, reading.Pod, err)
		}
		out = append(out, f)
	}
	return out, d.reference(result), nil
}

// reference is what a store Pod's facts are judged against: its healthy
// baseline and the healthy history around it.
type reference struct {
	baselines map[string]Facts
	history   map[string][]Facts
}

// reference returns the configured baselines (a history of one reading
// each), or else the controller's promoted ones and their history.
func (d detector) reference(result podread.Result) reference {
	if len(d.baselines) > 0 {
		return reference{baselines: d.baselines}
	}
	return reference{baselines: promotedBaselines(d.adapter, d.config.Component, result), history: promotedHistory(d.adapter, d.config.Component, result)}
}

// pick returns pod's baseline (pick) and the healthy readings of the baseline's
// Pod, the baseline included, oldest first.
func (r reference) pick(pod string) (Facts, []Facts, bool) {
	baseline, ok := pick(r.baselines, pod)
	if !ok {
		return Facts{}, nil, false
	}
	samples := slices.Clone(r.history[baseline.Pod.Name])
	if !slices.ContainsFunc(samples, func(f Facts) bool { return f.At.Equal(baseline.At) }) {
		samples = append(samples, baseline)
	}
	slices.SortStableFunc(samples, func(a, b Facts) int { return a.At.Compare(b.At) })
	return baseline, samples, true
}

// ConfirmationReason names the store Pods a GapWithoutBaseline reader does
// not observe and why; it is empty when every Pod is observed. The runtime
// records it with the clear evaluation, so the gap reaches the health verdict
// without holding it.
func (d detector) ConfirmationReason(ctx sdk.DetectionContext) string {
	if !d.config.GapWithoutBaseline {
		return ""
	}
	_, _, gaps, err := d.assess(ctx)
	if err != nil || len(gaps) == 0 {
		return ""
	}
	reason := fmt.Sprintf("store %s (%s) not observed until it is read healthy once: %s", d.config.Component, d.config.Engine, strings.Join(gaps, "; "))
	if len(reason) > maxGapReasonBytes {
		reason = reason[:maxGapReasonBytes] + "..."
	}
	return reason
}

// maxGapReasonBytes bounds a coverage reason; its head names the cause.
const maxGapReasonBytes = 400

// assess is observe for a GapWithoutBaseline reader: a Pod without a healthy
// baseline whose read gives no answer, or a refused one, is a gap (named in
// gaps, its refusals dropped) rather than an error or a finding. A reader
// without the option, and a Pod with a baseline, keep observe's contract.
func (d detector) assess(ctx sdk.DetectionContext) ([]Facts, reference, []string, error) {
	if !d.config.GapWithoutBaseline {
		facts, ref, err := d.observe(ctx)
		return facts, ref, nil, err
	}
	result, found := podread.ResultFrom(ctx, d.query.ID)
	ref := d.reference(result)
	if !found || result.Error != "" || len(result.Readings) == 0 {
		if len(ref.baselines) > 0 {
			_, err := podread.Require(ctx, d.query.ID)
			return nil, reference{}, nil, fmt.Errorf("store %s: %w", d.config.Component, err)
		}
		reason := result.Error
		switch {
		case !found:
			reason = "no pod read in this evaluation"
		case reason == "":
			reason = "no readings"
		}
		return nil, ref, []string{reason}, nil
	}
	var facts []Facts
	var gaps []string
	for _, reading := range result.Readings {
		_, baselined := pick(ref.baselines, reading.Pod)
		f, err := parseReading(d.adapter, d.config.Component, reading)
		switch {
		case err != nil && baselined:
			return nil, reference{}, nil, fmt.Errorf("store %s Pod %s: %w", d.config.Component, reading.Pod, err)
		case err != nil:
			gaps = append(gaps, fmt.Sprintf("Pod %s: %s", reading.Pod, firstLine(err.Error())))
			continue
		}
		if refused := refusals(f); len(refused) > 0 && !baselined {
			gaps = append(gaps, fmt.Sprintf("Pod %s refuses %s", reading.Pod, strings.Join(refused, ", ")))
			f.Items = slices.DeleteFunc(slices.Clone(f.Items), func(it Item) bool { return it.Kind == KindRefusal })
		}
		facts = append(facts, f)
	}
	return facts, ref, gaps, nil
}

// refusals lists a read's refusals other than an unsupported command, as
// "<section> <class>".
func refusals(f Facts) []string {
	var out []string
	for _, it := range f.Items {
		if it.Kind == KindRefusal && it.Value != "unsupported" {
			out = append(out, it.Name+" "+it.Value)
		}
	}
	return out
}

func (d detector) Detect(_ context.Context, ctx sdk.DetectionContext) ([]sdk.Finding, error) {
	facts, ref, _, err := d.assess(ctx)
	if err != nil {
		return nil, err
	}
	var findings []sdk.Finding
	for _, f := range facts {
		baseline, healthy, hasBaseline := ref.pick(f.Pod.Name)
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
			case (key.Kind == KindIndex || key.Kind == KindValidator || key.Kind == KindLock) && judgedCatalogChange(healthy, f, c):
				findings = append(findings, d.finding(f, RuleCatalogChanged, c, fmt.Sprintf("store %s (%s) %s changed since the healthy baseline", d.config.Component, f.Engine, key)))
			case judgedNamespace(f.Engine, key):
				if how := namespaceShift(c, rangeOf(healthy, key)); how != "" {
					finding := d.finding(f, RuleNamespaceChanged, c, fmt.Sprintf("store %s (%s) %s %s since the healthy baseline", d.config.Component, f.Engine, key, how))
					if appeared := appearedNamespaces(f.Engine, changes); appeared != "" {
						finding.Evidence += "; appeared since the healthy baseline: " + appeared
					}
					findings = append(findings, finding)
				}
			case key.Kind == KindRegistryEntry && key.Scope != "":
				if how := registryShift(c); how != "" {
					findings = append(findings, d.finding(f, RuleRegistryChanged, c, fmt.Sprintf("store %s (%s) %s %s since the healthy baseline", d.config.Component, f.Engine, key, how)))
				}
			}
		}
		// A session holding a lock others have waited on for
		// LockWaitThreshold is judged against the healthy history: a lock
		// object the healthy application itself kept sessions waiting on
		// that long is its own behavior.
		if hasBaseline {
			for _, w := range f.LockWaits {
				if w.Waited >= LockWaitThreshold && !routineLockWait(healthy, w.Object) {
					findings = append(findings, d.lockWaitFinding(ctx, f, w))
				}
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
		for _, name := range slices.Sorted(maps.Keys(f.Sequences)) {
			if g := f.Sequences[name]; g.Near(NearLimit) {
				finding := d.gaugeFinding(f, RuleSequenceNearLimit, "sequence "+name, g)
				finding.Fingerprint += "/" + name
				findings = append(findings, finding)
			}
		}
		// A connection cap reached, or a read refused at it, names who holds
		// the connections.
		if len(f.Holders) > 0 {
			groups := ResolveHolders(ctx, f.Pod, f.Holders)
			for i := range findings {
				finding := &findings[i]
				if finding.PrimaryResource != f.Pod {
					continue
				}
				change, _ := finding.Metadata["change"].(Change)
				if finding.RuleID == RuleConnectionsNearLimit || finding.RuleID == RuleRefused && change.After != nil && change.After.Value == "max-clients" {
					finding.Evidence += "; connections held: " + RenderHolders(groups)
					finding.Metadata["holders"] = groups
				}
			}
		}
	}
	sort.SliceStable(findings, func(i, j int) bool { return findings[i].Fingerprint < findings[j].Fingerprint })
	return findings, nil
}

func (d detector) noEviction(f Facts) bool {
	if evictions, ok := f.Lookup(Key{Kind: KindSetting, Name: "evictions"}); ok {
		return evictions.Value == "off"
	}
	policy, ok := f.Lookup(Key{Kind: KindSetting, Name: "maxmemory-policy"})
	return !ok || policy.Value == "noeviction"
}

// judgedCatalogChange reports whether an index, validator or lock change is
// evidence of a fault rather than the application's own churn:
//   - lock activity (waiters, an active lock counter, Value "active") comes and
//     goes with load; only a held lock (fsync, a global read lock, a metadata
//     lock) is judged, and a lock that blocks writes is also a write block;
//   - an index or validator belongs to its table's lifecycle when the table
//     itself appeared or disappeared (a new or rotated time partition, judged by
//     the namespace rule) or was missing from a healthy reading (a table the
//     application creates and drops itself); on a table present in every
//     healthy reading that still exists, an added, removed or changed one is
//     judged (a constraint added to a live table rejects writes that used to
//     succeed);
//   - a group digest of a large catalog (catalog_group.go) is judged when its
//     value changed under the same scope, which names the group's tables: a
//     definition changed while every table of the group still exists.
func judgedCatalogChange(healthy []Facts, current Facts, c Change) bool {
	key := c.Key()
	if key.Kind == KindLock {
		return !(c.Before != nil && c.Before.Value == "active" || c.After != nil && c.After.Value == "active")
	}
	db, object, scoped := strings.Cut(key.Scope, ".")
	if !scoped {
		return c.Before != nil
	}
	if isCatalogGroup(object) {
		// The scope carries the group's table digest: a change under the same
		// scope is a definition changed on tables that all still exist.
		return c.Before != nil && c.After != nil
	}
	table := Key{Kind: KindNamespace, Scope: db, Name: object}
	_, after := current.Lookup(table)
	// The table must have been read in a healthy reading, not only be
	// unread in all of them (a catalog read as group digests lists none).
	seen := slices.ContainsFunc(healthy, func(f Facts) bool { _, ok := f.Lookup(table); return ok })
	return after && seen && rangeOf(healthy, table).present
}

// judgedNamespace reports whether the namespace-changed rule judges an item:
// the tables and collections of a database, consul KV prefixes, and redis key
// patterns. Server-wide totals and a cache's item counts are not judged.
func judgedNamespace(engine Engine, k Key) bool {
	switch {
	case k.Kind == KindKeyPattern:
		return true
	case k.Kind != KindNamespace || k.Scope == "":
		return false
	}
	switch engine {
	case EngineMongoDB, EnginePostgres, EngineMySQL, EngineConsul:
		return true
	}
	return false
}

// count reads a namespace count; an estimate marker "~" is dropped.
func count(it *Item) (int64, bool) {
	if it == nil {
		return 0, false
	}
	n, err := strconv.ParseInt(strings.TrimPrefix(it.Value, "~"), 10, 64)
	return n, err == nil && n >= 0
}

// healthyRange is how one namespace behaved over a Pod's healthy readings
// that could read its kind, oldest first.
type healthyRange struct {
	// present is true when every such reading has the namespace.
	present bool
	// counted is true when every reading that has it gives a count.
	counted bool
	// shrank is true when a reading has fewer entries than the one before,
	// or lacks the namespace the one before had.
	shrank bool
}

func rangeOf(healthy []Facts, k Key) healthyRange {
	r := healthyRange{present: true, counted: true}
	var prev int64
	seen := false
	for _, f := range healthy {
		if f.unread(k.Kind) {
			continue
		}
		var n int64
		if it, ok := f.Lookup(k); !ok {
			r.present = false
		} else if v, ok := count(&it); ok {
			n = v
		} else {
			r.counted = false
			continue
		}
		r.shrank = r.shrank || seen && n < prev
		prev, seen = n, true
	}
	return r
}

// namespaceShift says how a judged namespace changed from its baseline, given
// how it behaved while healthy, or "" when the change is within the judged
// bounds. A namespace that appeared is not judged by itself (applications
// create tables and collections lazily); it is cited with a disappearance.
func namespaceShift(c Change, healthy healthyRange) string {
	before, hasBefore := count(c.Before)
	after, hasAfter := count(c.After)
	switch {
	case c.Before == nil:
		return ""
	case c.After == nil:
		if healthy.shrank || c.Before.Kind == KindKeyPattern && (!hasBefore || before < MinJudgedCount) {
			return ""
		}
		return "disappeared"
	case !hasBefore || !hasAfter || !healthy.counted:
		return ""
	case !healthy.shrank && before >= MinJudgedCount && after*2 < before:
		return fmt.Sprintf("dropped from %d to %d", before, after)
	}
	return ""
}

// appearedNamespaces lists the judged namespaces that appeared, for the
// evidence of a disappearance (a rename shows as one of each).
func appearedNamespaces(engine Engine, changes []Change) string {
	var out []string
	for _, c := range changes {
		if c.Before == nil && c.After != nil && judgedNamespace(engine, c.After.Key()) {
			out = append(out, fmt.Sprintf("%s (%s)", c.After.Key(), c.After.Value))
		}
	}
	return strings.Join(out, ", ")
}

// registryShift says how a registry entry (a registered service, a cluster
// member) changed, or "" for a change that loses nothing: a new entry, or a
// service gaining instances.
func registryShift(c Change) string {
	switch {
	case c.Before == nil:
		return ""
	case c.After == nil:
		return "disappeared"
	}
	before, okB := count(c.Before)
	after, okA := count(c.After)
	if okB && okA {
		if after < before {
			return fmt.Sprintf("dropped from %d to %d", before, after)
		}
		return ""
	}
	return fmt.Sprintf("changed from %s to %s", c.Before.Value, c.After.Value)
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

// lockWaitFinding names the holding session (its id, peer resolved to its
// Pod and workload, state, mode and transaction age), the locked object and
// the sessions waiting on it, so the holder can be ended with the store's own
// terminate command. The fingerprint is the object's, so the finding persists
// across reads while any session keeps others waiting on it.
func (d detector) lockWaitFinding(view sdk.DetectionContext, f Facts, w LockWait) sdk.Finding {
	holder := ResolveHolders(view, f.Pod, []Holder{{Peer: w.Peer, State: w.State, Count: 1}})[0]
	evidence := fmt.Sprintf("Pod %s: %s", f.Pod.Name, w)
	if holder.Workload != "" {
		evidence += fmt.Sprintf("; the holder connects from %s %s", holder.Workload, holder.Pod)
	}
	return sdk.Finding{
		DetectorID: d.spec.ID, RuleID: RuleLockWaiters, Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
		Summary: fmt.Sprintf("store %s (%s) session %s holds a lock on %s that %d session(s) have waited on for %s",
			d.config.Component, f.Engine, w.Holder, w.Object, w.Waiters, w.Waited),
		Evidence:        evidence,
		PrimaryResource: f.Pod,
		Playbooks:       slices.Clone(d.spec.Playbooks),
		Fingerprint:     fmt.Sprintf("%s/%s/%s/%s/%s", d.spec.ID, RuleLockWaiters, f.Pod.Namespace, f.Pod.Name, w.Object),
		Metadata:        map[string]any{"component": d.config.Component, "engine": string(f.Engine), "lock_wait": w, "holder": holder},
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

package runtime

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/traffic"
)

// Verification member kinds.
const (
	memberProbe = "probe"
	memberLink  = "link"
)

// recoveryVerdict is what the one "recovered" test concludes for a closure.
type recoveryVerdict int

const (
	// recoveryVerified: every in-scope component recovered at operation
	// strength (or the application has no health probes).
	recoveryVerified recoveryVerdict = iota
	// recoveryPending: an SDO write has not settled, or an in-scope member has
	// not passed in a sustained way yet. Not a contradiction: wait.
	recoveryPending
	// recoveryWeak: every in-scope member that can pass did, but some in-scope
	// component is observed only at reachability strength, or not at all.
	recoveryWeak
)

// recoveryCheck is the verification of one closure: the record the outcome
// carries, the verdict, why it is not verified, and how long its members may
// take to pass.
type recoveryCheck struct {
	record  *contracts.RecoveryVerification
	verdict recoveryVerdict
	reason  string
	horizon time.Duration
}

// verificationInput is everything Recovered reads: the accepted map, the live
// view (traffic windows, workloads, Pods), the incident's findings and SDO's
// writes. It is a pure function of these.
type verificationInput struct {
	m        appmap.Map
	doc      string
	view     sdk.DetectionContext
	request  IncidentRequest
	result   *IncidentResult
	detected time.Time
	// opening is the set of finding keys the incident opened on; late joiners
	// are not in it.
	opening map[string]bool
	// stability is the closure stability window: members must keep passing
	// through it.
	stability time.Duration
}

type verificationMember struct {
	id, kind string
	scenario string
	link     traffic.Link
	window   string
}

type componentSet struct {
	name     string
	strength contracts.VerificationStrength
	members  []verificationMember
	inScope  bool
	edgeFrom string
}

// closureRecoveryCheck runs the recovered test for a closure about to commit.
// Its record is nil when nothing can be decided (no view or no readable
// accepted map): the closure then rests on its detectors.
func (c *Controller) closureRecoveryCheck(ctx context.Context, snapshot sdk.DetectionContext, closure IncidentClosure) recoveryCheck {
	if snapshot == nil || len(closure.FinalDetectorStates) == 0 {
		return recoveryCheck{}
	}
	read := c.gitRead
	if read == nil {
		return recoveryCheck{}
	}
	document, err := read(ctx, closure.Request.RepositoryWorktree, "show", closure.Request.SourceCommit+":"+layout.ArchitectureFile)
	if err != nil {
		return recoveryCheck{}
	}
	region, found, err := appmap.Find(string(document))
	if !found || err != nil {
		return recoveryCheck{}
	}
	m, err := appmap.Parse(region.YAML)
	if err != nil {
		return recoveryCheck{}
	}
	opening := map[string]bool{}
	late := lateJoinedFromTimeline(closure.DetectorTimeline)
	for _, finding := range closure.Request.Findings {
		key := FindingStateKey(finding.DetectorID, FindingFingerprint(finding))
		if _, joined := late[key]; !joined {
			opening[key] = true
		}
	}
	return recovered(verificationInput{m: m, doc: string(document), view: snapshot, request: closure.Request, result: closure.Result,
		detected: closure.DetectedAt, opening: opening, stability: closure.ClosureStabilityWindow})
}

// recovered is the one "recovered" test. Each component an incident finding
// faulted, or an SDO write touched, has a verification set: the probes of user
// operations that cover it (operation strength) or, when none do, the link
// dials into it (reachability strength). It recovered when every member passed
// in a sustained way since the later of detection and the settle time of SDO's
// last write. Only components an opening finding names or SDO wrote to decide
// the verdict; the others are recorded.
func recovered(in verificationInput) recoveryCheck {
	windows := map[string]traffic.Window{}
	if set, ok := in.view.(traffic.WindowSet); ok {
		windows = set.TrafficWindows()
	}
	record := &contracts.RecoveryVerification{}
	for _, window := range windows {
		record.Probed = record.Probed || window.Workload.Purpose == traffic.PurposeHealthProbe
	}
	if !record.Probed {
		// No health probes (review U4): the closure rests on its detectors.
		return recoveryCheck{record: record}
	}
	resolve := newComponentResolver(in.m, in.view, in.request.Namespace)
	coverage := in.m.Coverage(appmap.ParseInventory(in.doc), windowWorkloads(windows))
	sets := map[string]*componentSet{}
	get := func(name string) *componentSet {
		if sets[name] == nil {
			sets[name] = &componentSet{name: name}
		}
		return sets[name]
	}
	scope := newRecoveryScope(keysOf(in.opening))
	for _, finding := range in.request.Findings {
		if strings.HasPrefix(finding.DetectorID, "drift.") || !scope.holds(finding) {
			continue
		}
		name := resolve.component(finding.PrimaryResource)
		if name == "" {
			continue
		}
		set := get(name)
		if in.opening[FindingStateKey(finding.DetectorID, FindingFingerprint(finding))] {
			set.inScope = true
		}
		if strings.HasPrefix(finding.RuleID, traffic.LinkRuleIDPrefix) {
			if from, _ := finding.Metadata["from"].(string); from != "" {
				set.edgeFrom = resolve.byService[from]
			}
		}
	}
	writes, since, settled := sdoWrites(in, resolve)
	record.Since = timestamppb.New(since)
	record.WritesSettled = settled
	for _, w := range writes {
		record.Writes = append(record.Writes, &contracts.WriteSettlement{ApplicationId: w.id, SettledAt: optionalTimestamp(w.settledAt), Settled: w.settled})
		for _, name := range w.components {
			get(name).inScope = true
		}
	}
	names := make([]string, 0, len(sets))
	for name := range sets {
		names = append(names, name)
	}
	sort.Strings(names)
	check := recoveryCheck{record: record}
	var weak, pending []string
	for _, name := range names {
		set := sets[name]
		buildMembers(set, in.m, coverage, windows, resolve)
		verification := &contracts.ComponentVerification{Component: name, Strength: set.strength, InScope: set.inScope, EdgeFrom: set.edgeFrom, Recovered: settled}
		for _, member := range set.members {
			passed, detail, horizon := memberPassed(member, windows, since, in.stability)
			check.horizon = max(check.horizon, horizon)
			verification.Members = append(verification.Members, &contracts.VerificationMember{Id: member.id, Kind: member.kind, Passed: passed, Detail: detail})
			if !passed {
				verification.Recovered = false
				if set.inScope {
					pending = append(pending, fmt.Sprintf("%s: %s %s %s", name, member.kind, member.id, detail))
				}
			}
		}
		if set.strength == contracts.VerificationStrength_VERIFICATION_STRENGTH_UNOBSERVABLE {
			verification.Recovered = false
		}
		record.Components = append(record.Components, verification)
		if !set.inScope {
			continue
		}
		switch set.strength {
		case contracts.VerificationStrength_VERIFICATION_STRENGTH_REACHABILITY:
			weak = append(weak, name+": reachability only (no probe of a user operation covers it; its link dials passed)")
		case contracts.VerificationStrength_VERIFICATION_STRENGTH_UNOBSERVABLE:
			weak = append(weak, name+": no probe or link covers it")
		}
	}
	switch {
	case !settled:
		check.verdict, check.reason = recoveryPending, "an SDO write has not settled (its rollout is still pending)"
	case len(pending) > 0:
		check.verdict, check.reason = recoveryPending, "not recovered since SDO's last write settled: "+strings.Join(pending, "; ")
	case len(weak) > 0:
		check.verdict, check.reason = recoveryWeak, "recovery verified below operation strength: "+strings.Join(weak, "; ")
	}
	return check
}

// buildMembers fills a component's verification set and its strength. A held
// finding on one edge is verified by probes whose operation path crosses that
// edge, else by that edge's link dials. A probe the accepted map lists but no
// health-probe window runs any more (a later health commit removed it) is
// dropped, so the component falls back rather than waiting for it.
func buildMembers(set *componentSet, m appmap.Map, coverage appmap.Coverage, windows map[string]traffic.Window, resolve componentResolver) {
	running := map[string]string{}
	for name, window := range windows {
		if window.Workload.Purpose != traffic.PurposeHealthProbe {
			continue
		}
		for _, observed := range window.Scenarios {
			if !knownBrokenIn(window.Workload, observed.Scenario.ID) {
				running[observed.Scenario.ID] = name
			}
		}
	}
	probes := coverage.Components[set.name].Probes
	if set.edgeFrom != "" {
		probes = probesCrossing(m, coverage, set.edgeFrom, set.name)
	}
	for _, id := range probes {
		if window, ok := running[id]; ok {
			set.members = append(set.members, verificationMember{id: id, kind: memberProbe, scenario: id, window: window})
		}
	}
	if len(set.members) > 0 {
		set.strength = contracts.VerificationStrength_VERIFICATION_STRENGTH_OPERATION
		return
	}
	target := map[string]bool{}
	for _, svc := range resolve.services[set.name] {
		target[svc] = true
	}
	from := map[string]bool{}
	for _, svc := range resolve.services[set.edgeFrom] {
		from[svc] = true
	}
	seen := map[string]bool{}
	var names []string
	for name := range windows {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		window := windows[name]
		if window.Workload.Purpose != traffic.PurposeLinkProbe {
			continue
		}
		for _, observed := range window.Links {
			link := observed.Link
			if !target[link.To] || (set.edgeFrom != "" && !from[link.From]) || seen[link.ID()] {
				continue
			}
			seen[link.ID()] = true
			set.members = append(set.members, verificationMember{id: link.ID(), kind: memberLink, link: link, window: name})
		}
	}
	if len(set.members) > 0 {
		set.strength = contracts.VerificationStrength_VERIFICATION_STRENGTH_REACHABILITY
		return
	}
	set.strength = contracts.VerificationStrength_VERIFICATION_STRENGTH_UNOBSERVABLE
}

func knownBrokenIn(workload traffic.Workload, id string) bool {
	for _, scenario := range workload.Scenarios {
		if scenario.ID == id {
			return scenario.KnownBroken != nil
		}
	}
	return false
}

// probesCrossing lists the covering probes of to whose operation path holds
// from immediately before to: the probes that send a request over that edge.
func probesCrossing(m appmap.Map, coverage appmap.Coverage, from, to string) []string {
	ops := map[string]appmap.Operation{}
	for _, op := range m.Operations {
		ops[op.ID] = op
	}
	covering := map[string]bool{}
	for _, id := range coverage.Components[to].Probes {
		covering[id] = true
	}
	var out []string
	for _, probe := range m.Probes {
		if !covering[probe.Scenario] {
			continue
		}
		for _, id := range probe.Operations {
			path := ops[id].Path
			for i := 0; i+1 < len(path); i++ {
				if path[i] == from && path[i+1] == to && !containsString(out, probe.Scenario) {
					out = append(out, probe.Scenario)
				}
			}
		}
	}
	sort.Strings(out)
	return out
}

// memberPassed reports whether a member passed in a sustained way since
// since: a probe's samples since then meet its SLO, and so do those in the
// trailing stability window; a link's dials since then end in a run of
// successes as long as its failure threshold and show no unreachable,
// intermittent or slow judgement. horizon is how long the member needs to show
// it.
func memberPassed(member verificationMember, windows map[string]traffic.Window, since time.Time, stability time.Duration) (bool, string, time.Duration) {
	window, ok := windows[member.window]
	if !ok {
		return false, "has no observations", 0
	}
	if window.LoadError != "" {
		return false, window.LoadError, 0
	}
	if member.kind == memberLink {
		setting := window.LinkSetting()
		needed := max(setting.Failures, traffic.MinLinkFailures)
		horizon := time.Duration(needed+2) * setting.Period
		for _, observed := range window.Links {
			if observed.Link.ID() != member.link.ID() || observed.EffectiveVantage() != traffic.LinkVantageProber {
				continue
			}
			var fresh []traffic.LinkSample
			for _, sample := range observed.Samples {
				if !sample.At.Before(since) {
					fresh = append(fresh, sample)
				}
			}
			if len(fresh) < needed {
				return false, fmt.Sprintf("has %d of %d dials since the last write settled", len(fresh), needed), horizon
			}
			for _, sample := range fresh[len(fresh)-needed:] {
				if !sample.OK {
					return false, "failed a dial since the last write settled", horizon
				}
			}
			observed.Samples = fresh
			if judged, bad := traffic.JudgeLinkIn(setting, observed); bad {
				return false, fmt.Sprintf("is %s since the last write settled", judged.Condition), horizon
			}
			return true, "", horizon
		}
		return false, "has no observations", horizon
	}
	slo := window.Workload.ScenarioSLO(member.scenario)
	horizon := slo.MaxAge.Duration() + 2*time.Second
	if rate := window.Workload.RatePerSecond; rate > 0 {
		horizon = max(horizon, time.Duration(float64(slo.MinSamples)/rate*float64(time.Second))+2*time.Second)
	}
	for _, observed := range window.Scenarios {
		if observed.Scenario.ID != member.scenario {
			continue
		}
		if !observed.Qualified {
			return false, "is not qualified", horizon
		}
		var fresh, trailing []traffic.Sample
		for _, sample := range observed.Samples {
			if sample.At.Before(since) {
				continue
			}
			fresh = append(fresh, sample)
			if stability > 0 && !sample.At.Before(window.ObservedAt.Add(-stability)) {
				trailing = append(trailing, sample)
			}
		}
		verdict := traffic.Evaluate(member.scenario, slo, fresh, window.ObservedAt)
		switch {
		case !verdict.Evaluated:
			return false, fmt.Sprintf("has %d of %d samples since the last write settled", verdict.Samples, slo.MinSamples), horizon
		case !verdict.Healthy:
			return false, "violates its SLO since the last write settled: " + strings.Join(verdict.Violations, ", "), horizon
		}
		if trailed := traffic.Evaluate(member.scenario, slo, trailing, window.ObservedAt); trailed.Evaluated && !trailed.Healthy {
			return false, "violates its SLO in the stability window: " + strings.Join(trailed.Violations, ", "), horizon
		}
		if last := fresh[len(fresh)-1]; last.Outcome != traffic.OutcomeOK && last.Outcome != traffic.OutcomeInvalid {
			return false, "its latest sample failed", horizon
		}
		return true, "", horizon
	}
	return false, "has no observations", horizon
}

// sdoWrite is one SDO mutation in the incident: when it settled and the
// components it touched.
type sdoWrite struct {
	id         string
	settledAt  time.Time
	settled    bool
	components []string
}

// sdoWrites lists every SDO mutation of the incident (applied plans, rolled
// back or failed plans that changed objects, and successful repair actions),
// and since: the later of detection and the latest settle time. A write whose
// rollout is still pending, or whose workload is still rolling out, has not
// settled, and settled is then false.
func sdoWrites(in verificationInput, resolve componentResolver) (writes []sdoWrite, since time.Time, settled bool) {
	since, settled = in.detected, true
	if in.result == nil {
		return nil, since, true
	}
	for i, application := range in.result.PlanApplications {
		var refs []sdk.ObjectRef
		for _, effect := range application.Result.ObjectEffects {
			if effect.State != "not_applied" {
				refs = append(refs, effect.Ref)
			}
		}
		if !application.Result.Applied && !application.Result.RolledBack && len(refs) == 0 {
			continue
		}
		id := application.ID
		if id == "" {
			id = fmt.Sprintf("plan-application-%d", i)
		}
		// A rollout pending when the plan completed settles once Go sees the
		// written workloads Ready again; without a workload to watch it never
		// settles. Either way the settle time is never earlier than the
		// latest Ready transition of the written workloads' Pods.
		pendingAtCompletion := application.Pending || application.Result.RolloutPending != nil
		w := sdoWrite{id: id, settledAt: application.CompletedAt, settled: !pendingAtCompletion || len(refs) > 0}
		for _, ref := range refs {
			if name := resolve.component(ref); name != "" && !containsString(w.components, name) {
				w.components = append(w.components, name)
			}
			ready, rolling := resolve.readySince(ref)
			w.settled = w.settled && !rolling
			if ready.After(w.settledAt) {
				w.settledAt = ready
			}
		}
		writes = append(writes, w)
	}
	for _, action := range in.result.RepairActions {
		if !action.Success {
			continue
		}
		w := sdoWrite{id: action.ActionID, settledAt: action.CompletedAt, settled: true}
		for _, ref := range action.Resources {
			if name := resolve.component(ref); name != "" && !containsString(w.components, name) {
				w.components = append(w.components, name)
			}
			ready, rolling := resolve.readySince(ref)
			if rolling {
				w.settled = false
			}
			if ready.After(w.settledAt) {
				w.settledAt = ready
			}
		}
		writes = append(writes, w)
	}
	for _, w := range writes {
		settled = settled && w.settled
		if w.settledAt.After(since) {
			since = w.settledAt
		}
	}
	return writes, since, settled
}

// componentResolver joins an object reference to a map component: a
// workload or Service the map names, a Pod through its controller owner
// chain, or else through the Services selecting it.
type componentResolver struct {
	view       sdk.DetectionContext
	namespace  string
	byWorkload map[string]string
	byService  map[string]string
	services   map[string][]string
}

func newComponentResolver(m appmap.Map, view sdk.DetectionContext, namespace string) componentResolver {
	r := componentResolver{view: view, namespace: namespace, byWorkload: map[string]string{}, byService: map[string]string{}, services: map[string][]string{}}
	for _, comp := range m.Components {
		r.byWorkload[comp.Workload] = comp.Name
		for _, svc := range comp.Services {
			r.byService[svc] = comp.Name
		}
		r.services[comp.Name] = comp.Services
	}
	return r
}

func (r componentResolver) component(ref sdk.ObjectRef) string {
	if ref.Namespace != "" && ref.Namespace != r.namespace {
		return ""
	}
	switch ref.Kind {
	case "Service", "Endpoints", "EndpointSlice":
		return r.byService[ref.Name]
	case "Pod":
		pod, ok := r.pod(ref.Name)
		if !ok {
			return r.byPodName(ref.Name)
		}
		if workload := r.owner(pod.OwnerReferences); workload != "" {
			if name := r.byWorkload[workload]; name != "" {
				return name
			}
		}
		return r.selecting(pod.Labels)
	case "ReplicaSet":
		for _, rs := range r.view.ReplicaSets() {
			if rs.Namespace == r.namespace && rs.Name == ref.Name {
				if name := r.byWorkload[r.owner(rs.OwnerReferences)]; name != "" {
					return name
				}
			}
		}
	}
	return r.byWorkload[ref.Kind+"/"+ref.Name]
}

// owner resolves a controller owner reference to the workload ("Kind/name")
// that runs the Pods, following a ReplicaSet to its Deployment.
func (r componentResolver) owner(owners []metav1.OwnerReference) string {
	for _, owner := range owners {
		if owner.Controller == nil || !*owner.Controller {
			continue
		}
		if owner.Kind == "ReplicaSet" {
			for _, rs := range r.view.ReplicaSets() {
				if rs.Namespace == r.namespace && rs.Name == owner.Name {
					if up := r.owner(rs.OwnerReferences); up != "" {
						return up
					}
				}
			}
		}
		return owner.Kind + "/" + owner.Name
	}
	return ""
}

// byPodName resolves a Pod that no longer exists (a write deleted it) by its
// generated name: the component whose workload name, followed by "-", is the
// longest prefix of the Pod's name.
func (r componentResolver) byPodName(pod string) string {
	best, component := 0, ""
	for kindName, name := range r.byWorkload {
		_, workload, _ := strings.Cut(kindName, "/")
		if strings.HasPrefix(pod, workload+"-") && len(workload) > best {
			best, component = len(workload), name
		}
	}
	return component
}

func (r componentResolver) pod(name string) (corev1.Pod, bool) {
	for _, pod := range r.view.Pods() {
		if pod.Namespace == r.namespace && pod.Name == name {
			return pod, true
		}
	}
	return corev1.Pod{}, false
}

func (r componentResolver) selecting(podLabels map[string]string) string {
	if len(podLabels) == 0 {
		return ""
	}
	for _, service := range r.view.Services() {
		if service.Namespace == r.namespace && len(service.Spec.Selector) > 0 && labels.SelectorFromSet(service.Spec.Selector).Matches(labels.Set(podLabels)) {
			if name := r.byService[service.Name]; name != "" {
				return name
			}
		}
	}
	return ""
}

// readySince is the latest Ready transition among the current Pods of the
// workload a written object belongs to, and whether that workload is still
// rolling out (a Pod not Ready, or fewer updated replicas than desired).
func (r componentResolver) readySince(ref sdk.ObjectRef) (time.Time, bool) {
	component := r.component(ref)
	if component == "" {
		return time.Time{}, false
	}
	var workload string
	for kindName, name := range r.byWorkload {
		if name == component {
			workload = kindName
		}
	}
	var latest time.Time
	rolling := false
	for _, pod := range r.view.Pods() {
		if pod.Namespace != r.namespace || pod.DeletionTimestamp != nil || r.owner(pod.OwnerReferences) != workload {
			continue
		}
		ready := false
		for _, condition := range pod.Status.Conditions {
			if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
				ready = true
				if condition.LastTransitionTime.After(latest) {
					latest = condition.LastTransitionTime.Time
				}
			}
		}
		rolling = rolling || !ready
	}
	kind, name, _ := strings.Cut(workload, "/")
	switch kind {
	case "Deployment":
		for _, d := range r.view.Deployments() {
			if d.Namespace == r.namespace && d.Name == name && d.Spec.Replicas != nil && d.Status.UpdatedReplicas < *d.Spec.Replicas {
				rolling = true
			}
		}
	case "StatefulSet":
		for _, s := range r.view.StatefulSets() {
			if s.Namespace == r.namespace && s.Name == name && s.Spec.Replicas != nil && s.Status.UpdatedReplicas < *s.Spec.Replicas && s.Status.UpdateRevision != "" && s.Status.CurrentRevision != s.Status.UpdateRevision {
				rolling = true
			}
		}
	}
	return latest, rolling
}

// windowWorkloads is what the map's coverage reads of the running probe
// workloads: their scenario IDs and knownBroken declarations.
func windowWorkloads(windows map[string]traffic.Window) []appmap.Workload {
	var out []appmap.Workload
	for name, window := range windows {
		w := appmap.Workload{Name: name}
		for _, scenario := range window.Workload.Scenarios {
			w.Scenarios = append(w.Scenarios, scenario.ID)
			if scenario.KnownBroken != nil {
				w.KnownBroken = append(w.KnownBroken, scenario.ID)
			}
		}
		out = append(out, w)
	}
	return out
}

func keysOf(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for key := range set {
		out = append(out, key)
	}
	return out
}

func containsString(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}

package runtime

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/traffic"
)

// recoveryVerdict is what Recovered concludes.
type recoveryVerdict int

const (
	// recoveryVerified: every opening user test passed in a sustained way
	// since SDO's last write settled.
	recoveryVerified recoveryVerdict = iota
	// recoveryPending: an SDO write has not settled, or an opening user test
	// has not passed in a sustained way yet. Not a contradiction: wait.
	recoveryPending
)

// recoveryCheck is one evaluation of Recovered: the record the outcome
// carries, the verdict, why it does not hold, and how long its user tests may
// take to show it.
type recoveryCheck struct {
	record  *contracts.RecoveryVerification
	verdict recoveryVerdict
	reason  string
	horizon time.Duration
}

// verificationInput is the live view, the incident alert set and SDO writes.
type verificationInput struct {
	view     sdk.DetectionContext
	request  IncidentRequest
	result   *IncidentResult
	detected time.Time
	// stability is the closure stability window: opening user tests must keep
	// passing through it.
	stability time.Duration
}

// closureRecoveryCheck evaluates Recovered for a closure about to commit.
func (c *Controller) closureRecoveryCheck(ctx context.Context, snapshot sdk.DetectionContext, closure IncidentClosure) recoveryCheck {
	return c.recoveryCheck(ctx, snapshot, closure.Request, closure.Result, closure.DetectedAt, closure.ClosureStabilityWindow)
}

// recoveryCheck evaluates Recovered for the open incident on snapshot.
func (c *Controller) recoveryCheck(ctx context.Context, snapshot sdk.DetectionContext, request IncidentRequest, result *IncidentResult, detected time.Time, stability time.Duration) recoveryCheck {
	if snapshot == nil {
		return recoveryCheck{verdict: recoveryVerified}
	}
	return recovered(verificationInput{view: snapshot, request: request, result: result, detected: detected, stability: stability})
}

// acceptedVerificationMap reads the application map at the incident's
// accepted commit. An unreadable or absent map is the empty map.
func acceptedVerificationMap(ctx context.Context, read gitRead, request IncidentRequest) (appmap.Map, string) {
	if read == nil || request.SourceCommit == "" {
		return appmap.Map{}, ""
	}
	document, err := read(ctx, request.RepositoryWorktree, "show", request.SourceCommit+":"+layout.ArchitectureFile)
	if err != nil {
		return appmap.Map{}, ""
	}
	region, found, err := appmap.Find(string(document))
	if !found || err != nil {
		return appmap.Map{}, ""
	}
	m, err := appmap.Parse(region.YAML)
	if err != nil {
		return appmap.Map{}, ""
	}
	return m, string(document)
}

// openingUserTest names the probe workload and scenario of an opening key
// that is a user test, or reports false for a Kubernetes condition.
func openingUserTest(request IncidentRequest, key string) (workload, scenario string, ok bool) {
	for _, finding := range request.Findings {
		if FindingStateKey(finding.DetectorID, FindingFingerprint(finding)) != key {
			continue
		}
		workload, isTraffic := strings.CutPrefix(finding.DetectorID, memory.TrafficDetectorPrefix)
		scenario, isScenario := strings.CutPrefix(finding.RuleID, traffic.RuleIDPrefix)
		return workload, scenario, isTraffic && isScenario
	}
	return "", "", false
}

// recovered is Recovered, the one predicate that closes an incident, clears
// the automatic stage and ends the responder session: no SDO write is still
// rolling out, and every user test of the incident's alert set (opening
// alerts and joined tests, alertKeys) passed in a sustained way since
// the later of detection and the settle time of SDO's last write (fresh
// samples meet its SLO, the trailing stability window too, the latest sample
// is good). An opening Kubernetes condition recovers when its finding is
// inactive with a clear streak of the stability window; the caller decides
// that from detector history (closure) or the stage's live findings.
func recovered(in verificationInput) recoveryCheck {
	windows := map[string]traffic.Window{}
	if set, ok := in.view.(traffic.WindowSet); ok {
		windows = set.TrafficWindows()
	}
	opening := openingKeys(&in.request)
	record := &contracts.RecoveryVerification{OpeningKeys: append([]string(nil), opening...), JoinedKeys: append([]string(nil), in.request.JoinedKeys...)}
	check := recoveryCheck{record: record}
	writes, since, settled := sdoWrites(in)
	record.Since = timestamppb.New(since)
	record.WritesSettled = settled
	for _, w := range writes {
		record.Writes = append(record.Writes, &contracts.WriteSettlement{ApplicationId: w.id, SettledAt: optionalTimestamp(w.settledAt), Settled: w.settled})
	}
	var pending []string
	for _, key := range alertKeys(&in.request) {
		workload, scenario, ok := openingUserTest(in.request, key)
		if !ok {
			if detector, fingerprint, found := strings.Cut(key, "\x00"); found &&
				strings.HasPrefix(detector, memory.TrafficDetectorPrefix) && strings.HasPrefix(fingerprint, detector+"/"+traffic.RuleIDPrefix) {
				pending = append(pending, "missing user test identity for "+detector)
			}
			continue
		}
		passed, detail, horizon := memberPassed(windows[workload], scenario, since, in.stability)
		check.horizon = max(check.horizon, horizon)
		if !passed {
			pending = append(pending, fmt.Sprintf("user test %s %s", scenario, detail))
		}
	}
	switch {
	case !settled:
		check.verdict, check.reason = recoveryPending, "an SDO write has not settled (its rollout is still pending)"
	case len(pending) > 0:
		check.verdict, check.reason = recoveryPending, "not recovered since SDO's last write settled: "+strings.Join(pending, "; ")
	default:
		if observed := sdk.ObservationTime(in.view); !observed.IsZero() {
			record.StableSince = timestamppb.New(observed)
		}
	}
	return check
}

// memberPassed reports whether a user test passed in a sustained way since
// since: its samples since then meet its SLO, and so do those in the trailing
// stability window, and its latest sample is good. horizon is how long the
// test needs to show it.
func memberPassed(window traffic.Window, scenario string, since time.Time, stability time.Duration) (bool, string, time.Duration) {
	if window.Workload.Name == "" && len(window.Scenarios) == 0 {
		return false, "has no observations", 0
	}
	if window.LoadError != "" {
		return false, window.LoadError, 0
	}
	slo := window.Workload.ScenarioSLO(scenario)
	horizon := slo.MaxAge.Duration() + 2*time.Second
	if rate := window.Workload.RatePerSecond; rate > 0 {
		horizon = max(horizon, time.Duration(float64(slo.MinSamples)/rate*float64(time.Second))+2*time.Second)
	}
	for _, observed := range window.Scenarios {
		if observed.Scenario.ID != scenario {
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
		verdict := traffic.Evaluate(scenario, slo, fresh, window.ObservedAt)
		switch {
		case !verdict.Evaluated:
			return false, fmt.Sprintf("has %d of %d samples since the last write settled", verdict.Samples, slo.MinSamples), horizon
		case !verdict.Healthy:
			return false, "violates its SLO since the last write settled: " + strings.Join(verdict.Violations, ", "), horizon
		}
		if trailed := traffic.Evaluate(scenario, slo, trailing, window.ObservedAt); trailed.Evaluated && !trailed.Healthy {
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
	id        string
	settledAt time.Time
	settled   bool
}

// sdoWrites lists every SDO mutation of the incident (applied plans, rolled
// back or failed plans that changed objects, and successful repair actions),
// and since: the later of detection and the latest settle time. A write whose
// rollout is still pending, or whose workload is still rolling out, has not
// settled, and settled is then false.
func sdoWrites(in verificationInput) (writes []sdoWrite, since time.Time, settled bool) {
	since, settled = in.detected, true
	if in.result == nil {
		return nil, since, true
	}
	for i, application := range in.result.PlanApplications {
		refs := recoveryApplicationRefs(application)
		pendingAtCompletion := application.Pending || application.Result.RolloutPending != nil
		if !application.Result.Applied && !application.Result.RolledBack && len(refs) == 0 {
			continue
		}
		id := application.ID
		if id == "" {
			id = fmt.Sprintf("plan-application-%d", i)
		}
		w := sdoWrite{id: id, settledAt: application.CompletedAt, settled: !pendingAtCompletion || len(refs) > 0}
		for _, ref := range refs {
			w.settled = w.settled && !writeRevisionPending(in.view, ref, pendingAtCompletion)
		}
		writes = append(writes, w)
	}
	for _, action := range in.result.RepairActions {
		if !action.Success {
			continue
		}
		w := sdoWrite{id: action.ActionID, settledAt: action.CompletedAt, settled: true}
		for _, ref := range action.Resources {
			w.settled = w.settled && !writeRevisionPending(in.view, ref, false)
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

// recoveryApplicationRefs are the resources written by the application. A
// pending receipt may predate the executor's readback of an action.
func recoveryApplicationRefs(application RecordedPlanApplication) []sdk.ObjectRef {
	var refs []sdk.ObjectRef
	for _, effect := range application.Result.ObjectEffects {
		if effect.State != "not_applied" {
			refs = append(refs, effect.Ref)
		}
	}
	actions := application.Result.ActionsApplied
	if len(actions) == 0 && (application.Pending || application.Result.RolloutPending != nil) &&
		(application.Result.Applied || application.Result.RolledBack) {
		actions = application.Result.Plan.Actions
	}
	for _, action := range actions {
		if ref, err := planActionRef(action); err == nil && ref.Name != "" {
			refs = append(refs, ref)
		}
	}
	return refs
}

// A workload write settles when the workload controller has observed the
// generation and, for a pending rollout, started the intended revision.
// Current Pod availability and Ready transitions are health observations.
func writeRevisionPending(view sdk.DetectionContext, ref sdk.ObjectRef, rolloutPending bool) bool {
	switch ref.Kind {
	case "Deployment":
		for _, d := range view.Deployments() {
			if d.Namespace == ref.Namespace && d.Name == ref.Name {
				return d.Status.ObservedGeneration < d.Generation ||
					rolloutPending && deploymentDesiredReplicas(&d) > 0 && d.Status.UpdatedReplicas == 0
			}
		}
	case "StatefulSet":
		for _, s := range view.StatefulSets() {
			if s.Namespace == ref.Namespace && s.Name == ref.Name {
				return s.Status.ObservedGeneration < s.Generation ||
					rolloutPending && s.Status.UpdateRevision != s.Status.CurrentRevision && s.Status.UpdatedReplicas == 0
			}
		}
	case "DaemonSet":
		for _, d := range view.DaemonSets() {
			if d.Namespace == ref.Namespace && d.Name == ref.Name {
				return d.Status.ObservedGeneration < d.Generation ||
					rolloutPending && d.Status.DesiredNumberScheduled > 0 && d.Status.UpdatedNumberScheduled == 0
			}
		}
	}
	return false
}

// stageRecoveryCheck is Recovered for the automatic stage's wait and the
// claim-time check: the incident's opening alerts and the loop's recorded
// writes, observed on view.
func (l *ResponderLoop) stageRecoveryCheck(ctx context.Context, view sdk.DetectionContext, state *IncidentState) recoveryCheck {
	result := IncidentResult{PlanApplications: l.applicationEvidenceLocked()}
	return recovered(verificationInput{view: view, request: state.Request, result: &result, detected: state.Request.DetectedAt})
}

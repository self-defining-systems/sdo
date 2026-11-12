package runtime

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
)

// A synthetic app: the exposed front calls api, api calls db and cache. One
// user operation enters at front and reaches front, api and db; nothing
// covers cache, which only the link api -> cache observes. ssh-gate is on no
// user path and has no link.
const verifyArch = `# Manifest inventory

- Service/app/front (source: k8s/front.yaml)
  - exposure:NodePort
- Service/app/api (source: k8s/api.yaml)
- Service/app/db (source: k8s/db.yaml)
- Service/app/cache (source: k8s/cache.yaml)
- Service/app/ssh-gate (source: k8s/ssh.yaml)

<!-- sdo:application-map:begin -->
` + "```yaml" + `
version: 1
components:
  - {name: front, workload: Deployment/front, services: [front], holds_data: false}
  - {name: api, workload: Deployment/api, services: [api], holds_data: false}
  - {name: db, workload: StatefulSet/db, services: [db], holds_data: true}
  - {name: cache, workload: Deployment/cache, services: [cache], holds_data: false, unprobed: warmed at start only}
  - {name: ssh-gate, workload: Deployment/ssh-gate, services: [ssh-gate], holds_data: false}
edges:
  - {from: front, to: api, evidence: [config], source: x}
  - {from: api, to: db, evidence: [config], source: x}
  - {from: api, to: cache, evidence: [config], source: x}
operations:
  - id: browse
    writes: false
    entry: {service: front, port: 80, method: GET, path: /items}
    path: [front, api, db]
probes:
  - {scenario: browse-items, kind: call, operations: [browse]}
` + "```" + `
<!-- sdo:application-map:end -->
`

var verifyT0 = time.Date(2026, 11, 12, 3, 0, 0, 0, time.UTC)

func samplesEverySecond(from, to time.Time, ok func(time.Time) bool) []traffic.Sample {
	var out []traffic.Sample
	for at := from; !at.After(to); at = at.Add(time.Second) {
		outcome := traffic.OutcomeOK
		if !ok(at) {
			outcome = traffic.OutcomeError
		}
		out = append(out, traffic.Sample{At: at, Outcome: outcome, Latency: 10 * time.Millisecond})
	}
	return out
}

func dialsEverySecond(from, to time.Time, ok func(time.Time) bool) []traffic.LinkSample {
	var out []traffic.LinkSample
	for at := from; !at.After(to); at = at.Add(time.Second) {
		out = append(out, traffic.LinkSample{At: at, OK: ok(at), Duration: time.Millisecond})
	}
	return out
}

func always(time.Time) bool { return true }

// verifyWorld is the live view: Pods of each component and the traffic
// windows, observed at now.
type verifyWorld struct {
	now        time.Time
	probeOK    func(time.Time) bool
	links      map[string]func(time.Time) bool
	noProbes   bool
	noScenario bool
}

func (w verifyWorld) view() sdktest.Snapshot {
	controller := true
	pod := func(name, owner, ownerKind string) corev1.Pod {
		return corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app", Labels: map[string]string{"app": owner},
			OwnerReferences: []metav1.OwnerReference{{Kind: ownerKind, Name: owner, Controller: &controller}}},
			Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(verifyT0.Add(-time.Hour))}}}}
	}
	snapshot := sdktest.Snapshot{NamespaceName: "app",
		PodList:         []corev1.Pod{pod("front-1", "front", "Deployment"), pod("api-1", "api", "Deployment"), pod("db-0", "db", "StatefulSet"), pod("cache-1", "cache", "Deployment"), pod("ssh-gate-1", "ssh-gate", "Deployment")},
		StatefulSetList: []appsv1.StatefulSet{{ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: "app"}}},
		Traffic:         map[string]traffic.Window{},
	}
	if !w.noProbes {
		probeOK := w.probeOK
		if probeOK == nil {
			probeOK = always
		}
		window := traffic.Window{Workload: traffic.Workload{Name: "probe-call", Purpose: traffic.PurposeHealthProbe, RatePerSecond: 1,
			Scenarios: []traffic.WorkloadScenario{{ID: "browse-items"}}}, ObservedAt: w.now}
		if !w.noScenario {
			window.Scenarios = []traffic.ScenarioObservations{{Scenario: traffic.Descriptor{ID: "browse-items", Target: traffic.Target{Service: "front", Port: 80}, DependsOn: []string{"front", "api", "db"}},
				Qualified: true, Samples: samplesEverySecond(verifyT0, w.now, probeOK)}}
		}
		snapshot.Traffic["probe-call"] = window
	}
	var observed []traffic.LinkObservations
	for _, edge := range []struct{ from, to string }{{"api", "cache"}, {"front", "api"}} {
		link := traffic.Link{From: edge.from, To: edge.to, Port: 80, Protocol: "tcp"}
		ok := always
		if f := w.links[link.ID()]; f != nil {
			ok = f
		}
		observed = append(observed, traffic.LinkObservations{Link: link, Qualified: true, Samples: dialsEverySecond(verifyT0, w.now, ok)})
	}
	snapshot.Traffic["links"] = traffic.Window{Workload: traffic.Workload{Name: "links", Purpose: traffic.PurposeLinkProbe, Interval: traffic.Duration(time.Second), Timeout: traffic.Duration(time.Second)}, ObservedAt: w.now, Links: observed}
	return snapshot
}

func verifyFinding(detector, kind, name string, severity sdk.FindingSeverity) sdk.Finding {
	return sdk.Finding{DetectorID: detector, RuleID: "synthetic", Status: sdk.FindingActive, Severity: severity,
		PrimaryResource: sdk.ObjectRef{Kind: kind, Namespace: "app", Name: name}}
}

// runVerify is recovered() over the synthetic map: findings[:opening] opened
// the incident, the rest joined later.
func runVerify(t *testing.T, world verifyWorld, findings []sdk.Finding, opening int, result *IncidentResult) recoveryCheck {
	t.Helper()
	region, found, err := appmap.Find(verifyArch)
	if !found || err != nil {
		t.Fatal(err)
	}
	m, err := appmap.Parse(region.YAML)
	if err != nil {
		t.Fatal(err)
	}
	keys := map[string]bool{}
	for _, finding := range findings[:opening] {
		keys[FindingStateKey(finding.DetectorID, FindingFingerprint(finding))] = true
	}
	return recovered(verificationInput{m: m, doc: verifyArch, view: world.view(), request: IncidentRequest{Namespace: "app", Findings: findings},
		result: result, detected: verifyT0, opening: keys, stability: 10 * time.Second})
}

func componentOf(t *testing.T, check recoveryCheck, name string) *contracts.ComponentVerification {
	t.Helper()
	for _, component := range check.record.GetComponents() {
		if component.GetComponent() == name {
			return component
		}
	}
	t.Fatalf("no verification for component %s: %v", name, check.record)
	return nil
}

func restartOf(kind, name string, completed time.Time) *IncidentResult {
	return &IncidentResult{PlanApplications: []RecordedPlanApplication{{ID: "app-1", CompletedAt: completed,
		Result: PlanApplicationResult{Applied: true, ObjectEffects: []PlanObjectEffect{{Operation: "patch", Ref: sdk.ObjectRef{Kind: kind, Namespace: "app", Name: name}, State: "applied"}}}}}}
}

// (c) A store finding whose component a user operation through the entry
// covers verifies once that probe passes, at operation strength.
func TestStoreFindingCoveredByAnEntryOperationVerifies(t *testing.T) {
	now := verifyT0.Add(2 * time.Minute)
	check := runVerify(t, verifyWorld{now: now}, []sdk.Finding{verifyFinding("store-state", "StatefulSet", "db", sdk.SeverityCritical)}, 1, restartOf("StatefulSet", "db", verifyT0.Add(30*time.Second)))
	if check.verdict != recoveryVerified {
		t.Fatalf("verdict %v: %s", check.verdict, check.reason)
	}
	db := componentOf(t, check, "db")
	if db.GetStrength() != contracts.VerificationStrength_VERIFICATION_STRENGTH_OPERATION || !db.GetRecovered() || !db.GetInScope() || len(db.GetMembers()) != 1 || db.GetMembers()[0].GetId() != "browse-items" {
		t.Fatalf("db = %v", db)
	}
}

// (e) A probe that passed only before SDO's last write settled does not
// verify: it failed after the write.
func TestProbePassingOnlyBeforeTheWriteSettledDoesNotVerify(t *testing.T) {
	write := verifyT0.Add(30 * time.Second)
	now := write.Add(time.Minute)
	failing := func(at time.Time) bool { return at.Before(write) }
	check := runVerify(t, verifyWorld{now: now, probeOK: failing}, []sdk.Finding{verifyFinding("store-state", "StatefulSet", "db", sdk.SeverityCritical)}, 1, restartOf("StatefulSet", "db", write))
	if check.verdict != recoveryPending || !strings.Contains(check.reason, "browse-items") {
		t.Fatalf("verdict %v: %s", check.verdict, check.reason)
	}
	if check.record.GetSince().AsTime() != write {
		t.Fatalf("since = %s, want the write's settle time %s", check.record.GetSince().AsTime(), write)
	}
}

// A flapping member (passes, then fails again) does not verify.
func TestFlappingMemberDoesNotVerify(t *testing.T) {
	write := verifyT0.Add(30 * time.Second)
	now := write.Add(time.Minute)
	flapping := func(at time.Time) bool { return at.Before(now.Add(-3 * time.Second)) }
	check := runVerify(t, verifyWorld{now: now, probeOK: flapping}, []sdk.Finding{verifyFinding("store-state", "StatefulSet", "db", sdk.SeverityCritical)}, 1, restartOf("StatefulSet", "db", write))
	if check.verdict != recoveryPending {
		t.Fatalf("a member failing again after passing verified: %v %s", check.verdict, check.reason)
	}
}

// (d) A component only links observe recovers at reachability strength: the
// closure is weak, and the record says so.
func TestLinkOnlyComponentIsReachabilityStrength(t *testing.T) {
	now := verifyT0.Add(2 * time.Minute)
	check := runVerify(t, verifyWorld{now: now}, []sdk.Finding{verifyFinding("workload-health", "Deployment", "cache", sdk.SeverityCritical)}, 1, nil)
	if check.verdict != recoveryWeak || !strings.Contains(check.reason, "cache: reachability only") {
		t.Fatalf("verdict %v: %s", check.verdict, check.reason)
	}
	cache := componentOf(t, check, "cache")
	if cache.GetStrength() != contracts.VerificationStrength_VERIFICATION_STRENGTH_REACHABILITY || !cache.GetRecovered() || cache.GetMembers()[0].GetKind() != memberLink {
		t.Fatalf("cache = %v", cache)
	}
}

// A component nothing observes is unobservable, and the closure is weak.
func TestUnobservableComponentIsWeak(t *testing.T) {
	now := verifyT0.Add(2 * time.Minute)
	check := runVerify(t, verifyWorld{now: now}, []sdk.Finding{verifyFinding("workload-health", "Pod", "ssh-gate-1", sdk.SeverityCritical)}, 1, nil)
	if check.verdict != recoveryWeak || componentOf(t, check, "ssh-gate").GetStrength() != contracts.VerificationStrength_VERIFICATION_STRENGTH_UNOBSERVABLE {
		t.Fatalf("verdict %v: %s %v", check.verdict, check.reason, check.record)
	}
}

// (a, review C3) A critical finding that joins the incident on a component SDO
// never touched is a clue: it does not downgrade the closure the opening
// finding decides, and adds no component to the record.
func TestJoinedCriticalFindingOnAnUntouchedComponentDoesNotDowngrade(t *testing.T) {
	now := verifyT0.Add(2 * time.Minute)
	findings := []sdk.Finding{
		verifyFinding("store-state", "StatefulSet", "db", sdk.SeverityCritical),
		verifyFinding("workload-health", "Pod", "ssh-gate-1", sdk.SeverityCritical),
	}
	check := runVerify(t, verifyWorld{now: now}, findings, 1, restartOf("StatefulSet", "db", verifyT0.Add(30*time.Second)))
	if check.verdict != recoveryVerified {
		t.Fatalf("verdict %v: %s", check.verdict, check.reason)
	}
	for _, component := range check.record.GetComponents() {
		if component.GetComponent() == "ssh-gate" {
			t.Fatalf("a joined clue's component was recorded: %v", component)
		}
	}
	// The same component written by SDO is in scope.
	result := restartOf("StatefulSet", "db", verifyT0.Add(30*time.Second))
	result.PlanApplications[0].Result.ObjectEffects = append(result.PlanApplications[0].Result.ObjectEffects, PlanObjectEffect{Operation: "delete", Ref: sdk.ObjectRef{Kind: "Pod", Namespace: "app", Name: "ssh-gate-7f9c-x2"}, State: "applied"})
	if check := runVerify(t, verifyWorld{now: now}, findings, 1, result); check.verdict != recoveryWeak || !componentOf(t, check, "ssh-gate").GetInScope() {
		t.Fatalf("a component SDO wrote is in scope: %v %s", check.verdict, check.reason)
	}
}

// (b) A related resource is context, not a fault location: a finding on the
// covered db that names the uncovered cache as related leaves the close at
// operation strength.
func TestRelatedResourceAddsNoComponent(t *testing.T) {
	now := verifyT0.Add(2 * time.Minute)
	finding := verifyFinding("store-state", "StatefulSet", "db", sdk.SeverityCritical)
	finding.RelatedResources = []sdk.ObjectRef{{Kind: "Pod", Namespace: "app", Name: "cache-1"}}
	check := runVerify(t, verifyWorld{now: now}, []sdk.Finding{finding}, 1, nil)
	if check.verdict != recoveryVerified || len(check.record.GetComponents()) != 1 {
		t.Fatalf("verdict %v: %s %v", check.verdict, check.reason, check.record)
	}
}

// (g) A probe the accepted map lists that no window runs any more (a later
// health commit removed it) is dropped: the component falls back to its link
// dials instead of waiting.
func TestRemovedProbeFallsBackToLinks(t *testing.T) {
	now := verifyT0.Add(2 * time.Minute)
	check := runVerify(t, verifyWorld{now: now, noScenario: true}, []sdk.Finding{verifyFinding("workload-health", "Deployment", "api", sdk.SeverityCritical)}, 1, nil)
	api := componentOf(t, check, "api")
	if api.GetStrength() != contracts.VerificationStrength_VERIFICATION_STRENGTH_REACHABILITY || check.verdict != recoveryWeak {
		t.Fatalf("api = %v (verdict %v %s)", api, check.verdict, check.reason)
	}
}

// A held finding on one edge is verified by that edge: a probe that covers
// both ends without crossing the edge does not count, the edge's links do.
func TestEdgeFindingIsVerifiedByThatEdge(t *testing.T) {
	now := verifyT0.Add(2 * time.Minute)
	finding := verifyFinding("links", "Service", "cache", sdk.SeverityCritical)
	finding.RuleID = traffic.LinkRuleIDPrefix + "api->cache"
	finding.Metadata = map[string]any{"from": "api", "to": "cache"}
	broken := verifyWorld{now: now, links: map[string]func(time.Time) bool{traffic.Link{From: "api", To: "cache", Port: 80, Protocol: "tcp"}.ID(): func(time.Time) bool { return false }}}
	check := runVerify(t, broken, []sdk.Finding{finding}, 1, nil)
	cache := componentOf(t, check, "cache")
	if cache.GetEdgeFrom() != "api" || cache.GetRecovered() || check.verdict != recoveryPending {
		t.Fatalf("cache = %v (verdict %v %s)", cache, check.verdict, check.reason)
	}
}

// An unsettled write (its workload still rolling out) holds the verdict.
func TestUnsettledWriteHoldsRecovery(t *testing.T) {
	now := verifyT0.Add(2 * time.Minute)
	world := verifyWorld{now: now}
	result := restartOf("Deployment", "api", verifyT0.Add(30*time.Second))
	region, _, _ := appmap.Find(verifyArch)
	m, _ := appmap.Parse(region.YAML)
	view := world.view()
	view.PodList[1].Status.Conditions[0].Status = corev1.ConditionFalse
	check := recovered(verificationInput{m: m, doc: verifyArch, view: view, request: IncidentRequest{Namespace: "app", Findings: []sdk.Finding{verifyFinding("workload-health", "Deployment", "api", sdk.SeverityCritical)}},
		result: result, detected: verifyT0, opening: map[string]bool{FindingStateKey("workload-health", FindingFingerprint(verifyFinding("workload-health", "Deployment", "api", sdk.SeverityCritical))): true}})
	if check.verdict != recoveryPending || check.record.GetWritesSettled() {
		t.Fatalf("verdict %v: %s", check.verdict, check.reason)
	}
}

// (review U4) An application with no health probes is outside the test: its
// closures rest on its detectors, as before.
func TestNoHealthProbesIsOutsideTheTest(t *testing.T) {
	check := runVerify(t, verifyWorld{now: verifyT0.Add(time.Minute), noProbes: true}, []sdk.Finding{verifyFinding("workload-health", "Deployment", "cache", sdk.SeverityCritical)}, 1, nil)
	if check.verdict != recoveryVerified || check.record.GetProbed() {
		t.Fatalf("verdict %v: %v", check.verdict, check.record)
	}
}

// The closure asks the one recovered test. A finding on a component only links
// observe commits unconfirmed with the reason and the per-component record,
// and reflection may not learn from it; a finding on a component a passing
// user-operation probe covers commits verified, with the record.
func TestClosureCommitsTheRecoveredTestsVerdict(t *testing.T) {
	ctx := t.Context()
	for name, test := range map[string]struct {
		finding  sdk.Finding
		want     contracts.ClosureUnconfirmedReason
		strength contracts.VerificationStrength
	}{
		"reachability only": {verifyFinding("workload-health", "Service", "cache", sdk.SeverityCritical), contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_OBSERVATION_INCOMPLETE, contracts.VerificationStrength_VERIFICATION_STRENGTH_REACHABILITY},
		"operation covered": {verifyFinding("workload-health", "Service", "db", sdk.SeverityCritical), contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_UNSPECIFIED, contracts.VerificationStrength_VERIFICATION_STRENGTH_OPERATION},
	} {
		t.Run(name, func(t *testing.T) {
			now := verifyT0.Add(2 * time.Minute)
			after := verifyT0.Add(time.Minute)
			cfg := testControllerConfig()
			cfg.Namespace = "app"
			world := &sameWriteWorld{}
			world.healthy.Store(true)
			controller, err := NewController(cfg, []sdk.Detector{sameWriteDetector{world: world}}, riskSnapshots{view: verifyWorld{now: now}.view()}, &recordingDispatcher{}, after)
			if err != nil {
				t.Fatal(err)
			}
			controller.now = func() time.Time { return now }
			controller.gitRead = fakeArchitecture(verifyArch)
			controller.broker = &riskBroker{}
			result := completedResult("synthetic-incident")
			result.Timing = TimingMetrics{StartedAt: verifyT0, CompletedAt: after}
			request := IncidentRequest{SchemaVersion: ProtocolSchemaVersion, Application: "demo", Namespace: "app", IncidentID: "synthetic-incident", SourceCommit: "synthetic-accepted", RepositoryWorktree: "synthetic-worktree",
				Findings: []sdk.Finding{test.finding}}
			controller.pendingClosure = &IncidentClosure{Request: request, Result: &result, DetectedAt: verifyT0, DispatchedAt: verifyT0, ResponderCompletedAt: after, VerifiedAt: after,
				FinalDetectorStates: []DetectorEvaluation{{DetectorID: "traffic", EvaluatedAt: after, Status: DetectorEvaluationClear}}}
			controller.closureState = contracts.ClosureState_CLOSURE_STATE_PENDING
			effect, ok := controller.PendingClosureEffect()
			if !ok || !effect.Freeze {
				t.Fatalf("want a freeze effect, got %+v ok=%v", effect, ok)
			}
			if err := controller.ExecuteClosureEffect(ctx, effect); err != nil {
				t.Fatal(err)
			}
			awaitEffectGoroutines(controller)
			pending := controller.pendingClosure
			if pending == nil {
				t.Fatal("closure lost")
			}
			if pending.UnconfirmedReason != test.want {
				t.Fatalf("reason = %s, want %s (detail %q)", pending.UnconfirmedReason, test.want, pending.UnconfirmedDetail)
			}
			components := pending.RecoveryVerification.GetComponents()
			if len(components) != 1 || components[0].GetStrength() != test.strength {
				t.Fatalf("verification = %v", pending.RecoveryVerification)
			}
			derived, err := ProtoOutcomeDeriver{}.Derive(ctx, *pending, "", "synthetic", "synthetic")
			if err != nil {
				t.Fatal(err)
			}
			if derived.Record.GetRecoveryVerification().GetComponents()[0].GetStrength() != test.strength {
				t.Fatalf("the outcome does not carry the record: %v", derived.Record.GetRecoveryVerification())
			}
			if test.want == contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_UNSPECIFIED {
				if unverifiedRecoveryOutcome(derived.Record) {
					t.Fatal("an operation-strength recovery is verified")
				}
				return
			}
			if !strings.Contains(pending.UnconfirmedDetail, "cache: reachability only") || len(pending.FinalDetectorStates) != 0 {
				t.Fatalf("the closure must name the component and make no health claim: %+v", pending)
			}
			if derived.Record.GetClassification() != "unconfirmed" || derived.RequiresReflection || !unverifiedRecoveryOutcome(derived.Record) {
				t.Fatalf("the outcome must be unconfirmed and carry the reason: %+v", derived.Record)
			}
		})
	}
}

// (review C4) Until one credit rule decides what reachability evidence may
// teach, a close whose in-scope component recovered below operation strength
// gives reflection no verified repair: the partial path stays blocked.
func TestReachabilityCloseGivesReflectionNoVerifiedRepair(t *testing.T) {
	at := timestamppb.New(verifyT0)
	ref := &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "cache"}
	outcome := &contracts.OutcomeRecord{
		Classification: "unconfirmed",
		RecoveryVerification: &contracts.RecoveryVerification{Probed: true, WritesSettled: true, Components: []*contracts.ComponentVerification{
			{Component: "cache", Strength: contracts.VerificationStrength_VERIFICATION_STRENGTH_REACHABILITY, Recovered: true, InScope: true}}},
		ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Summary: "cache restarted", Resources: []*contracts.ObjectRef{ref}}},
		RepairActions:       []*contracts.RepairActionReceipt{{Success: true, Resources: []*contracts.ObjectRef{ref}, StartedAt: at, CompletedAt: at}},
	}
	if got := partialRepairCauses(outcome); len(got) != 0 {
		t.Fatalf("a repair verified only at reachability is no verified cause: %v", got)
	}
	if reflectionRepairVerified(outcome) {
		t.Fatal("reflection must not publish learning from a recovery no operation probe verified")
	}
	// The same component out of scope (a joined finding SDO never touched)
	// does not block learning from the in-scope repair.
	outcome.RecoveryVerification.Components[0].InScope = false
	if unverifiedRecoveryOutcome(outcome) {
		t.Fatal("an out-of-scope component does not block learning")
	}
}

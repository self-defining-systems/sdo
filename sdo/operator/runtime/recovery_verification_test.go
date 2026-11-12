package runtime

import (
	"strings"
	"testing"
	"time"

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

// browseTest is the failing user test the synthetic incident opens on.
func browseTest() sdk.Finding {
	return sdk.Finding{DetectorID: "traffic-probe-call", RuleID: "scenario-slo.browse-items", Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
		PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "front"}}
}

// runVerify is recovered() over the synthetic map: findings[:opening] opened
// the incident, the rest are clues.
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
	request := IncidentRequest{Namespace: "app", Findings: findings, OpeningKeys: findingKeys(findings[:opening])}
	return recovered(verificationInput{m: m, view: world.view(), request: request, result: result, detected: verifyT0, stability: 10 * time.Second})
}

func restartOf(kind, name string, completed time.Time) *IncidentResult {
	return &IncidentResult{PlanApplications: []RecordedPlanApplication{{ID: "app-1", CompletedAt: completed,
		Result: PlanApplicationResult{Applied: true, ObjectEffects: []PlanObjectEffect{{Operation: "patch", Ref: sdk.ObjectRef{Kind: kind, Namespace: "app", Name: name}, State: "applied"}}}}}}
}

// An opening user test that passes in a sustained way after SDO's write
// settled is Recovered; the record names the opening alerts.
func TestOpeningUserTestPassingAfterTheWriteRecovers(t *testing.T) {
	now := verifyT0.Add(2 * time.Minute)
	check := runVerify(t, verifyWorld{now: now}, []sdk.Finding{browseTest()}, 1, restartOf("StatefulSet", "db", verifyT0.Add(30*time.Second)))
	if check.verdict != recoveryVerified {
		t.Fatalf("verdict %v: %s", check.verdict, check.reason)
	}
	if keys := check.record.GetOpeningKeys(); len(keys) != 1 || check.record.GetStableSince() == nil {
		t.Fatalf("record = %v", check.record)
	}
}

// Passing samples from before SDO's last write settled do not count.
func TestUserTestPassingOnlyBeforeTheWriteSettledDoesNotRecover(t *testing.T) {
	write := verifyT0.Add(30 * time.Second)
	now := write.Add(time.Minute)
	failing := func(at time.Time) bool { return at.Before(write) }
	check := runVerify(t, verifyWorld{now: now, probeOK: failing}, []sdk.Finding{browseTest()}, 1, restartOf("StatefulSet", "db", write))
	if check.verdict != recoveryPending || !strings.Contains(check.reason, "browse-items") {
		t.Fatalf("verdict %v: %s", check.verdict, check.reason)
	}
	if check.record.GetSince().AsTime() != write {
		t.Fatalf("since = %s, want the write's settle time %s", check.record.GetSince().AsTime(), write)
	}
}

// A user test that passes, then fails again, does not recover.
func TestFlappingUserTestDoesNotRecover(t *testing.T) {
	write := verifyT0.Add(30 * time.Second)
	now := write.Add(time.Minute)
	flapping := func(at time.Time) bool { return at.Before(now.Add(-3 * time.Second)) }
	check := runVerify(t, verifyWorld{now: now, probeOK: flapping}, []sdk.Finding{browseTest()}, 1, restartOf("StatefulSet", "db", write))
	if check.verdict != recoveryPending {
		t.Fatalf("a user test failing again after passing recovered: %v %s", check.verdict, check.reason)
	}
}

// A clue never holds Recovered, and a condition opener is decided by its
// detector history, not here.
func TestCluesAndConditionsDoNotHoldTheUserTestPredicate(t *testing.T) {
	now := verifyT0.Add(2 * time.Minute)
	failing := func(time.Time) bool { return false }
	condition := verifyFinding("workload-health", "Deployment", "api", sdk.SeverityCritical)
	if check := runVerify(t, verifyWorld{now: now, probeOK: failing}, []sdk.Finding{condition, browseTest()}, 1, nil); check.verdict != recoveryVerified {
		t.Fatalf("a failing user test that is only a clue held recovery: %s", check.reason)
	}
}

// An unsettled write (its workload still rolling out) holds the verdict.
func TestUnsettledWriteHoldsRecovery(t *testing.T) {
	now := verifyT0.Add(2 * time.Minute)
	region, _, _ := appmap.Find(verifyArch)
	m, _ := appmap.Parse(region.YAML)
	view := verifyWorld{now: now}.view()
	view.PodList[1].Status.Conditions[0].Status = corev1.ConditionFalse
	opening := browseTest()
	check := recovered(verificationInput{m: m, view: view, request: IncidentRequest{Namespace: "app", Findings: []sdk.Finding{opening}, OpeningKeys: findingKeys([]sdk.Finding{opening})},
		result: restartOf("Deployment", "api", verifyT0.Add(30*time.Second)), detected: verifyT0})
	if check.verdict != recoveryPending || check.record.GetWritesSettled() {
		t.Fatalf("verdict %v: %s", check.verdict, check.reason)
	}
}

// The broker's recheck at freeze asks the same predicate: an opening user test
// that has not passed since the write keeps the closure waiting, unfrozen; one
// that passed freezes it verified with the record.
func TestClosureCommitsTheRecoveredVerdict(t *testing.T) {
	ctx := t.Context()
	for name, test := range map[string]struct {
		probeOK func(time.Time) bool
		frozen  bool
	}{
		"user test recovered": {always, true},
		"user test failing":   {func(time.Time) bool { return false }, false},
	} {
		t.Run(name, func(t *testing.T) {
			now := verifyT0.Add(2 * time.Minute)
			after := verifyT0.Add(time.Minute)
			cfg := testControllerConfig()
			cfg.Namespace = "app"
			world := &sameWriteWorld{}
			world.healthy.Store(true)
			controller, err := NewController(cfg, []sdk.Detector{sameWriteDetector{world: world}}, riskSnapshots{view: verifyWorld{now: now, probeOK: test.probeOK}.view()}, &recordingDispatcher{}, after)
			if err != nil {
				t.Fatal(err)
			}
			controller.now = func() time.Time { return now }
			controller.gitRead = fakeArchitecture(verifyArch)
			controller.broker = &riskBroker{}
			result := completedResult("synthetic-incident")
			result.Timing = TimingMetrics{StartedAt: verifyT0, CompletedAt: after}
			request := IncidentRequest{SchemaVersion: ProtocolSchemaVersion, Application: "demo", Namespace: "app", IncidentID: "synthetic-incident", SourceCommit: "synthetic-accepted", RepositoryWorktree: "synthetic-worktree",
				Findings: []sdk.Finding{browseTest()}, OpeningKeys: findingKeys([]sdk.Finding{browseTest()})}
			controller.pendingClosure = &IncidentClosure{Request: request, Result: &result, DetectedAt: verifyT0, DispatchedAt: verifyT0, ResponderCompletedAt: after, VerifiedAt: after,
				FinalDetectorStates: []DetectorEvaluation{{DetectorID: "traffic-probe-call", EvaluatedAt: after, Status: DetectorEvaluationClear}}}
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
			if pending == nil || pending.Frozen != test.frozen {
				t.Fatalf("frozen = %v, want %v: %+v", pending != nil && pending.Frozen, test.frozen, pending)
			}
			if !test.frozen {
				return
			}
			if pending.UnconfirmedReason != contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_UNSPECIFIED {
				t.Fatalf("a recovered closure is unconfirmed: %s %q", pending.UnconfirmedReason, pending.UnconfirmedDetail)
			}
			if keys := pending.RecoveryVerification.GetOpeningKeys(); len(keys) != 1 {
				t.Fatalf("verification = %v", pending.RecoveryVerification)
			}
		})
	}
}

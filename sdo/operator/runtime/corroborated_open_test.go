package runtime

import (
	"context"
	"slices"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
)

// corroboratedArch maps two user tests: browse-items (front, api, db) and
// list-cache (front, cache). The cache component is on no browse path.
const corroboratedArch = "# Manifest inventory\n\n- Service/app/front (source: k8s/front.yaml)\n  - exposure:NodePort\n\n" +
	"<!-- sdo:application-map:begin -->\n```yaml\nversion: 1\ncomponents:\n" +
	"  - {name: front, workload: Deployment/front, services: [front], holds_data: false}\n" +
	"  - {name: api, workload: Deployment/api, services: [api], holds_data: false}\n" +
	"  - {name: db, workload: StatefulSet/db, services: [db], holds_data: true}\n" +
	"  - {name: cache, workload: Deployment/cache, services: [cache], holds_data: false}\n" +
	"edges:\n  - {from: front, to: api, evidence: [config], source: x}\n  - {from: api, to: db, evidence: [config], source: x}\n" +
	"  - {from: front, to: cache, evidence: [config], source: x}\n" +
	"operations:\n" +
	"  - {id: browse, writes: false, entry: {service: front, port: 80, method: GET, path: /items}, path: [front, api, db]}\n" +
	"  - {id: list, writes: false, entry: {service: front, port: 80, method: GET, path: /list}, path: [front, cache]}\n" +
	"probes:\n  - {scenario: browse-items, kind: call, operations: [browse]}\n  - {scenario: list-cache, kind: call, operations: [list]}\n" +
	"```\n<!-- sdo:application-map:end -->\n"

// corroboratedWorld is the simulated application: user tests sampled every
// half second, each failing from its failAt offset with outcome; a learned
// detector whose finding on component learnedOn appears from learnedAt; and
// Pods of db, the newest created at dbPodAt.
type corroboratedWorld struct {
	t0        time.Time
	now       time.Time
	failAt    map[string]time.Duration
	outcome   traffic.Outcome
	learnedOn string
	learnedAt time.Duration // negative: never
	dbPodAt   time.Duration // a db Pod created at this offset; zero: an hour before
}

func (w *corroboratedWorld) Snapshot(context.Context) (sdk.DetectionContext, error) {
	controller := true
	pod := func(name, owner, kind string, created time.Time) corev1.Pod {
		return corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app", CreationTimestamp: metav1.NewTime(created),
			OwnerReferences: []metav1.OwnerReference{{Kind: kind, Name: owner, Controller: &controller}}}}
	}
	old := w.t0.Add(-time.Hour)
	dbCreated := old
	if w.dbPodAt != 0 {
		dbCreated = w.t0.Add(w.dbPodAt)
	}
	workload := traffic.Workload{Name: "probe-call", Purpose: traffic.PurposeHealthProbe, RatePerSecond: 2,
		Scenarios: []traffic.WorkloadScenario{{ID: "browse-items", Weight: 1}, {ID: "list-cache", Weight: 1}}}
	window := traffic.Window{Workload: workload, ObservedAt: w.now}
	for _, scenario := range []string{"browse-items", "list-cache"} {
		var samples []traffic.Sample
		failAt, failing := w.failAt[scenario]
		for at := w.t0.Add(-20 * time.Second); !at.After(w.now); at = at.Add(500 * time.Millisecond) {
			sample := traffic.Sample{At: at, Outcome: traffic.OutcomeOK, Status: 200, Latency: 10 * time.Millisecond}
			if failing && !at.Before(w.t0.Add(failAt)) {
				sample = traffic.Sample{At: at, Outcome: w.outcome, Status: 500, Error: "synthetic failure", Latency: 10 * time.Millisecond}
				if w.outcome == traffic.OutcomeTimeout {
					sample.Status, sample.Latency = 0, time.Second
				}
			}
			samples = append(samples, sample)
		}
		window.Scenarios = append(window.Scenarios, traffic.ScenarioObservations{Qualified: true, Samples: samples,
			Scenario: traffic.Descriptor{ID: scenario, Target: traffic.Target{Service: "front", Port: 80}}})
	}
	return sdktest.Snapshot{NamespaceName: "app",
		PodList: []corev1.Pod{pod("front-1", "front", "Deployment", old), pod("db-0", "db", "StatefulSet", dbCreated), pod("cache-1", "cache", "Deployment", old)},
		Traffic: map[string]traffic.Window{"probe-call": window},
	}, nil
}

// learnedDetector reports the world's learned finding, a store flag that a
// learned playbook restores.
type learnedDetector struct{ world *corroboratedWorld }

func (d learnedDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "store-flag-drift", Class: sdk.DetectorClassLearned, Owner: sdk.DetectorOwnerResponder,
		Interval: 500 * time.Millisecond, Playbooks: []string{"store-flag-restore"},
		Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Batching: sdk.BatchingPolicy{Severity: sdk.SeverityWarning}, OriginatingCommit: "seed", OriginatingIncident: "inc-1"}
}

func (d learnedDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	w := d.world
	if w.learnedAt < 0 || w.now.Before(w.t0.Add(w.learnedAt)) {
		return nil, nil
	}
	kind := map[string]string{"db": "StatefulSet", "cache": "Deployment"}[w.learnedOn]
	return []sdk.Finding{{DetectorID: "store-flag-drift", RuleID: "flag", Status: sdk.FindingActive, Severity: sdk.SeverityWarning,
		Summary: "store flag differs", Evidence: "synthetic flag read", PrimaryResource: sdk.ObjectRef{APIVersion: "apps/v1", Kind: kind, Namespace: "app", Name: w.learnedOn},
		Playbooks: []string{"store-flag-restore"}}}, nil
}

func corroboratedController(t *testing.T, w *corroboratedWorld, startup time.Duration) *Controller {
	t.Helper()
	cfg := testControllerConfig()
	cfg.Namespace, cfg.SourceCommit, cfg.RepositoryWorktree = "app", "synthetic-accepted", "synthetic-worktree"
	cfg.StartupBound = startup
	probe := traffic.NewDetector(sdk.DetectorSpec{ID: "traffic-probe-call", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge, Interval: 500 * time.Millisecond,
		Persistence: sdk.PersistencePolicy{Firing: 2, Clearing: 2}, Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: 500 * time.Millisecond}, OriginatingCommit: "seed"}, "probe-call")
	c, err := NewController(cfg, []sdk.Detector{probe, learnedDetector{w}}, w, &recordingDispatcher{requests: make(chan IncidentRequest, 4)}, w.t0.Add(-30*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	c.gitRead = fakeArchitecture(corroboratedArch)
	return c
}

// runUntil steps the controller every half second from t0-10s to t0+limit and
// returns the offset at which done first held, or false.
func runUntil(t *testing.T, c *Controller, w *corroboratedWorld, limit time.Duration, done func() bool) (time.Duration, bool) {
	t.Helper()
	for at := w.t0.Add(-10 * time.Second); !at.After(w.t0.Add(limit)); at = at.Add(500 * time.Millisecond) {
		w.now = at
		c.now = func() time.Time { return at }
		if err := c.Step(context.Background(), at, nil); err != nil {
			t.Fatal(err)
		}
		if done() {
			return at.Sub(w.t0), true
		}
	}
	return limit, false
}

func openedAt(t *testing.T, c *Controller, w *corroboratedWorld, limit time.Duration) (time.Duration, bool) {
	return runUntil(t, c, w, limit, c.IncidentOpen)
}

func newCorroboratedWorld() *corroboratedWorld {
	return &corroboratedWorld{t0: time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC), failAt: map[string]time.Duration{"browse-items": 5 * time.Second},
		outcome: traffic.OutcomeError, learnedOn: "db", learnedAt: 2 * time.Second}
}

// A learned playbook's detector reported db at +2; browse-items, whose path
// runs through db, fails hard from +5: the incident opens at that first
// failure instead of after the 4 s hold (+10s with the same persistence).
func TestCorroboratedUserTestOpensAtItsFirstHardFailure(t *testing.T) {
	w := newCorroboratedWorld()
	c := corroboratedController(t, w, 0)
	at, ok := openedAt(t, c, w, 20*time.Second)
	// The detector's own persistence (two evaluations) and the batcher's
	// debounce still apply: one second after the first failing sample.
	if !ok || at > 6*time.Second {
		t.Fatalf("opened = %v at %s, want right after the first failing sample (+5s)", ok, at)
	}
	request := currentRequest(c)
	if len(request.OpeningKeys) != 1 || !slices.ContainsFunc(request.Findings, func(f sdk.Finding) bool {
		return f.DetectorID == "traffic-probe-call" && f.RuleID == traffic.RuleIDPrefix+"browse-items"
	}) {
		t.Fatalf("opening keys %v, findings %+v: want the user test as the one opener", request.OpeningKeys, request.Findings)
	}
}

// Every case without corroboration keeps today's hold: the same hard failure
// opens only once it spans 4 s.
func TestUncorroboratedUserTestsKeepTheHold(t *testing.T) {
	for name, change := range map[string]func(*corroboratedWorld){
		"no learned finding":                 func(w *corroboratedWorld) { w.learnedAt = -1 },
		"learned finding after the failure":  func(w *corroboratedWorld) { w.learnedAt = 6 * time.Second },
		"learned finding off the test path":  func(w *corroboratedWorld) { w.learnedOn = "cache" },
		"db rolled out since the finding":    func(w *corroboratedWorld) { w.dbPodAt = 3 * time.Second },
		"db rolled out in the finding's sec": func(w *corroboratedWorld) { w.dbPodAt = 2 * time.Second },
	} {
		t.Run(name, func(t *testing.T) {
			w := newCorroboratedWorld()
			change(w)
			c := corroboratedController(t, w, 0)
			at, ok := openedAt(t, c, w, 20*time.Second)
			if !ok || at < 9*time.Second {
				t.Fatalf("opened = %v at %s, want the 4 s hold (≥ +9s)", ok, at)
			}
		})
	}
}

// Slowness is not a hard failure: timeouts wait for the hold even when a
// learned finding is on the path.
func TestCorroboratedTimeoutsKeepTheHold(t *testing.T) {
	w := newCorroboratedWorld()
	w.outcome = traffic.OutcomeTimeout
	c := corroboratedController(t, w, 0)
	if at, ok := openedAt(t, c, w, 20*time.Second); ok && at < 9*time.Second {
		t.Fatalf("timeouts opened at %s, before the hold", at)
	}
}

// The join path agrees with the opening: list-cache fails from 0 with no
// corroboration and opens at the hold; browse-items then fails hard on db's
// corroborated path and joins at its first failing sample.
func TestCorroboratedUserTestJoinsAtItsFirstHardFailure(t *testing.T) {
	w := newCorroboratedWorld()
	w.failAt = map[string]time.Duration{"list-cache": 0, "browse-items": 10 * time.Second}
	c := corroboratedController(t, w, 0)
	if at, ok := openedAt(t, c, w, 8*time.Second); !ok || at < 4*time.Second {
		t.Fatalf("list-cache opened = %v at %s, want the hold", ok, at)
	}
	joined := func() bool {
		request := currentRequest(c)
		return request != nil && len(alertKeys(request)) == 2
	}
	at, ok := runUntil(t, c, w, 20*time.Second, joined)
	if !ok || at > 10500*time.Millisecond {
		t.Fatalf("browse-items joined = %v at %s, want right after its first failing sample (+10s)", ok, at)
	}
}

// The startup period still holds a corroborated opener: an install never seen
// healthy opens nothing until its bound.
func TestCorroboratedOpenerWaitsForTheStartupPeriod(t *testing.T) {
	w := newCorroboratedWorld()
	w.failAt = map[string]time.Duration{"browse-items": -9 * time.Second}
	w.learnedAt = -10 * time.Second
	c := corroboratedController(t, w, 2*time.Minute)
	if at, ok := openedAt(t, c, w, 20*time.Second); ok {
		t.Fatalf("opened at %s inside the startup period", at)
	}
}

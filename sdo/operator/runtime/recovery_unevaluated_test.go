package runtime

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
)

// sampledProvider adds one user test to the store's view. It has samples from
// evaluatedAt on.
type sampledProvider struct {
	base        *restartedStore
	clock       func() time.Time
	mu          sync.Mutex
	evaluatedAt time.Time
	test        traffic.ScenarioObservations
}

func (p *sampledProvider) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	base, err := p.base.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	view, now := base.(sdktest.Snapshot), p.clock()
	test := p.test
	if !p.evaluatedAt.IsZero() && !now.Before(p.evaluatedAt) {
		test.Samples = samplesEverySecond(now.Add(-10*time.Second), now, always)
	}
	view.Traffic = map[string]traffic.Window{"probe": {ObservedAt: now, Scenarios: []traffic.ScenarioObservations{test},
		Workload: traffic.Workload{Name: "probe", RatePerSecond: 1, Scenarios: []traffic.WorkloadScenario{{ID: test.Scenario.ID}}}}}
	return view, nil
}

func TestRecoveryWaitsForUnevaluatedUserTests(t *testing.T) {
	const window, schedule = 15 * time.Second, 30 * time.Second // the default SLO's maxAge
	for name, tc := range map[string]struct {
		evaluatedAfter time.Duration // after the heal; 0 never
		mutate         func(*traffic.ScenarioObservations)
		timeout        time.Duration
		want           time.Duration
	}{
		"already evaluated":                 {evaluatedAfter: -time.Minute, want: window},
		"evaluates during the wait":         {evaluatedAfter: 21 * time.Second, want: 21 * time.Second},
		"never evaluates":                   {want: schedule},
		"never, capped by the verification": {timeout: 20 * time.Second, want: 20 * time.Second},
		"never succeeded":                   {mutate: func(o *traffic.ScenarioObservations) { o.Qualified = false }, want: window},
		"not a user test":                   {mutate: func(o *traffic.ScenarioObservations) { o.UserTest = false }, want: window},
	} {
		t.Run(name, func(t *testing.T) {
			w := newClosureRestartWorld(t)
			c := w.controller
			c.config.ResponseTimeout = 10 * time.Minute
			if tc.timeout > 0 {
				c.config.VerificationTimeout = tc.timeout
			}
			provider := &sampledProvider{base: newRestartedStore(w.healedAt), clock: c.clock, test: traffic.ScenarioObservations{
				Scenario: traffic.Descriptor{ID: "write-item", Target: traffic.Target{Service: "api"}}, Qualified: true, UserTest: true}}
			if tc.evaluatedAfter != 0 {
				provider.evaluatedAt = w.healedAt.Add(tc.evaluatedAfter)
			}
			if tc.mutate != nil {
				tc.mutate(&provider.test)
			}
			c.provider = provider
			w.runUntil(t, 200, func() {}, func() bool {
				if wake := c.NextWake(); c.IncidentOpen() && !c.unevaluatedHoldUntil.IsZero() && wake.Before(w.healedAt) {
					t.Fatalf("the wait spins: wake %s", wake)
				}
				return c.earlyReportPendingLocked()
			})
			if !c.earlyReportPendingLocked() {
				t.Fatal("recovery never verified")
			}
			took := c.pendingClosure.VerifiedAt.Sub(w.healedAt)
			if took < tc.want || took > tc.want+3*time.Second {
				t.Fatalf("verified %s after the heal, want %s", took, tc.want)
			}
			c.responderCancel()
			c.Wait()
		})
	}
}

// Only tests that reach a Service of the alert set hold it. An alert that
// cannot be placed makes every user test its own.
func TestUnevaluatedTestsAreScopedToTheIncidentsServices(t *testing.T) {
	service := func(name string) corev1.Service {
		return corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": name}}}
	}
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "api-1", Namespace: "app", Labels: map[string]string{"app": "api"}}}
	test := func(id string, depends ...string) traffic.ScenarioObservations {
		return traffic.ScenarioObservations{Scenario: traffic.Descriptor{ID: id, Target: traffic.Target{Service: depends[0]}, DependsOn: depends[1:]}, Qualified: true, UserTest: true}
	}
	view := sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{service("api"), service("mail")}, PodList: []corev1.Pod{pod},
		Traffic: map[string]traffic.Window{"probe": {Workload: traffic.Workload{Name: "probe"}, Scenarios: []traffic.ScenarioObservations{test("write", "front", "api"), test("send", "mail")}}}}
	for _, tc := range []struct {
		ref  sdk.ObjectRef
		want []string
	}{
		{sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}, []string{"write"}},
		{sdk.ObjectRef{Kind: "Pod", Namespace: "app", Name: "api-1"}, []string{"write"}},
		{sdk.ObjectRef{Kind: "Node", Name: "synthetic"}, []string{"send", "write"}},
	} {
		finding := sdk.Finding{DetectorID: "d", RuleID: "r", Status: sdk.FindingActive, PrimaryResource: tc.ref}
		request := IncidentRequest{Findings: []sdk.Finding{finding}, OpeningKeys: findingKeys([]sdk.Finding{finding})}
		got, need := unevaluatedRecoveryTests(view, request)
		if !slices.Equal(got, tc.want) || need != traffic.DefaultMaxAge {
			t.Errorf("%s: %s need %s, want %s", tc.ref.Kind, strings.Join(got, ","), need, tc.want)
		}
	}
}

// The wait ends with whichever deadline already governs the recovery.
func TestTheUnevaluatedWaitNeverOutlivesAnExistingDeadline(t *testing.T) {
	c, world, _ := automaticRecoveryWorld(t, settlingCase{}, settings.AfterPlaybookAgent)
	report := autoReport(c)
	hold := func(at time.Time) bool {
		c.closureStableSince = at.Add(-time.Second)
		return c.unevaluatedHoldLocked(at, []string{"write-item"}, time.Hour) != ""
	}
	bound := c.automaticDeadlineLocked(report)
	if !hold(world.clock.Now()) || !hold(bound.Add(-time.Millisecond)) || hold(bound) {
		t.Fatal("the automatic stage's deadline must end the wait")
	}
	report.HandoffSelected = true // the agent owns the incident now
	grace := c.recoveryGraceEndsLocked()
	if !hold(grace.Add(-time.Millisecond)) || hold(grace) {
		t.Fatal("a returned responder's verification bound must end the wait")
	}
	if !hold(grace.Add(-3*time.Second)) || hold(grace) || !c.unevaluatedHoldSince.IsZero() {
		t.Fatal("an ended wait must close its duration")
	}
	c.responderDone = false
	deadline := c.currentIncidentRequest.ResponseDeadline
	if hold(deadline) {
		t.Fatal("the response deadline must end the wait")
	}
}

func TestUnevaluatedTestsSkipBrokenWorkloadsAndFollowTheirSchedule(t *testing.T) {
	// One arrival per 20 s: three samples take longer than maxAge.
	slow := traffic.Workload{Name: "probe", RatePerSecond: 0.05, Scenarios: []traffic.WorkloadScenario{{ID: "write", Weight: 1}}}
	broken := slow
	broken.Scenarios = []traffic.WorkloadScenario{{ID: "write", Weight: 1, KnownBroken: &traffic.KnownBroken{Reason: "synthetic", FailsWith: []string{"404"}}}}
	for name, tc := range map[string]struct {
		workload  traffic.Workload
		loadError string
		held      bool
	}{
		"waits for its schedule": {workload: slow, held: true},
		"known broken":           {workload: broken},
		"cannot be loaded":       {workload: slow, loadError: "synthetic"},
	} {
		view := sdktest.Snapshot{NamespaceName: "app", Traffic: map[string]traffic.Window{"probe": {Workload: tc.workload, LoadError: tc.loadError,
			Scenarios: []traffic.ScenarioObservations{{Scenario: traffic.Descriptor{ID: "write"}, Qualified: true, UserTest: true}}}}}
		waiting, need := unevaluatedRecoveryTests(view, IncidentRequest{})
		if want := time.Duration(traffic.DefaultMinSamples) * 20 * time.Second; tc.held && (len(waiting) != 1 || need != want) || !tc.held && len(waiting) != 0 {
			t.Errorf("%s: waiting %v need %s, want held %v %s", name, waiting, need, tc.held, want)
		}
	}
}

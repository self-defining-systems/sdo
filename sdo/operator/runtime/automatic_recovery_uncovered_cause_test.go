package runtime

import (
	"slices"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk/sdktest"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/sdk"
)

// storeStateClue is a store-state finding on component store: positive
// evidence of a data fault, a clue that never opens or holds an incident.
func storeStateClue(kind, name string) sdk.Finding {
	return sdk.Finding{DetectorID: "store-state-store", RuleID: "store-state.namespace-changed", Status: sdk.FindingActive, Severity: sdk.SeverityWarning,
		PrimaryResource: sdk.ObjectRef{APIVersion: "apps/v1", Kind: kind, Namespace: "app", Name: name}, Fingerprint: "store-state.namespace-changed/" + name, Summary: "a collection was renamed", Evidence: "synthetic"}
}

// addClue puts the clue among the incident's findings and keeps it active in
// the tracker, as its detector's evaluations do every step.
func addClue(t *testing.T, c *Controller, clue sdk.Finding, active bool) {
	t.Helper()
	c.mu.Lock()
	c.currentIncidentRequest.Findings = append(c.currentIncidentRequest.Findings, clue)
	c.mu.Unlock()
	if active {
		c.tracker.SetPolicy(clue.DetectorID, 1, 1, 0)
		c.tracker.Observe(c.clock(), clue.DetectorID, []sdk.Finding{clue})
	}
}

// admitClue is the clue's detector reporting it active at the current step:
// the controller's one admission path for a new finding.
func admitClue(t *testing.T, c *Controller, clue sdk.Finding) {
	t.Helper()
	c.tracker.SetPolicy(clue.DetectorID, 1, 1, 0)
	c.tracker.Observe(c.clock(), clue.DetectorID, []sdk.Finding{clue})
	c.admitActivation(t.Context(), clue, c.clock(), sdk.DetectorSpec{})
}

// Two faults on one failing path: the stage restarted api, which the user
// test's path runs through, and the store's data changed before that, which
// no applied program targeted and api neither calls nor is called by. The
// agent starts once the user test failed again after the restart settled,
// not after its full window or the bound.
func TestAStageThatDidNotCoverACauseHandsOffOnceItsWritesSettle(t *testing.T) {
	test := &buyTest{failing: alwaysFails}
	c, world, key := freshWorld(t, test, settings.AfterPlaybookAgent)
	c.gitRead = buyMap("web, api, store", "{from: web, to: api}, {from: web, to: store}")
	addClue(t, c, storeStateClue("StatefulSet", "store"), true)
	at, ok := handoffAfter(t, c, world, test.start, horizon(test))
	report := autoReport(c)
	if !ok || at >= c.config.ClosureStabilityWindow+2*time.Second {
		t.Fatalf("handoff = %v at %s, want right after the settle, before a stability window (%s)", ok, at, c.config.ClosureStabilityWindow)
	}
	if !slices.Contains(report.GetHandoffKeys(), key) || report.GetRecoveryWait().GetExitReason() != contracts.RecoveryWait_EXIT_REASON_UNCOVERED || world.restarts.Load() != 1 {
		t.Fatalf("report = %v, %d writes", report, world.restarts.Load())
	}
	if c.learnedPassAllowedLocked() || c.dispatchState != contracts.DispatchState_DISPATCH_STATE_PENDING {
		t.Fatalf("the stage may still write, or dispatch state = %s", c.dispatchState)
	}
}

// What the stage's writes may still clear keeps today's wait: a clue on the
// written component (also one first seen after the stage applied), on one
// that calls it or that it calls (a misconfigured caller shows up in its
// store's state), off every alert's path, no longer active, or with no map to
// place it; and
// any clue while no alert on its path failed again after the writes settled
// (the alerts may still be clearing).
func TestACauseTheStageMayHaveAddressedKeepsTheWait(t *testing.T) {
	path := buyMap("web, api, store", "{from: web, to: api}, {from: web, to: store}")
	for name, tc := range map[string]struct {
		read     gitRead
		clue     sdk.Finding
		inactive bool
		late     bool
		failing  func(time.Duration) bool
	}{
		"on the written component":          {read: path, clue: storeStateClue("Deployment", "api")},
		"on a caller of the written one":    {read: path, clue: storeStateClue("Deployment", "web")},
		"on a callee of the written one":    {read: buyMap("web, api, store", "{from: web, to: api}, {from: api, to: store}"), clue: storeStateClue("StatefulSet", "store")},
		"off every alert's path":            {read: buyMap("web, api", "{from: web, to: api}, {from: web, to: store}"), clue: storeStateClue("StatefulSet", "store")},
		"no longer active":                  {read: path, clue: storeStateClue("StatefulSet", "store"), inactive: true},
		"late, on the written component":    {read: path, clue: storeStateClue("Deployment", "api"), late: true},
		"the user test passes since settle": {read: path, clue: storeStateClue("StatefulSet", "store"), failing: func(at time.Duration) bool { return at < 2*time.Second }},
		"no application map":                {clue: storeStateClue("StatefulSet", "store")},
	} {
		t.Run(name, func(t *testing.T) {
			test := &buyTest{failing: alwaysFails}
			if tc.failing != nil {
				test.failing = tc.failing
			}
			c, world, _ := freshWorld(t, test, settings.AfterPlaybookAgent)
			c.gitRead = tc.read
			if tc.late {
				autoStep(t, c, world.clock, time.Second)
				if !playbookWrote(autoReport(c)) {
					t.Fatal("the stage did not apply in the first step")
				}
			}
			if tc.late {
				admitClue(t, c, tc.clue)
			} else {
				addClue(t, c, tc.clue, !tc.inactive)
			}
			if at, ok := handoffAfter(t, c, world, test.start, horizon(test)-time.Second); ok {
				t.Fatalf("handed off at %s, want no earlier than %s", at, horizon(test))
			}
		})
	}
}

// The rows' shape: the built-in source restore wrote a ConfigMap or a Service
// (objects no component's workload controls) and listed every alert as its
// scope. The writes reach the components that consume or are fronted by the
// object; a clue the restore did not list is a cause it did not cover.
func TestCausesBeyondAStageThatRestoredAConfigMapOrAService(t *testing.T) {
	c, _, _ := freshWorld(t, &buyTest{failing: alwaysFails}, settings.AfterPlaybookAgent)
	view := sdktest.Snapshot{NamespaceName: "app",
		DeploymentList: []appsv1.Deployment{{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "api"}, Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "api-config"}}}}}}}}}},
		StatefulSetList: []appsv1.StatefulSet{{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "store"}}}}
	// A job the map does not place mounts the shared ConfigMap; the store may
	// read the API's ConfigMap through the Kubernetes API with its own account.
	mount := corev1.PodSpec{Volumes: []corev1.Volume{{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "shared"}}}}}}
	view.DeploymentList[0].Spec.Template.Spec.Volumes = append(view.DeploymentList[0].Spec.Template.Spec.Volumes, mount.Volumes...)
	view.JobList = []batchv1.Job{{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "migrate"}, Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: mount}}}}
	reader := view
	reader.StatefulSetList = []appsv1.StatefulSet{{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "store"}, Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{ServiceAccountName: "store-reader"}}}}}
	clue := storeStateClue("StatefulSet", "store")
	settled := c.clock()
	for name, tc := range map[string]struct {
		written  sdk.ObjectRef
		path     string
		view     *sdktest.Snapshot
		targeted bool
		want     bool
	}{
		"a ConfigMap a workload off the map also mounts": {written: sdk.ObjectRef{Kind: "ConfigMap", Namespace: "app", Name: "shared"}, path: "web, api, store"},
		"a ConfigMap the store may read through the API": {written: sdk.ObjectRef{Kind: "ConfigMap", Namespace: "app", Name: "api-config"}, path: "web, api, store", view: &reader},
		"a restored ConfigMap its consumer api reads":    {written: sdk.ObjectRef{Kind: "ConfigMap", Namespace: "app", Name: "api-config"}, path: "web, api, store", want: true},
		"a restored Service of api":                      {written: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}, path: "web, api, store", want: true},
		"the clue is in the restore's scope":             {written: sdk.ObjectRef{Kind: "ConfigMap", Namespace: "app", Name: "api-config"}, path: "web, api, store", targeted: true},
		"a ConfigMap no component consumes":              {written: sdk.ObjectRef{Kind: "ConfigMap", Namespace: "app", Name: "other"}, path: "web, api, store"},
		"the store's own Service":                        {written: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "store"}, path: "web, api, store"},
		"the clue is off the failing path":               {written: sdk.ObjectRef{Kind: "ConfigMap", Namespace: "app", Name: "api-config"}, path: "web, api"},
	} {
		t.Run(name, func(t *testing.T) {
			request := *c.currentIncidentRequest
			request.Findings = append(slices.Clone(request.Findings), clue)
			scope := []string{}
			for _, key := range alertKeys(&request) {
				for _, f := range request.Findings {
					if findingKey(f) == key {
						scope = append(scope, FindingFingerprint(f))
					}
				}
			}
			if tc.targeted {
				scope = append(scope, FindingFingerprint(clue))
			}
			report := &contracts.StageReport{Mitigations: []*contracts.StageMitigation{{
				ApplyReceipt:               &contracts.PlanApplication{Applied: true, StartedAt: contractTimestamp(settled.Add(-30 * time.Second)), ObjectEffects: []*contracts.PlanObjectEffect{{State: "applied"}}},
				TouchedObjects:             []*contracts.ObjectRef{objectRefContract(tc.written)},
				ClearedFindingFingerprints: scope,
			}}}
			c.gitRead = fakeArchitecture("# Manifest inventory\n\n<!-- sdo:application-map:begin -->\n```yaml\nversion: 1\ncomponents:\n" +
				"  - {name: api, workload: Deployment/api, services: [api], holds_data: false}\n" +
				"  - {name: web, workload: Deployment/web, services: [web], holds_data: false}\n" +
				"  - {name: store, workload: StatefulSet/store, services: [store], holds_data: true}\n" +
				"edges: [{from: web, to: api}, {from: web, to: store}]\n" +
				"operations:\n  - {id: checkout, writes: true, entry: {service: web, port: 80, method: POST, path: /buy}, path: [" + tc.path + "]}\n" +
				"probes:\n  - {scenario: buy, kind: call, operations: [checkout]}\n```\n<!-- sdo:application-map:end -->\n")
			m, _ := acceptedVerificationMap(t.Context(), c.gitRead, request)
			live := view
			if tc.view != nil {
				live = *tc.view
			}
			windowed := windowSnapshot{DetectionContext: live, windows: (&buyTest{every: 2 * time.Second, failing: alwaysFails, start: settled}).window(settled.Add(10 * time.Second))}
			got := causesBeyondStage(windowed, m, request, report, settled, func(string) bool { return true })
			if (len(got) == 1 && got[0] == findingKey(clue)) != tc.want || len(got) > 1 {
				t.Fatalf("uncovered = %q, want the clue: %v", got, tc.want)
			}
		})
	}
}

// Multi-fault timeline, a fault that lands after the stage's look: the stage
// restarted api at the page, and the store's data changes 20 s later, off the
// written component's paths in both directions. Its finding joins the
// incident while the stage still owns it, and the agent starts at the next
// step with the late cause among its findings, not after the user test's full
// window or the stage's bound.
func TestALateCauseTheStageDidNotCoverHandsOffAtOnce(t *testing.T) {
	test := &buyTest{failing: alwaysFails}
	c, world, key := freshWorld(t, test, settings.AfterPlaybookAgent)
	c.gitRead = buyMap("web, api, store", "{from: web, to: api}, {from: web, to: store}")
	if at, ok := handoffAfter(t, c, world, test.start, 20*time.Second); ok {
		t.Fatalf("handed off at %s with every cause covered", at)
	}
	clue := storeStateClue("StatefulSet", "store")
	admitClue(t, c, clue)
	if !slices.ContainsFunc(c.currentIncidentRequest.Findings, func(f sdk.Finding) bool { return findingKey(f) == findingKey(clue) }) {
		t.Fatal("the late finding did not join the incident the stage owns")
	}
	at, ok := handoffAfter(t, c, world, test.start, horizon(test))
	report := autoReport(c)
	if !ok || at > 22*time.Second {
		t.Fatalf("handoff = %v at %s, want within a step of the late finding at 20s", ok, at)
	}
	if !slices.Contains(report.GetHandoffKeys(), key) || !slices.Contains(report.GetHandoffKeys(), findingKey(clue)) ||
		report.GetRecoveryWait().GetExitReason() != contracts.RecoveryWait_EXIT_REASON_UNCOVERED || world.restarts.Load() != 1 {
		t.Fatalf("report = %v, %d writes", report, world.restarts.Load())
	}
	agent := stageDispatchRequest(*c.currentIncidentRequest)
	if !slices.ContainsFunc(agent.Findings, func(f sdk.Finding) bool { return findingKey(f) == findingKey(clue) }) {
		t.Fatalf("the agent's findings %v omit the uncovered cause", findingKeys(agent.Findings))
	}
}

// A late finding a learned playbook links is the stage's first: it runs one
// more pass for it while it owns the incident, and only what that pass did not
// cover reaches the agent.
func TestALateFindingALearnedPlaybookLinksGetsAStagePassFirst(t *testing.T) {
	test := &buyTest{failing: alwaysFails}
	c, world, _ := freshWorld(t, test, settings.AfterPlaybookAgent)
	c.gitRead = buyMap("web, api, store", "{from: web, to: api}, {from: web, to: store}")
	var passes []bool
	c.tuneMidStage = func(stage *DeterministicResponseStage) {
		passes = append(passes, autoReport(c).GetHandoffSelected())
	}
	if at, ok := handoffAfter(t, c, world, test.start, 20*time.Second); ok {
		t.Fatalf("handed off at %s with every cause covered", at)
	}
	clue := storeStateClue("StatefulSet", "store")
	clue.Playbooks = []string{"api-request-stall"}
	admitClue(t, c, clue)
	if !c.midStage.pending {
		t.Fatal("no stage pass queued for the late finding its learned playbook links")
	}
	at, ok := handoffAfter(t, c, world, test.start, horizon(test))
	if len(passes) == 0 || slices.Contains(passes, true) {
		t.Fatalf("stage passes (handoff selected at each) = %v, want them all before the handoff", passes)
	}
	if !ok || at > 23*time.Second || !slices.Contains(autoReport(c).GetHandoffKeys(), findingKey(clue)) {
		t.Fatalf("handoff = %v at %s, keys %v: the pass could not bind the store, so its cause goes to the agent", ok, at, autoReport(c).GetHandoffKeys())
	}
}

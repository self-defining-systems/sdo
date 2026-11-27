package runtime

import (
	"slices"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
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

// Two faults on one failing path: the stage restarted api, which the user
// test's path runs through, and the store's data changed, which no applied
// program targeted and api's restart cannot reach. The agent starts as soon as
// the restart settled, not after the user test's full window or the bound.
func TestAStageThatDidNotCoverACauseHandsOffOnceItsWritesSettle(t *testing.T) {
	test := &buyTest{failing: alwaysFails}
	c, world, key := freshWorld(t, test, settings.AfterPlaybookAgent)
	c.gitRead = buyMap("web, api, store", "{from: web, to: api}, {from: api, to: store}")
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
// written component, on one that calls it, off every alert's path, targeted
// by the applied program, no longer active, or with no map to place it.
func TestACauseTheStageMayHaveAddressedKeepsTheWait(t *testing.T) {
	path := buyMap("web, api, store", "{from: web, to: api}, {from: api, to: store}")
	for name, tc := range map[string]struct {
		read     gitRead
		clue     sdk.Finding
		inactive bool
	}{
		"on the written component":       {read: path, clue: storeStateClue("Deployment", "api")},
		"on a caller of the written one": {read: path, clue: storeStateClue("Deployment", "web")},
		"off every alert's path":         {read: buyMap("web, api", "{from: web, to: api}"), clue: storeStateClue("StatefulSet", "store")},
		"no longer active":               {read: path, clue: storeStateClue("StatefulSet", "store"), inactive: true},
		"no application map":             {clue: storeStateClue("StatefulSet", "store")},
	} {
		t.Run(name, func(t *testing.T) {
			test := &buyTest{failing: alwaysFails}
			c, world, _ := freshWorld(t, test, settings.AfterPlaybookAgent)
			c.gitRead = tc.read
			addClue(t, c, tc.clue, !tc.inactive)
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
	clue := storeStateClue("StatefulSet", "store")
	for name, tc := range map[string]struct {
		written  sdk.ObjectRef
		path     string
		targeted bool
		want     bool
	}{
		"a restored ConfigMap its consumer api reads": {written: sdk.ObjectRef{Kind: "ConfigMap", Namespace: "app", Name: "api-config"}, path: "web, api, store", want: true},
		"a restored Service of api":                   {written: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}, path: "web, api, store", want: true},
		"the clue is in the restore's scope":          {written: sdk.ObjectRef{Kind: "ConfigMap", Namespace: "app", Name: "api-config"}, path: "web, api, store", targeted: true},
		"a ConfigMap no component consumes":           {written: sdk.ObjectRef{Kind: "ConfigMap", Namespace: "app", Name: "other"}, path: "web, api, store"},
		"the store's own Service":                     {written: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "store"}, path: "web, api, store"},
		"the clue is off the failing path":            {written: sdk.ObjectRef{Kind: "ConfigMap", Namespace: "app", Name: "api-config"}, path: "web, api"},
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
				ApplyReceipt:               &contracts.PlanApplication{Applied: true, ObjectEffects: []*contracts.PlanObjectEffect{{State: "applied"}}},
				TouchedObjects:             []*contracts.ObjectRef{objectRefContract(tc.written)},
				ClearedFindingFingerprints: scope,
			}}}
			c.gitRead = fakeArchitecture("# Manifest inventory\n\n<!-- sdo:application-map:begin -->\n```yaml\nversion: 1\ncomponents:\n" +
				"  - {name: api, workload: Deployment/api, services: [api], holds_data: false}\n" +
				"  - {name: web, workload: Deployment/web, services: [web], holds_data: false}\n" +
				"  - {name: store, workload: StatefulSet/store, services: [store], holds_data: true}\n" +
				"edges: [{from: web, to: api}, {from: api, to: store}]\n" +
				"operations:\n  - {id: checkout, writes: true, entry: {service: web, port: 80, method: POST, path: /buy}, path: [" + tc.path + "]}\n" +
				"probes:\n  - {scenario: buy, kind: call, operations: [checkout]}\n```\n<!-- sdo:application-map:end -->\n")
			m, _ := acceptedVerificationMap(t.Context(), c.gitRead, request)
			got := causesBeyondStage(view, m, request, report, func(string) bool { return true })
			if (len(got) == 1 && got[0] == findingKey(clue)) != tc.want || len(got) > 1 {
				t.Fatalf("uncovered = %q, want the clue: %v", got, tc.want)
			}
		})
	}
}

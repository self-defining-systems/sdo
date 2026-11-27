package runtime

import (
	"slices"
	"strings"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/requestmetrics"
	"sdo.dev/operator/sdk/sdktest"
)

// buyMap is the accepted map of components api (the stage's restart writes
// it), web and store; user test buy exercises operation checkout along path.
func buyMap(path, edges string) gitRead { return jobMap(path, edges, "") }

// jobMap is buyMap with checkout triggering the job classes of triggers, and
// jobs as further operations entering at the api component's job metric.
func jobMap(path, edges, triggers string, jobs ...string) gitRead {
	return fakeArchitecture("# Manifest inventory\n\n<!-- sdo:application-map:begin -->\n```yaml\nversion: 1\ncomponents:\n" +
		"  - {name: api, workload: Deployment/api, services: [api], holds_data: false}\n" +
		"  - {name: web, workload: Deployment/web, services: [web], holds_data: false}\n" +
		"  - {name: store, workload: StatefulSet/store, services: [store], holds_data: true}\n" +
		"edges: [" + edges + "]\n" +
		"operations:\n  - {id: checkout, writes: true, entry: {service: web, port: 80, method: POST, path: /buy}, path: [" + path + "], triggers: [" + triggers + "]}\n" +
		strings.Join(jobs, "") +
		"entries:\n  - {service: api, metric: jobs_total, labels: {route: class}}\n" +
		"probes:\n  - {scenario: buy, kind: call, operations: [checkout]}\n```\n<!-- sdo:application-map:end -->\n")
}

// job is a background operation of the given class along path, triggering the
// job classes of triggers.
func job(id, class, path, triggers string) string {
	return "  - {id: " + id + ", writes: true, entry: {service: api, path: " + class + "}, path: [" + path + "], triggers: [" + triggers + "]}\n"
}

// A program repaired api; user test buy, whose operation never reaches api,
// still fails: the agent starts once the write settled and had its stability
// window, not after a full window of fresh samples or the bound.
func TestAPartialFixHandsOffAUserTestItsWritesCannotReach(t *testing.T) {
	test := &buyTest{failing: alwaysFails}
	c, world, key := freshWorld(t, test, settings.AfterPlaybookAgent)
	c.gitRead = buyMap("web, store", "{from: web, to: store}, {from: api, to: store}")
	at, ok := handoffAfter(t, c, world, test.start, horizon(test))
	report := autoReport(c)
	if !ok || at > 15*time.Second {
		t.Fatalf("handoff = %v at %s, want right after the settle (horizon %s)", ok, at, horizon(test))
	}
	if !slices.Contains(report.GetHandoffKeys(), key) || report.GetRecoveryWait().GetExitReason() != contracts.RecoveryWait_EXIT_REASON_UNCOVERED || world.restarts.Load() != 1 {
		t.Fatalf("report = %v, %d writes", report, world.restarts.Load())
	}
}

// Background work checkout triggers on a component the write did not touch
// does not hold the agent back.
func TestAPartialFixHandsOffAUserTestWhoseJobsItDidNotWrite(t *testing.T) {
	test := &buyTest{failing: alwaysFails}
	c, world, _ := freshWorld(t, test, settings.AfterPlaybookAgent)
	// The job triggers itself again: resolution ends on a cycle.
	c.gitRead = jobMap("web, store", "", "Mail", job("mail", "Mail", "store", "Mail"))
	if at, ok := handoffAfter(t, c, world, test.start, horizon(test)); !ok || at > 15*time.Second {
		t.Fatalf("handoff = %v at %s, want right after the settle (horizon %s)", ok, at, horizon(test))
	}
}

// What still waits for fresh failing evidence or the bound: a user test whose
// path runs through, or calls into, the written component, a test whose
// newest sample passed, an application without a map, a scenario or
// operation the map does not place.
func TestAUserTestTheWritesCanStillClearWaits(t *testing.T) {
	recovering := func(at time.Duration) bool { return at < 0 }
	for name, tc := range map[string]struct {
		read  gitRead
		test  *buyTest
		bound bool // the test passes again, so only the bound hands off
	}{
		"the path runs through the written component": {read: buyMap("web, api", "{from: web, to: api}"), test: &buyTest{failing: alwaysFails}},
		"a path component calls the written one":      {read: buyMap("web", "{from: web, to: store}, {from: store, to: api}"), test: &buyTest{failing: alwaysFails}},
		"the newest sample passed":                    {read: buyMap("web, store", ""), test: &buyTest{failing: recovering}, bound: true},
		"no application map":                          {test: &buyTest{failing: alwaysFails}},
		"an unmapped path component":                  {read: buyMap("web, cache", ""), test: &buyTest{failing: alwaysFails}},
		// No call edge reaches a queue worker: the restarted worker drains the
		// backlog while the test that reads its effect later still fails.
		"a job it triggers runs on the written worker": {read: jobMap("web, store", "", "Drain", job("drain", "Drain", "api, store", "")), test: &buyTest{failing: alwaysFails}},
		"a job's job runs on the written worker":       {read: jobMap("web, store", "", "Relay", job("relay", "Relay", "store", "Drain"), job("drain", "Drain", "api", "")), test: &buyTest{failing: alwaysFails}},
		"a trigger the map does not resolve":           {read: jobMap("web, store", "", "Drain"), test: &buyTest{failing: alwaysFails}},
		"a job's trigger the map does not resolve":     {read: jobMap("web, store", "", "Relay", job("relay", "Relay", "store", "Drain")), test: &buyTest{failing: alwaysFails}},
		"a job on a component the map does not place":  {read: jobMap("web, store", "", "Drain", job("drain", "Drain", "cache", "")), test: &buyTest{failing: alwaysFails}},
	} {
		t.Run(name, func(t *testing.T) {
			c, world, _ := freshWorld(t, tc.test, settings.AfterPlaybookAgent)
			c.gitRead = tc.read
			want := horizon(tc.test)
			if tc.bound {
				want = autoReport(c).GetRecoveryDeadline().AsTime().Sub(tc.test.start)
			}
			if at, ok := handoffAfter(t, c, world, tc.test.start, want-time.Second); ok {
				t.Fatalf("handed off at %s, want no earlier than %s", at, want)
			}
		})
	}
}

// A request-error alert reports its operation's path and triggered work; an
// undeclared route, or an operation the map does not hold, reports nothing.
func TestARequestErrorAlertIsBeyondTheStageByItsOperation(t *testing.T) {
	c, _, _ := freshWorld(t, &buyTest{failing: alwaysFails}, settings.AfterPlaybookAgent)
	report := autoReport(c)
	view := sdktest.Snapshot{NamespaceName: "app"}
	finding := func(meta map[string]any) sdk.Finding {
		return sdk.Finding{DetectorID: memory.RequestMetricsDetectorID, RuleID: requestmetrics.RuleErrors, Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
			PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "web"}, Fingerprint: "requestmetrics/errors/" + meta["operation"].(string), Metadata: meta}
	}
	for name, tc := range map[string]struct {
		path, edges string
		triggers    string
		meta        map[string]any
		beyond      bool
	}{
		"apart from the write":      {path: "web, store", meta: map[string]any{"operation": "checkout"}, beyond: true},
		"through the write":         {path: "web, api", edges: "{from: web, to: api}", meta: map[string]any{"operation": "checkout"}},
		"an undeclared route":       {path: "web, store", meta: map[string]any{"operation": "checkout", "undeclared_route": true}},
		"an operation not in a map": {path: "web, store", meta: map[string]any{"operation": "refund"}},
		"a job on the written one":  {path: "web, store", triggers: "Drain", meta: map[string]any{"operation": "checkout"}},
	} {
		t.Run(name, func(t *testing.T) {
			request := *c.currentIncidentRequest
			f := finding(tc.meta)
			request.Findings = []sdk.Finding{f}
			request.OpeningKeys, request.JoinedKeys = []string{findingKey(f)}, nil
			c.gitRead = jobMap(tc.path, tc.edges, tc.triggers, job("drain", "Drain", "api", ""))
			m, _ := acceptedVerificationMap(t.Context(), c.gitRead, request)
			if got := alertsBeyondStage(view, m, request, report); (len(got) == 1) != tc.beyond {
				t.Fatalf("beyond = %v, want %v", got, tc.beyond)
			}
		})
	}
}

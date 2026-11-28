package runtime

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/requestmetrics"
	"sdo.dev/operator/sdk/sdktest"
)

// firingHealth is a health detector that always reports one active finding.
type firingHealth struct{ id, rule string }

func (d firingHealth) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: d.id, Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Interval: time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1},
		Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical}, OriginatingCommit: "synthetic"}
}

func (d firingHealth) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	finding := sdk.Finding{DetectorID: d.id, RuleID: d.rule, Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
		Summary: d.id + " synthetic finding", Evidence: "synthetic",
		PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "demo", Name: "web"}}
	finding.Fingerprint = FindingFingerprint(finding)
	return []sdk.Finding{finding}, nil
}

// stagedDispatcher is a responder transport with an automatic stage, like the
// Job dispatcher: the stage runs first, then the agent (counted) if needed.
type stagedDispatcher struct {
	recordingDispatcher
	stage ResponseStage
}

func (d *stagedDispatcher) DeterministicResponseStage() ResponseStage { return d.stage }

// emptyStage is an automatic stage with no learned playbook to apply.
type emptyStage struct{}

func (emptyStage) Name() string { return "deterministic" }
func (emptyStage) Run(context.Context, *IncidentState) (*contracts.StageReport, error) {
	return &contracts.StageReport{}, nil
}

func stepFor(t *testing.T, c *Controller, now *time.Time, count int) {
	t.Helper()
	for range count {
		*now = now.Add(time.Second)
		if err := c.Step(context.Background(), *now, nil); err != nil {
			t.Fatal(err)
		}
	}
}

// Under detection.pages_only no opener role opens an incident: a failing user
// test, failing user traffic and a Kubernetes condition are clues. A page
// opens one with an empty alert set, and the finding rides in it as a clue (as
// on a page without the setting), so the automatic stage can match it.
// Without the setting each of them opens an incident on its own.
func TestPagesOnlyNoDetectorFindingOpensAndAPageCarriesItAsAClue(t *testing.T) {
	openers := map[string]sdk.Detector{
		"user test": func() sdk.Detector {
			probe := &switchedProbe{id: memory.TrafficDetectorPrefix + "web", scenario: "browse"}
			probe.failing.Store(true)
			return probe
		}(),
		"user traffic":         firingHealth{id: memory.RequestMetricsDetectorID, rule: requestmetrics.RuleErrors},
		"kubernetes condition": firingHealth{id: memory.KubernetesConditionDetectorIDs()[0], rule: "synthetic-condition"},
	}
	for name, opener := range openers {
		for _, pagesOnly := range []bool{false, true} {
			t.Run(name+map[bool]string{false: "/detector paging", true: "/pages only"}[pagesOnly], func(t *testing.T) {
				now := time.Unix(1000, 0).UTC()
				cfg := testControllerConfig()
				cfg.PagesOnly = pagesOnly
				cfg.RequestMetricsOpens = true
				cfg.ClosureStabilityWindow = time.Second
				c, err := NewController(cfg, []sdk.Detector{opener}, staticProvider{}, &recordingDispatcher{requests: make(chan IncidentRequest, 4)}, now)
				if err != nil {
					t.Fatal(err)
				}
				c.now = func() time.Time { return now }
				var page *ExternalIncident
				c.ExternalIncidents = func(context.Context) (*ExternalIncident, error) { return page, nil }
				stepFor(t, c, &now, 10)
				if !pagesOnly {
					if !c.incidentOpen || len(openingKeys(c.currentIncidentRequest)) == 0 {
						t.Fatalf("%s did not open an incident with detector paging", name)
					}
					return
				}
				if c.incidentOpen || c.openerFiring("") {
					t.Fatalf("%s opened or held an incident under pages_only: %+v", name, c.currentIncidentRequest)
				}
				page = &ExternalIncident{ID: "page-1", Namespace: cfg.Namespace, Message: syntheticPage}
				stepFor(t, c, &now, 1)
				request := c.currentIncidentRequest
				if !c.incidentOpen || request == nil || request.Page != syntheticPage || !pageOpened(request) {
					t.Fatalf("the page did not open an incident under pages_only: %+v", request)
				}
				if len(alertKeys(request)) != 0 {
					t.Fatalf("a detector finding became an alert of the paged incident: %v", alertKeys(request))
				}
				stepFor(t, c, &now, 5)
				if len(request.JoinedKeys) != 0 {
					t.Fatalf("a detector finding joined the paged incident's alert set: %v", request.JoinedKeys)
				}
				if len(request.Findings) != 1 || !slices.Equal(request.StandingKeys, findingKeys(request.Findings)) {
					t.Fatalf("the active finding does not ride as the page's clue: findings %v standing %v", request.Findings, request.StandingKeys)
				}
			})
		}
	}
}

// Under pages_only a page that finds nothing active waits (unaccepted) until a
// finding of any kind is active, health finding or clue, so its incident has
// something the automatic stage can match; with nothing at all it opens once
// every health detector had time to confirm one. A page that finds a finding
// already active is accepted at once, and without the setting a page never
// waits.
func TestPagesOnlyPageWaitsOnlyWhileNothingIsActive(t *testing.T) {
	cases := []struct {
		name      string
		pagesOnly bool
		// active fires before the page is read, appears after the page has waited.
		active, appears string
	}{
		{name: "a clue is already active", pagesOnly: true, active: "clue"},
		{name: "a user test is already failing", pagesOnly: true, active: "user test"},
		{name: "a clue appears", pagesOnly: true, appears: "clue"},
		{name: "a user test fails", pagesOnly: true, appears: "user test"},
		{name: "nothing ever fires", pagesOnly: true},
		{name: "setting off, nothing fires"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Unix(1000, 0).UTC()
			probe := &switchedProbe{id: memory.TrafficDetectorPrefix + "web", scenario: "browse"}
			slow := &switchedProbe{id: memory.TrafficDetectorPrefix + "slow", scenario: "checkout"}
			clue := &switchedProbe{id: "synthetic-drift", scenario: "drifted"}
			switches := map[string]*switchedProbe{"user test": probe, "clue": clue}
			cfg := testControllerConfig()
			cfg.PagesOnly = tc.pagesOnly
			c, err := NewController(cfg, []sdk.Detector{probe, slowProbe{slow}, learnedClue{clue}}, staticProvider{}, &recordingDispatcher{requests: make(chan IncidentRequest, 4)}, now)
			if err != nil {
				t.Fatal(err)
			}
			c.now = func() time.Time { return now }
			if wait := c.pageEvidenceWait(); wait != 30*time.Second {
				t.Fatalf("page evidence wait %s, want the slowest health detector's confirmation time 30s", wait)
			}
			var page *ExternalIncident
			c.ExternalIncidents = func(context.Context) (*ExternalIncident, error) { return page, nil }
			if tc.active != "" {
				switches[tc.active].failing.Store(true)
				stepFor(t, c, &now, 3)
				if c.incidentOpen {
					t.Fatalf("the %s opened an incident without a page", tc.active)
				}
			}
			page = &ExternalIncident{ID: "page-1", Namespace: cfg.Namespace, Message: syntheticPage}
			opened := func(findings int) {
				t.Helper()
				request := c.currentIncidentRequest
				if !c.incidentOpen || request == nil || request.Page != syntheticPage || len(request.Findings) != findings {
					t.Fatalf("the page did not open with %d findings: %+v", findings, request)
				}
			}
			stepFor(t, c, &now, 1)
			switch {
			case tc.active != "":
				opened(1)
				return
			case !tc.pagesOnly:
				opened(0)
				return
			}
			stepFor(t, c, &now, 2)
			if c.incidentOpen || c.lastPageID != "" {
				t.Fatal("the page opened with nothing active and before the wait")
			}
			if tc.appears != "" {
				switches[tc.appears].failing.Store(true)
				stepFor(t, c, &now, 1)
				opened(1)
				return
			}
			stepFor(t, c, &now, 26)
			if c.incidentOpen {
				t.Fatal("the page opened before the evidence wait passed")
			}
			stepFor(t, c, &now, 2)
			opened(0)
		})
	}
}

// slowProbe is a user test evaluated every 10 s that needs two failing passes.
type slowProbe struct{ *switchedProbe }

func (d slowProbe) Spec() sdk.DetectorSpec {
	spec := d.switchedProbe.Spec()
	spec.Interval, spec.Persistence.Firing = 10*time.Second, 2
	return spec
}

// learnedClue is a learned detector: a finding of its own is a clue, not a
// health finding.
type learnedClue struct{ *switchedProbe }

func (d learnedClue) Spec() sdk.DetectorSpec {
	spec := d.switchedProbe.Spec()
	spec.Class, spec.Owner, spec.OriginatingIncident = sdk.DetectorClassLearned, sdk.DetectorOwnerResponder, "synthetic"
	return spec
}

// pagedRouteController is a pages_only controller whose real-traffic alert
// names a learned playbook, paged while the alert fires.
func pagedRouteController(t *testing.T, now *time.Time) (*Controller, *sameWriteWorld, *e2eRoute, IncidentRequest) {
	t.Helper()
	ctx := context.Background()
	world := e2eWorld()
	route := &e2eRoute{}
	route.shown.Store(true)
	detectors := []sdk.Detector{route}
	cfg := testControllerConfig()
	cfg.Namespace = "app"
	cfg.PagesOnly = true
	cfg.RequestMetricsOpens = true
	cfg.AfterPlaybook = settings.AfterPlaybookWait
	restart := componentRestart{id: "fix"}
	registry, err := program.NewRegistry([]program.Diagnoser{restart}, []program.Mitigator{restart})
	if err != nil {
		t.Fatal(err)
	}
	dispatcher := &stagedDispatcher{recordingDispatcher: recordingDispatcher{requests: make(chan IncidentRequest, 4)}}
	c, err := NewController(cfg, detectors, world, dispatcher, *now)
	if err != nil {
		t.Fatal(err)
	}
	c.now = func() time.Time { return *now }
	var page *ExternalIncident
	c.ExternalIncidents = func(context.Context) (*ExternalIncident, error) { return page, nil }
	for range 5 {
		*now = now.Add(time.Second)
		if err := c.Step(ctx, *now, nil); err != nil {
			t.Fatal(err)
		}
	}
	if c.incidentOpen {
		t.Fatal("the real-traffic alert opened an incident under pages_only")
	}
	page = &ExternalIncident{ID: "page-1", Namespace: cfg.Namespace, Message: syntheticPage}
	*now = now.Add(time.Second)
	if err := c.Step(ctx, *now, nil); err != nil {
		t.Fatal(err)
	}
	if !c.incidentOpen || c.currentIncidentRequest == nil {
		t.Fatal("the page did not open an incident")
	}
	request := *cloneIncidentRequest(c.currentIncidentRequest)
	request.ResponseDeadline = lifecycleio.Now().Add(time.Hour)
	request.RepairPolicy = "recorded-actions"
	executor := PlanExecutor{Client: TypedPlanClient{Client: world.client}, Source: world, Rollouts: world, Verifier: PlanHealthVerifier{Detectors: detectors, Provider: world}}
	loop := NewResponderLoop(request, registry, world, detectors, executor, 5)
	loop.ReadView = func(v sdk.DetectionContext) program.ReadView {
		return &sdktest.ReplayReadView{Snapshot: v.(sdktest.Snapshot)}
	}
	loop.saveToolState = func(context.Context, []byte) error { return nil }
	instant := newInstantRecoveryClock()
	dispatcher.stage = &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true,
		recoveryNow: instant.now, recoveryWait: instant.wait, Budget: time.Minute, RolloutTimeout: time.Minute, ObservationInterval: time.Millisecond}
	c.codePath = &CodePath{Selection: CodePathSelection{ResponseStages: []string{"deterministic", "agent"}}}
	return c, world, route, request
}

// The response pipeline on a page-opened incident under pages_only matches
// and applies the learned playbook the clue names, exactly as on a
// detector-opened incident, and the agent is not asked.
func TestPagesOnlyPagedIncidentAppliesTheLearnedPlaybook(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	c, world, _, request := pagedRouteController(t, &now)
	if len(request.Findings) != 1 || request.Findings[0].DetectorID != memory.RequestMetricsDetectorID {
		t.Fatalf("the page does not carry the real-traffic clue: %v", request.Findings)
	}
	_, reports, err := c.runResponsePipeline(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	applied := false
	for _, report := range reports {
		for _, m := range report.Mitigations {
			applied = applied || m.PlaybookId == "fix" && m.GetApplyReceipt().GetApplied()
		}
	}
	if !applied || world.restarts.Load() != 1 {
		t.Fatalf("the learned playbook was not applied once on the paged incident: writes %d, %v", world.restarts.Load(), reports)
	}
	if calls := c.dispatcher.(*stagedDispatcher).calls; calls != 0 {
		t.Fatalf("the agent was dispatched %d times after the playbook cleared the page's clue", calls)
	}
}

// A page that opened with no finding is still work under pages_only: the
// pipeline runs the automatic stage and then the agent. Without the setting
// the pin's pipeline stops on an empty finding set.
func TestPagesOnlyCluelessPageReachesTheAgent(t *testing.T) {
	for _, pagesOnly := range []bool{false, true} {
		t.Run(map[bool]string{false: "setting off", true: "pages only"}[pagesOnly], func(t *testing.T) {
			now := time.Unix(1000, 0).UTC()
			cfg := testControllerConfig()
			cfg.PagesOnly = pagesOnly
			dispatcher := &stagedDispatcher{recordingDispatcher: recordingDispatcher{requests: make(chan IncidentRequest, 4)}, stage: emptyStage{}}
			c, err := NewController(cfg, nil, staticProvider{}, dispatcher, now)
			if err != nil {
				t.Fatal(err)
			}
			c.codePath = &CodePath{Selection: CodePathSelection{ResponseStages: []string{"deterministic", "agent"}}}
			request := IncidentRequest{IncidentID: "synthetic-page", Application: "demo", Namespace: cfg.Namespace, Page: syntheticPage,
				ResponseDeadline: lifecycleio.Now().Add(time.Hour)}
			c.incidentOpen, c.currentIncidentRequest = true, cloneIncidentRequest(&request)
			if _, _, err := c.runResponsePipeline(context.Background(), request); err != nil {
				t.Fatal(err)
			}
			want := map[bool]int{false: 0, true: 1}[pagesOnly]
			if dispatcher.calls != want {
				t.Fatalf("agent dispatched %d times, want %d", dispatcher.calls, want)
			}
			if pagesOnly {
				if got := <-dispatcher.requests; got.Page != syntheticPage || len(got.Findings) != 0 {
					t.Fatalf("the agent's request is not the page alone: %+v", got)
				}
			}
		})
	}
}

// pages_only needs a controller that reads pages: watch-only ignores them and
// external mode has no detectors that could open anything.
func TestPagesOnlyRequiresADetectorMode(t *testing.T) {
	for _, mode := range settings.Modes {
		config := settings.Default()
		config.Agent.Model = "synthetic-model"
		config.Mode = mode
		config.Detection.PagesOnly = true
		err := config.Validate()
		allowed := mode == settings.ModeOperate || mode == settings.ModeFrozen
		if allowed != (err == nil) {
			t.Fatalf("mode %s with pages_only: %v", mode, err)
		}
	}
}

// The default omits pages_only from the detection digest, so installs that do
// not set it keep the pin's digest their operability reports are bound to,
// and WithSettings carries the setting to the controller.
func TestPagesOnlyOffKeepsTheDetectionDigest(t *testing.T) {
	config := settings.Default()
	encoded, err := yaml.Marshal(config.Detection)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "pages_only") {
		t.Fatalf("the default detection section names pages_only:\n%s", encoded)
	}
	digest := config.DetectionDigest()
	if (ControllerConfig{}).WithSettings(config).PagesOnly {
		t.Fatal("pages_only is on by default")
	}
	config.Detection.PagesOnly = true
	if config.DetectionDigest() == digest {
		t.Fatal("pages_only on must change the detection digest")
	}
	if !(ControllerConfig{}).WithSettings(config).PagesOnly {
		t.Fatal("WithSettings dropped detection.pages_only")
	}
}

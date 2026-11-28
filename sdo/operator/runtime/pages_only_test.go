package runtime

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/evaluation"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/requestmetrics"
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

func stepPages(t *testing.T, c *Controller, now *time.Time, count int) {
	t.Helper()
	for range count {
		*now = now.Add(time.Second)
		if err := c.Step(context.Background(), *now, nil); err != nil {
			t.Fatal(err)
		}
	}
}

func pagesOnlyOpeners() map[string]sdk.Detector {
	probe := &switchedProbe{id: memory.TrafficDetectorPrefix + "web", scenario: "browse"}
	probe.failing.Store(true)
	return map[string]sdk.Detector{
		"user test":            probe,
		"user traffic":         firingHealth{id: memory.RequestMetricsDetectorID, rule: requestmetrics.RuleErrors},
		"kubernetes condition": firingHealth{id: memory.KubernetesConditionDetectorIDs()[0], rule: "synthetic-condition"},
	}
}

// Under detection.pages_only no opener role opens, joins or holds an
// incident: a failing user test, failing user traffic and a Kubernetes
// condition are clues. A page opens one with an empty alert set and the
// finding rides in it as a clue (as on a page without the setting). Without
// the setting each of them opens an incident on its own.
func TestPagesOnlyNoDetectorFindingOpensAndAPageCarriesItAsAClue(t *testing.T) {
	for name, opener := range pagesOnlyOpeners() {
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
				stepPages(t, c, &now, 10)
				if !pagesOnly {
					if !c.incidentOpen || len(openingKeys(c.currentIncidentRequest)) == 0 {
						t.Fatalf("%s did not open an incident with detector paging", name)
					}
					return
				}
				if c.incidentOpen || c.openerFiring("") {
					t.Fatalf("%s opened or held an incident under pages_only: %+v", name, c.currentIncidentRequest)
				}
				if _, batched := c.batcher.Deadline(); batched {
					t.Fatalf("%s waits in the batcher for an incident under pages_only", name)
				}
				page = &ExternalIncident{ID: "page-1", Namespace: cfg.Namespace, Message: syntheticPage}
				stepPages(t, c, &now, 1)
				request := c.currentIncidentRequest
				if !c.incidentOpen || request == nil || request.Page != syntheticPage || !pageOpened(request) {
					t.Fatalf("the page did not open an incident under pages_only: %+v", request)
				}
				if len(alertKeys(request)) != 0 {
					t.Fatalf("a detector finding became an alert of the paged incident: %v", alertKeys(request))
				}
				stepPages(t, c, &now, 5)
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

// On this pin a page needs no evidence wait: it is accepted on the step that
// reads it, with or without the setting and with nothing active, and its
// incident goes straight to the responder (a page-opened incident with no
// automatic-stage report never runs the automatic stage here).
func TestPagesOnlyPageIsAcceptedAtOnceAndGoesToTheResponder(t *testing.T) {
	for _, pagesOnly := range []bool{false, true} {
		t.Run(map[bool]string{false: "setting off", true: "pages only"}[pagesOnly], func(t *testing.T) {
			now := time.Unix(1000, 0).UTC()
			probe := &switchedProbe{id: memory.TrafficDetectorPrefix + "web", scenario: "browse"}
			cfg := testControllerConfig()
			cfg.PagesOnly = pagesOnly
			c, err := NewController(cfg, []sdk.Detector{probe}, staticProvider{}, &recordingDispatcher{requests: make(chan IncidentRequest, 4)}, now)
			if err != nil {
				t.Fatal(err)
			}
			c.now = func() time.Time { return now }
			stepPages(t, c, &now, 3)
			c.ExternalIncidents = func(context.Context) (*ExternalIncident, error) {
				return &ExternalIncident{ID: "page-1", Namespace: cfg.Namespace, Message: syntheticPage}, nil
			}
			stepPages(t, c, &now, 1)
			request := c.currentIncidentRequest
			if !c.incidentOpen || request == nil || request.Page != syntheticPage || len(request.Findings) != 0 {
				t.Fatalf("the page was not accepted on the step that read it: %+v", request)
			}
			c.codePath = &CodePath{Selection: CodePathSelection{ResponseStages: []string{"deterministic", "agent"}}}
			if entry := c.responseEntry(*request); entry != dispatchDirectResponder {
				t.Fatalf("a page-opened incident takes entry %d, want the responder directly", entry)
			}
		})
	}
}

// No detector joins a paged incident's closure proof under pages_only;
// without it an opener detector outside the alert set does.
func TestPagesOnlyClosureAdmitsNoOpener(t *testing.T) {
	for _, pagesOnly := range []bool{false, true} {
		cfg := testControllerConfig()
		cfg.PagesOnly = pagesOnly
		detectors := []sdk.Detector{firingHealth{id: memory.KubernetesConditionDetectorIDs()[0], rule: "synthetic-condition"}}
		c, err := NewController(cfg, detectors, staticProvider{}, &recordingDispatcher{requests: make(chan IncidentRequest, 4)}, time.Unix(1000, 0))
		if err != nil {
			t.Fatal(err)
		}
		got := c.closureOpeners(context.Background(), detectors)[detectors[0].Spec().ID]
		if got == pagesOnly {
			t.Fatalf("pages_only %v: the condition detector is a closure opener: %v", pagesOnly, got)
		}
	}
}

// One --evaluate-once run (the deployment health verdict) classifies openers
// as SDO's own paging does even under pages_only, so a failing health
// detector still makes the verdict Firing.
func TestPagesOnlyEvaluateOnceStillFiresOnAnOpener(t *testing.T) {
	config := settings.Default()
	config.Agent.Model = "synthetic-model"
	config.Detection.PagesOnly = true
	data, err := yaml.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(file, data, 0o600); err != nil {
		t.Fatal(err)
	}
	broken := classedDetector("workload-health", sdk.DetectorClassHealth, true)
	got, err := evaluateOnceClassed(t, []sdk.Detector{broken}, "--settings", file)
	if err != nil || got.Status != evaluation.Firing {
		t.Fatalf("evaluate-once under pages_only: %+v, %v; want firing", got, err)
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

package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/sdk"
)

func externalCluesConfig() ControllerConfig {
	cfg := testControllerConfig()
	cfg.ExternalIncidentsOnly = true
	cfg.ExternalClues = true
	return cfg
}

// External clues keep detectors observing, but their findings never open an
// incident, join the external one or close it: the caller owns all three.
func TestExternalCluesNeverOpenJoinOrCloseTheExternalIncident(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	probe := &switchedProbe{id: "traffic-health", scenario: "browse"}
	cfg := externalCluesConfig()
	c, err := NewController(cfg, []sdk.Detector{probe}, staticProvider{}, &recordingDispatcher{}, now)
	if err != nil {
		t.Fatal(err)
	}
	c.now = func() time.Time { return now }
	if len(c.detectors) != 1 {
		t.Fatal("external clues must keep the detectors")
	}
	ctx := context.Background()

	// Idle: a failing user test is tracked as a clue and opens nothing.
	probe.failing.Store(true)
	for i := 0; i < 3; i++ {
		now = now.Add(2 * time.Second)
		if err := c.Step(ctx, now, nil); err != nil {
			t.Fatal(err)
		}
	}
	if c.incidentOpen || c.pendingClosure != nil {
		t.Fatal("a detector finding opened an incident in external-clues mode")
	}
	if active := c.tracker.Snapshot(); len(active) == 0 {
		t.Fatal("detectors stopped tracking findings")
	}
	if _, batched := c.batcher.Deadline(); batched {
		t.Fatal("an opener was queued for a later incident")
	}

	// The harness page opens the incident; later findings do not reshape it.
	alert := ExternalIncident{ID: "page-1", Namespace: cfg.Namespace, Message: "users see failures"}
	if err := c.OpenExternalIncident(ctx, now, alert); err != nil {
		t.Fatal(err)
	}
	other := &switchedProbe{id: "traffic-health", scenario: "push"}
	c.admitActivation(ctx, other.finding(), now, other.Spec())
	request := c.currentIncidentRequest
	if len(request.Findings) != 0 || len(request.OpeningKeys) != 0 || request.ExternalMessage != alert.Message {
		t.Fatalf("detector findings reshaped the external incident: %+v", request)
	}
	if _, batched := c.batcher.Deadline(); batched {
		t.Fatal("an opener was queued for a next incident")
	}

	// Findings clearing never close it; the responder's completion does.
	probe.failing.Store(false)
	for i := 0; i < 3; i++ {
		now = now.Add(2 * time.Second)
		if err := c.Step(ctx, now, nil); err != nil {
			t.Fatal(err)
		}
	}
	if !c.incidentOpen {
		t.Fatal("cleared detectors closed the external incident")
	}
	result := completedResult(request.IncidentID)
	c.currentIncidentResult = &result
	c.responderDone, c.responderCompletedAt = true, now
	if err := c.Step(ctx, now, nil); err != nil {
		t.Fatal(err)
	}
	if c.incidentOpen || c.pendingClosure == nil {
		t.Fatal("responder completion did not close the external incident")
	}
}

func TestExternalCluesRequireExternalMode(t *testing.T) {
	cfg := settings.Default()
	cfg.Agent.Model = "m"
	cfg.Response.ExternalClues = true
	if err := cfg.Validate(); err == nil {
		t.Fatal("response.external_clues accepted outside mode external")
	}
	cfg.Mode = settings.ModeExternal
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
}

// Production defaults are unchanged: external mode alone observes nothing.
func TestExternalModeDefaultsToNoClues(t *testing.T) {
	cfg := ControllerConfig{}.WithSettings(settings.Default()).WithMode(settings.ModeExternal)
	if !cfg.ExternalIncidentsOnly || cfg.ExternalClues {
		t.Fatalf("external mode default changed: %+v", cfg)
	}
	on := settings.Default()
	on.Response.ExternalClues = true
	if !(ControllerConfig{}.WithSettings(on).WithMode(settings.ModeExternal)).ExternalClues {
		t.Fatal("response.external_clues not carried")
	}
}

// Under external clues `sdo detect` is the candidate view over fresh detector
// findings; check and verify stay without detector authority.
func TestExternalCluesDetectServesCandidateViewOnly(t *testing.T) {
	request := externalGoldenRequest(t)
	request.ExternalMessage = "Investigate"
	request.Findings = nil
	request.DetectorHistory = nil
	probe := &switchedProbe{id: "traffic-health", scenario: "browse"}
	probe.failing.Store(true)
	loop := NewResponderLoop(request, nil, staticProvider{}, nil, PlanExecutor{}, 2)
	loop.Detectors, loop.ExternalClues = []sdk.Detector{probe}, true
	detect := loop.Handle(context.Background(), ToolRequest{Command: "detect"})
	if len(detect.Findings) != 1 || detect.Healthy || detect.IncidentVerified != nil {
		t.Fatalf("detect under external clues: %+v", detect)
	}
	for _, command := range []string{"check", "verify"} {
		response := loop.Handle(context.Background(), ToolRequest{Command: command})
		if len(response.Findings) != 0 || response.Healthy || response.IncidentVerified != nil {
			t.Fatalf("%s grants detector authority under external clues: %+v", command, response)
		}
	}
}

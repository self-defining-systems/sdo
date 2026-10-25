package runtime

import (
	"context"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
	"time"
)

func TestContinuationStillBrokenExcludesFindingAwaitingClearing(t *testing.T) {
	now := time.Now().UTC()
	c, err := NewController(testControllerConfig(), nil, staticProvider{snapshot: sdktest.Snapshot{}}, &verificationRetryDispatcher{}, now)
	if err != nil {
		t.Fatal(err)
	}
	c.healthDetectorIDs = []string{"health"}
	c.responderCompletedAt = now
	a := sdk.Finding{DetectorID: "health", Fingerprint: "a", Status: sdk.FindingActive}
	b := sdk.Finding{DetectorID: "health", Fingerprint: "b", Status: sdk.FindingActive}
	c.tracker.SetPolicy("health", 1, 2, 0)
	c.tracker.Observe(now, "health", []sdk.Finding{a, b})
	c.tracker.Observe(now.Add(time.Second), "health", []sdk.Finding{b})
	c.recordEvaluation("health", now.Add(time.Second), []sdk.Finding{b})
	got := c.continuationVerificationLocked(false, true, nil)
	if len(got.Findings) != 1 || FindingFingerprint(got.Findings[0]) != "b" {
		t.Fatalf("repaired finding resurrected: %+v", got)
	}
	if got.Healthy {
		t.Fatal("clearing persistence must still gate closure")
	}
}

func TestCoverageRequiresExplicitFindingProgramRelationAndKeepsAggregateMatch(t *testing.T) {
	ref := sdk.ObjectRef{Kind: "Deployment", Namespace: "app", Name: "api"}
	image := sdk.Finding{DetectorID: "health", RuleID: "image", Fingerprint: "image", Status: sdk.FindingActive, PrimaryResource: ref, Playbooks: []string{"image"}}
	env := image
	env.Fingerprint = "env"
	env.RuleID = "env"
	env.Playbooks = nil
	registry, err := program.NewRegistry([]program.Diagnoser{candidateProgram{spec: program.ProgramSpec{ID: "image", Params: []program.ParamSpec{{Name: "workload", Kind: "Deployment", Required: true}}}, run: func(context.Context, program.Bindings) program.Verdict {
		return program.Verdict{Result: program.Match, Evidence: []program.Evidence{{Fact: "endpoint", Observed: "failed", Expected: "reachable"}}}
	}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	findings := []sdk.Finding{image, env}
	runs := SelectProgramCandidates(context.Background(), registry, &sdktest.ReplayReadView{}, findings, time.Second)
	got := buildPlaybookCoverage(findings, runs, nil, nil)
	if len(got.Matches) != 1 {
		t.Fatalf("aggregate Match disappeared: %+v", got)
	}
	if len(got.Matches[0].ExplainedFindingFingerprints) != 1 || got.Matches[0].ExplainedFindingFingerprints[0] != "image" || len(got.UnexplainedFindingFingerprints) != 1 || got.UnexplainedFindingFingerprints[0] != "env" {
		t.Fatalf("shared Deployment falsely attributed: %+v", got)
	}
}

func TestApplicationReceiptsSurviveRestartBeforeFirstContinuation(t *testing.T) {
	loop, client := trustedPortLoop(t, false, false, true)
	dispatcher := KubernetesJobDispatcher{Client: client, Namespace: "app"}
	ctx := context.Background()
	if err := dispatcher.restoreToolState(ctx, loop.Request, loop); err != nil {
		t.Fatal(err)
	}
	planned := loop.Handle(ctx, ToolRequest{Command: "plan", ProgramID: "service-targetport-mismatch", Bindings: program.Bindings{Objects: program.ObjectBindings{"service": {APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}}}})
	applied := loop.Handle(ctx, ToolRequest{Command: "apply", PlanIDs: []string{planned.PlanID}})
	if planned.Error != "" || applied.Error != "" || applied.ApplicationID == "" {
		t.Fatalf("plan=%+v apply=%+v", planned, applied)
	}
	before := loop.continuationRepairs(IncidentResult{})
	if len(before) != 1 {
		t.Fatalf("expected controller receipt before restart: %+v", before)
	}
	restored := NewResponderLoop(loop.Request, loop.Programs, loop.Provider, loop.Detectors, loop.Executor, 3)
	if err := dispatcher.restoreToolState(ctx, loop.Request, restored); err != nil {
		t.Fatal(err)
	}
	after := restored.continuationRepairs(IncidentResult{})
	if len(after) != 1 || after[0].ApplicationID != applied.ApplicationID || after[0].Receipt.ActionID != before[0].Receipt.ActionID || !after[0].Receipt.Success {
		t.Fatalf("application receipt lost before first continuation: %+v", after)
	}
	if len(restored.applications) != 0 || len(restored.plans) != 0 {
		t.Fatal("receipt restore must not resurrect mutation authority")
	}
}

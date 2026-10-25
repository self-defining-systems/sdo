package runtime

import (
	"context"
	"path/filepath"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
	"time"
)

func TestContinuationStillBrokenExcludesFindingAwaitingClearing(t *testing.T) {
	now := time.Now().UTC()
	a := sdk.Finding{DetectorID: "availability", RuleID: "a", Fingerprint: "a", Status: sdk.FindingActive, Severity: sdk.SeverityCritical, Summary: "a failing", Evidence: "synthetic sample", PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "a"}, Playbooks: []string{"base"}}
	b := a
	b.RuleID = "b"
	b.Fingerprint = "b"
	b.PrimaryResource.Name = "b"
	spec := fastloopDetector{}.Spec()
	spec.Persistence.Clearing = 2
	detector := &sequenceDetector{spec: spec, samples: [][]sdk.Finding{{a, b}, {b}, nil, nil}}
	provider := staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}
	c, err := NewController(testControllerConfig(), []sdk.Detector{detector}, provider, &verificationRetryDispatcher{}, now)
	if err != nil {
		t.Fatal(err)
	}
	request := IncidentRequest{IncidentID: "clearing", Namespace: "app", Findings: []sdk.Finding{a, b}, ResponseDeadline: now.Add(time.Minute)}
	c.currentIncidentRequest = &request
	c.incidentOpen = true
	c.responderCompletedAt = now
	loop := NewResponderLoop(request, nil, provider, nil, PlanExecutor{}, 3)
	loop.VerifyClosure = func(ctx context.Context) ToolResponse { return c.responderVerification(ctx, request.IncidentID) }
	server := SessionToolServer{Directory: filepath.Join(t.TempDir(), "tools"), Handle: loop.Handle}
	if err = server.Prepare(); err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	observe := func(sample int) {
		if err := c.StepEvents(context.Background(), now.Add(time.Duration(sample)*time.Second), []sdk.WatchKind{{APIVersion: "v1", Kind: "Service"}}); err != nil {
			t.Fatal(err)
		}
	}
	observe(0)
	observe(1)
	got := exchangeSelfCheck(t, &server, `{"command":"verify"}`)
	if len(got.Findings) != 1 || FindingFingerprint(got.Findings[0]) != "b" {
		t.Fatalf("repaired finding resurrected: %+v", got)
	}
	if got.Healthy {
		t.Fatal("clearing persistence must still gate closure")
	}
	observe(2)
	pending := exchangeSelfCheck(t, &server, `{"command":"verify"}`)
	if pending.Healthy || len(pending.Findings) != 0 {
		t.Fatalf("clearing must wait without another repair: %+v", pending)
	}
	observe(3)
	if final := exchangeSelfCheck(t, &server, `{"command":"verify"}`); !final.Healthy {
		t.Fatalf("confirmed clearing never verified: %+v", final)
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

func TestCoverageRecognizesControllerOwnedProgramFinding(t *testing.T) {
	c := &Controller{}
	ref := sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}
	run := ProgramRun{ProgramID: "routing", Bindings: program.Bindings{Objects: program.ObjectBindings{"service": ref}}, Verdict: program.Verdict{Result: program.Match}}
	finding := c.programFinding(sdk.DriftEvent{Ref: ref, Fields: []sdk.DriftField{{Path: "spec.ports[0].targetPort"}}}, run)
	run.FindingFingerprint = FindingFingerprint(finding)
	got := buildPlaybookCoverage([]sdk.Finding{finding}, []ProgramRun{run}, nil, nil)
	if len(got.Matches) != 1 || len(got.Matches[0].ExplainedFindingFingerprints) != 1 || len(got.UnexplainedFindingFingerprints) != 0 {
		t.Fatalf("controller-owned finding lost its program relation: %+v", got)
	}
}

package runtime

import (
	"context"
	"k8s.io/client-go/kubernetes/fake"
	"path/filepath"
	"reflect"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
	"time"
)

// Coverage and the automatic stage share one explanation rule: a diagnosis
// Match explains each active finding that bound those roles. Two findings on
// one Deployment both bind it, so both are explained, as the stage targets them.
func TestCoverageExplainsEveryFindingThatBoundTheMatchedRoles(t *testing.T) {
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
	targets := matchedFindingFingerprints(findings, runs, "image", programBindingsState(got.Matches[0].Bindings))
	if !reflect.DeepEqual(got.Matches[0].ExplainedFindingFingerprints, []string{"env", "image"}) || !reflect.DeepEqual(targets, got.Matches[0].ExplainedFindingFingerprints) || len(got.UnexplainedFindingFingerprints) != 0 {
		t.Fatalf("coverage differs from the automatic stage's targets %v: %+v", targets, got)
	}
}

func assertRestoredApplicationReceiptReachesWorkerResult(t *testing.T, client *fake.Clientset, loop *ResponderLoop, receiptID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dispatcher := KubernetesJobDispatcher{Client: client, Namespace: "demo", Registry: testIncidentRegistry(t, "responder:test"), AgentID: testIncidentAgentID(t), RepositoryPVC: "repo", RepositoryMountPath: filepath.Dir(loop.Request.RepositoryWorktree), ServiceAccount: "responder", PollInterval: time.Millisecond, RequireMemorySnapshot: true, Catalog: fixedMemoryCatalog{loop.Request.RepositoryBaseCommit}, LoopFactory: func(IncidentRequest) *ResponderLoop { return loop }}
	type result struct {
		value IncidentResult
		err   error
	}
	done := make(chan result, 1)
	go func() { value, err := dispatcher.Dispatch(ctx, loop.Request); done <- result{value, err} }()
	job := awaitJob(t, client, IncidentJobName(loop.Request.IncidentID))
	response := completedResult(loop.Request.IncidentID)
	response.RepairActions = nil
	publishContinuationResult(t, client, job, response)
	completed := <-done
	if completed.err != nil {
		t.Fatal(completed.err)
	}
	for _, receipt := range completed.value.RepairActions {
		if receipt.ActionID == receiptID && receipt.Success {
			return
		}
	}
	t.Fatalf("worker result lost pre-restart controller receipt: %+v", completed.value.RepairActions)
}


//go:build integration

package runtime

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk"
)

func TestResponderPersistedNoProgressRechecksFreshVerification(t *testing.T) {
	for _, mode := range []string{"changed-set", "changed-evidence", "unchanged"} {
		t.Run(mode, func(t *testing.T) {
			request := goldenRequest(t)
			request.ResponseDeadline = time.Now().Add(time.Minute)
			request.MaxResponseLoops = 5
			finding := noProgressInventory(t)
			repair := noProgressFailure(finding)
			verification := ToolResponse{Findings: []sdk.Finding{finding}}
			feedback, err := continuationFeedback(2, verification, repair)
			if err != nil {
				t.Fatal(err)
			}
			result := completedResult(request.IncidentID)
			result.Status, result.Error, result.ResponseLoopsUsed = IncidentFailed, "no_progress", 2
			result.UnresolvedFindings = verification.Findings
			result.RepairActions = []RepairActionReceipt{{ActionID: repair.ActionID, Resources: repair.Receipt.Resources, FailureReason: repair.Receipt.FailureReason, StartedAt: repair.Receipt.StartedAt, CompletedAt: repair.Receipt.CompletedAt}}
			typed, err := incidentResultContract(result)
			if err != nil {
				t.Fatal(err)
			}
			loop := NewResponderLoop(request, nil, nil, nil, PlanExecutor{}, 5)
			dispatcher := KubernetesJobDispatcher{Client: fake.NewSimpleClientset(), Namespace: "demo", LoopFactory: func(IncidentRequest) *ResponderLoop { return loop }}
			if err := dispatcher.restoreToolState(context.Background(), request, loop); err != nil {
				t.Fatal(err)
			}
			if err := loop.saveContinuation(context.Background(), responderContinuation{Attempts: 2, Done: true, Feedback: string(feedback), Result: typed}); err != nil {
				t.Fatal(err)
			}
			// Restore into a new loop, exercising the durable terminal decision.
			loop = NewResponderLoop(request, nil, nil, nil, PlanExecutor{}, 5)
			if mode == "changed-set" {
				verification.Findings[0].Fingerprint += "/new"
			}
			if mode == "changed-evidence" {
				verification.Findings[0].Evidence += "; new observation"
			}
			resumed, err := dispatcher.ContinueAfterVerification(context.Background(), request, verification, time.Now())
			if err != nil || resumed != (mode != "unchanged") {
				t.Fatalf("fresh verification mode=%s resumed=%v error=%v", mode, resumed, err)
			}
			if resumed && (loop.continuation.Done || loop.continuation.Attempts != 2 || !loop.Request.ResponseDeadline.Equal(request.ResponseDeadline)) {
				t.Fatal("resumption reset original budget or remained terminal")
			}
		})
	}
}

func TestResponderProgressContinuesToRecovery(t *testing.T) {
	for _, mode := range []string{"advancing-health", "uncertain-write"} {
		t.Run(mode, func(t *testing.T) {
			request := goldenRequest(t)
			request.ResponseDeadline = time.Now().Add(time.Minute)
			request.RepositoryWorktree = filepath.Join(t.TempDir(), "proposal")
			request.MaxResponseLoops = 5
			finding := noProgressInventory(t)
			if mode == "advancing-health" {
				finding.DetectorID = "health"
				finding.Metadata = nil
				finding.Evidence = "ready=0/3"
			}
			client := fake.NewSimpleClientset()
			loop := NewResponderLoop(request, nil, nil, nil, PlanExecutor{}, 5)
			samples := 0
			loop.VerifyResult = func(context.Context, IncidentResult) ToolResponse {
				samples++
				if samples == 3 {
					return ToolResponse{Healthy: true, Summary: "independent recovery confirmed"}
				}
				current := finding
				if samples == 2 && mode == "advancing-health" {
					current.Evidence = "ready=2/3"
				}
				return ToolResponse{Summary: "independent findings remain", Findings: []sdk.Finding{current}}
			}
			dispatcher := KubernetesJobDispatcher{Client: client, Namespace: "demo", Registry: testIncidentRegistry(t, "responder:test"), AgentID: testIncidentAgentID(t), RepositoryPVC: "repo", RepositoryMountPath: filepath.Dir(request.RepositoryWorktree), ServiceAccount: "responder", PollInterval: time.Millisecond, LoopFactory: func(IncidentRequest) *ResponderLoop { return loop }}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			type reply struct {
				result IncidentResult
				err    error
			}
			done := make(chan reply, 1)
			go func() { result, err := dispatcher.Dispatch(ctx, request); done <- reply{result, err} }()
			result := completedResult(request.IncidentID)
			repair := noProgressFailure(finding)
			reason := "outcome_unknown"
			success := false
			if mode == "advancing-health" {
				reason = ""
				success = true
			}
			result.RepairActions = []RepairActionReceipt{{ActionID: repair.ActionID, Kind: "sdk-plan", Summary: "synthetic mutation", Target: "ServiceAccount/app/cleanup", Details: "synthetic receipt", Resources: repair.Receipt.Resources, FailureReason: reason, Success: success, StartedAt: repair.Receipt.StartedAt, CompletedAt: repair.Receipt.CompletedAt}}
			for turn := 0; turn < 3; turn++ {
				id := request.IncidentID
				if turn > 0 {
					id += fmt.Sprintf("-continue-%d", turn)
				}
				job := awaitJob(t, client, IncidentJobName(id))
				publishContinuationResult(t, client, job, result)
			}
			outcome := <-done
			if outcome.err != nil || outcome.result.Status != IncidentCompleted || outcome.result.ResponseLoopsUsed != 3 || outcome.result.ResponseLoopExhausted {
				t.Fatalf("progress did not reach recovery: %+v error=%v", outcome.result, outcome.err)
			}
		})
	}
}

//go:build integration

package runtime

import (
	"context"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"path/filepath"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"testing"
	"time"
)

func TestHermeticTrustedLatentDriftFastPathSkipsResponderAndReflection(t *testing.T) {
	loop, client := trustedPortLoop(t, false, false, true)
	loop.Detectors = []sdk.Detector{fastloopClearDetector{}}
	finding := loop.Request.Findings[0]
	finding.DetectorID = "drift.service-targetport-mismatch"
	finding.Severity = sdk.SeverityLatent
	finding.ParameterBindings = map[string]sdk.ObjectRef{"service": finding.PrimaryResource}
	request := goldenRequest(t)
	request.Namespace = "app"
	request.SourceCommit = "tree-a"
	request.RepairPolicy = "recorded-actions"
	request.Findings = []sdk.Finding{finding}
	request.DriftEvents = []sdk.DriftEvent{{Ref: finding.PrimaryResource, Classification: sdk.DriftFault, ProgramID: "service-targetport-mismatch", ObservedAt: time.Now().UTC()}}
	request.RepositoryWorktree = "/workspace/worktrees/" + request.IncidentID
	outcomes := []memory.Outcome{{IncidentID: "earlier-drift", Record: verifiedTrustOutcome(t, "service-targetport-mismatch", "tree-a")}}
	dispatcher := KubernetesJobDispatcher{Client: client, Namespace: "app", Registry: testIncidentRegistry(t, "responder:test"), AgentID: testIncidentAgentID(t),
		ServiceAccount: "sdo-responder", RepositoryPVC: "repository", RepositoryMountPath: "/workspace",
		ProgramFastPath: true, ProgramTrustThreshold: 1, Outcomes: priorOutcomeReader{outcomes: outcomes},
		LoopFactory: func(request IncidentRequest) *ResponderLoop { loop.Request = request; return loop }}
	prepared, err := dispatcher.PrepareDispatch(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	result, err := dispatcher.Dispatch(context.Background(), prepared)
	if err != nil || result.ResolutionPath != "fast_path" || len(result.FastPathPrograms) != 1 {
		t.Fatalf("trusted latent drift did not resolve: %+v %v", result, err)
	}
	jobs, err := client.BatchV1().Jobs("app").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(jobs.Items) != 0 {
		t.Fatalf("responder Job dispatched: %+v %v", jobs, err)
	}
	target := nativeRepository(t)
	repository, err := broker.New(target, filepath.Join(t.TempDir(), "proposals"), nil)
	if err != nil {
		t.Fatal(err)
	}
	reflections := 0
	native := NativeIncidentBroker{Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"),
		ResponderBackend: "test", ResponderModel: "test", ForceReflection: true,
		Deriver: OutcomeDeriverFunc(func(_ context.Context, closure IncidentClosure, _, _, _ string) (DerivedOutcome, error) {
			derived := testDerivedOutcome(closure.Request.IncidentID, "success", true)
			derived.Record.ResolutionPath = closure.Result.ResolutionPath
			derived.Record.FastPathPrograms = closure.Result.FastPathPrograms
			return derived, nil
		}),
		Reflector: ReflectionWorkerFunc(func(context.Context, IncidentClosure, IncidentWorkspace, *contracts.OutcomeRecord, string, string, ReflectionFeedback) (ReflectionDecision, error) {
			reflections++
			return ReflectionDecision{}, nil
		})}
	workspace, err := native.PrepareIncident(context.Background(), "trusted-drift")
	if err != nil {
		t.Fatal(err)
	}
	closure := nativeClosure(workspace)
	closure.Request.RepairPolicy = "recorded-actions"
	result.IncidentID = closure.Request.IncidentID
	closure.Result = &result
	if _, err := native.ProcessClosure(context.Background(), closure); err != nil {
		t.Fatal(err)
	}
	ledger, err := native.readLedger("trusted-drift")
	if err != nil || reflections != 0 || ledger.ReflectionSkippedReason != "trusted program fast path" {
		t.Fatalf("trusted drift dispatched reflection: ledger=%+v calls=%d err=%v", ledger, reflections, err)
	}
}

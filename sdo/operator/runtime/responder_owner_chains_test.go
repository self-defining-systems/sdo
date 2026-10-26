package runtime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk/sdktest"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/sdk"
)

// REV12 F1: a worker can write any owner chain into its result. Cause
// attribution must follow only owner references SDO observed itself.
func TestWorkerReportedOwnerChainsAreIgnored(t *testing.T) {
	request := goldenRequest(t)
	task, _, err := encodeIncidentAgentTask(request, testIncidentRegistry(t, ""), testIncidentAgentID(t))
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 10, 26, 15, 18, 0, 0, time.UTC)
	unrelated := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "unrelated-7c9d"}
	result := completedResult(request.IncidentID)
	result.RepairActions = []RepairActionReceipt{{
		ActionID: "sdo-act-forged", Kind: "kubectl-delete", Target: "Pod/unrelated-7c9d", Resources: []sdk.ObjectRef{unrelated},
		Summary: "delete", Details: "delete", StartedAt: at, CompletedAt: at.Add(time.Second), Success: true,
		Owners: []ResourceOwners{{Resource: unrelated, OwnerChain: []sdk.ObjectRef{{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "cause"}}}},
	}}
	typed, err := incidentResultToProto(result)
	if err != nil {
		t.Fatal(err)
	}
	if len(typed.GetRepairActions()[0].GetOwners()) != 1 {
		t.Fatal("fixture lost its forged chain before the worker boundary")
	}
	packed, err := anypb.New(typed)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := protojson.Marshal(&contracts.AgentResult{SchemaVersion: ProtocolSchemaVersion, TaskId: task.GetTaskId(), Status: string(result.Status), ProposalId: task.GetWorkspace().GetProposalId(), Response: packed})
	if err != nil {
		t.Fatal(err)
	}
	agentResult := &contracts.AgentResult{}
	if err := wire.Unmarshal(payload, agentResult); err != nil {
		t.Fatal(err)
	}
	decoded, err := incidentResultFromAgent(agentResult, task, testIncidentRegistry(t, "test/image"))
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.RepairActions) != 1 || len(decoded.RepairActions[0].Owners) != 0 {
		t.Fatalf("worker-reported owner chain reached SDO: %#v", decoded.RepairActions)
	}
}

// The broker records `sdo act` owner chains from the incident's open capture
// before the outcome is committed; a write the capture never observed has none.
func TestNativeBrokerDerivesOwnerChainsFromTheOpenCapture(t *testing.T) {
	t.Parallel()
	const incidentID = "inc-owned-write"
	target := nativeRepository(t)
	repositoryBroker, err := broker.New(target, filepath.Join(t.TempDir(), "proposals"), nil)
	if err != nil {
		t.Fatal(err)
	}
	observed := &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "ReplicaSet", Namespace: "app", Name: "search-old"}
	unobserved := &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "ReplicaSet", Namespace: "app", Name: "search-new"}
	native := NativeIncidentBroker{
		Repository: repositoryBroker, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), SessionRoot: t.TempDir(), ResponderBackend: "test", ResponderModel: "test",
		Deriver: OutcomeDeriverFunc(func(_ context.Context, c IncidentClosure, _, _, _ string) (DerivedOutcome, error) {
			outcome := testDerivedOutcome(c.Request.IncidentID, "cleared_without_action", false)
			outcome.Record.RepairActions = []*contracts.RepairActionReceipt{{ActionId: "sdo-act-1", Kind: "kubectl-scale", Target: "ReplicaSet/search-old", Summary: "scale", Details: "scale", Success: true, Resources: []*contracts.ObjectRef{observed, unobserved}, StartedAt: timestamppb.New(c.DetectedAt), CompletedAt: timestamppb.New(c.ResponderCompletedAt)}}
			return outcome, nil
		}),
	}
	capture, err := memory.IncidentCorpusDir(native.SessionRoot, incidentID)
	if err != nil {
		t.Fatal(err)
	}
	controller := true
	at := time.Date(2026, 10, 26, 15, 18, 0, 0, time.UTC)
	open, err := sdktest.CaptureSnapshot(sdktest.Snapshot{NamespaceName: "app", ObservedAtTime: &at, ReplicaSetList: []appsv1.ReplicaSet{{ObjectMeta: metav1.ObjectMeta{Name: "search-old", Namespace: "app", OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: "search", Controller: &controller}}}}}}, "app")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(open)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(capture, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(capture, "open@detect.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	workspace, err := native.PrepareIncident(context.Background(), incidentID)
	if err != nil {
		t.Fatal(err)
	}
	closure := nativeClosure(workspace)
	closure.Request.RepairPolicy = "recorded-actions"
	if _, err := native.ProcessClosure(context.Background(), closure); err != nil {
		t.Fatal(err)
	}
	ledger, err := native.readLedger(incidentID)
	if err != nil {
		t.Fatal(err)
	}
	owners := ledger.GetOutcomeDocument().GetRepairActions()[0].GetOwners()
	if len(owners) != 1 || owners[0].GetResource().GetName() != "search-old" || len(owners[0].GetOwnerChain()) != 1 || owners[0].GetOwnerChain()[0].GetName() != "search" {
		t.Fatalf("owners = %v, want only search-old -> Deployment/search", owners)
	}
}

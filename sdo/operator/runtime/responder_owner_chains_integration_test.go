//go:build integration

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

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

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


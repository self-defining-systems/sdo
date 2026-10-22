package runtime

import (
	"context"
	"os"
	"path/filepath"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

func wireState(t *testing.T) *contracts.ControllerState {
	now := time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC)
	closure := nativeClosure(IncidentWorkspace{IncidentID: "incident-wire", Worktree: "/work", BaseCommit: "base"})
	closure.Request.StateChanges = &StateChanges{BaselineAt: now, ObservedAt: now, Changes: []StateChange{}}
	closure.FinalStateChanges = &StateChanges{BaselineAt: now, ObservedAt: now, Changes: []StateChange{}}
	request := closure.Request
	finding := sdk.Finding{
		DetectorID: "health", RuleID: "rule", Status: sdk.FindingActive, Severity: sdk.SeverityCritical, Summary: "s", Evidence: "e",
		PrimaryResource: sdk.ObjectRef{Kind: "Service", Name: "web"}, Fingerprint: "fp", Metadata: map[string]any{"count": float64(2)},
	}
	return &contracts.ControllerState{
		Version:            RuntimeStateVersion,
		SchedulerDeadlines: timestampsContract(map[string]time.Time{"health": now}),
		FindingStates: mustConvert(t, findingStatesContract, map[string]FindingState{
			"health/fp": {DetectorID: "health", Finding: finding, FiringCount: 2, Active: true, FirstSeenAt: now, Activations: 1},
		}),
		PendingBatch:    &contracts.BatcherState{Findings: mustConvert(t, findingsContract, []sdk.Finding{finding}), Deadlines: timestampsContract(map[string]time.Time{"health/fp": now}), Deadline: contractTimestamp(now)},
		History:         evaluationsContract([]DetectorEvaluation{{DetectorID: "health", EvaluatedAt: now, Status: DetectorEvaluationFiring, Fingerprints: []string{"fp"}}}),
		IncidentOpen:    true,
		IncidentRequest: mustProject(t, incidentRequestContract, request),
		IncidentResult:  mustProject(t, incidentResultContract, *closure.Result),
		DispatchState:   contracts.DispatchState_DISPATCH_STATE_PENDING, IncidentFindingKeys: []string{"health/fp"},
		IncidentDetectedAt: contractTimestamp(now),
		PendingClosure:     mustProject(t, incidentClosureContract, closure),
		ClosureFailure:     &contracts.ClosureFailure{IncidentId: "incident-wire", Attempts: 1, MaxAttempts: 3, LastError: "x", FirstFailedAt: contractTimestamp(now), LastFailedAt: contractTimestamp(now)},
		IncidentView: mustProject(t, incidentViewContract, &IncidentView{
			IncidentID: "incident-wire", ObservedAt: now, BlockingDetectors: []string{"health"}, BlockingFindings: []sdk.Finding{finding},
		}),
	}
}

func TestStateStorePersistsCanonicalProtoJSON(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	store := lifecycleio.NewConfigMapStateStore(client, "demo", "sdo-controller-state")
	state := wireState(t)
	if _, err := store.Save(ctx, state, ""); err != nil {
		t.Fatalf("save: %v", err)
	}
	configMap, err := client.CoreV1().ConfigMaps("demo").Get(ctx, "sdo-controller-state", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	persisted := &contracts.ControllerState{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal([]byte(configMap.Data[stateDataKey]), persisted); err != nil {
		t.Fatalf("runtime state is not canonical ProtoJSON: %v\n%s", err, configMap.Data[stateDataKey])
	}
	if persisted.GetIncidentView().GetIncidentId() != "incident-wire" || persisted.GetPendingClosure().GetRequest().GetIncidentId() != "incident-wire" {
		t.Fatalf("persisted state lost its incident view or closure: %v", persisted)
	}
	loaded, _, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	want := proto.Clone(state).(*contracts.ControllerState)
	got := proto.Clone(loaded).(*contracts.ControllerState)
	want.IncidentView, got.IncidentView = nil, nil
	if !proto.Equal(want, got) {
		t.Fatalf("state changed across a save and load:\nwant %v\ngot  %v", want, got)
	}

}

func TestStateStoreSurvivesRepeatedSavesWithoutRewriting(t *testing.T) {
	ctx := context.Background()
	store := lifecycleio.NewConfigMapStateStore(fake.NewSimpleClientset(), "demo", "sdo-controller-state")
	state := wireState(t)
	revision, err := store.Save(ctx, state, "")
	if err != nil {
		t.Fatal(err)
	}
	again, err := store.Save(ctx, state, revision)
	if err != nil || again != revision {
		t.Fatalf("an unchanged state must not advance the revision: %q %q %v", revision, again, err)
	}
}

func TestStateStoreRejectsLegacyEncodingTags(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "sdo-controller-state", Namespace: "demo", Annotations: map[string]string{stateRevisionAnnotation: "1"}},
		Data:       map[string]string{stateDataKey: `{"version":"sdo.controller/v1","Unexpected":true}`},
	})
	if _, _, err := lifecycleio.NewConfigMapStateStore(client, "demo", "sdo-controller-state").Load(ctx); err == nil {
		t.Fatal("unknown state fields must be rejected, not ignored")
	}
}

func TestBrokerLedgerIsPersistedAsProtoJSON(t *testing.T) {
	root := filepath.Join(t.TempDir(), "ledger")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	n := NativeIncidentBroker{LedgerRoot: root}
	closure, err := incidentClosureContract(nativeClosure(IncidentWorkspace{IncidentID: "incident-ledger", Worktree: "/work", BaseCommit: "base"}))
	if err != nil {
		t.Fatal(err)
	}
	ledger := &contracts.BrokerLedger{
		IncidentId: "incident-ledger", Closure: closure, ProposalCommit: "abc",
		Receipt: &contracts.ClosureReceipt{IncidentId: "incident-ledger", OutcomeCommit: "def", AckToken: "t"},
	}
	if err := n.saveLedger(ledger); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(n.ledgerPath("incident-ledger"))
	if err != nil {
		t.Fatal(err)
	}
	persisted := &contracts.BrokerLedger{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(data, persisted); err != nil {
		t.Fatalf("ledger is not canonical ProtoJSON: %v\n%s", err, data)
	}
	read, err := n.readLedger("incident-ledger")
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(read, ledger) {
		t.Fatalf("ledger changed across a save and read: %v vs %v", read, ledger)
	}
}

func TestStateStoreNamesThePreCutoverConfigMapAndTheRecovery(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "sdo-controller-state", Namespace: "demo", Annotations: map[string]string{stateRevisionAnnotation: "1"}},
		Data:       map[string]string{stateDataKey: `{"version":"sdo.controller/v1","dispatch_state":"DISPATCH_STATE_IDLE","playbooks":["old"]}`},
	})
	_, _, err := lifecycleio.NewConfigMapStateStore(client, "demo", "sdo-controller-state").Load(context.Background())
	if err == nil {
		t.Fatal("a pre-cutover state document must not load")
	}
	for _, want := range []string{"demo/sdo-controller-state", "predates the ProtoJSON cutover", "delete the ConfigMap"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not mention %q", err, want)
		}
	}
}

func TestBrokerLedgerNamesThePreCutoverFileAndTheRecovery(t *testing.T) {
	root := t.TempDir()
	n := NativeIncidentBroker{LedgerRoot: root}
	path := n.ledgerPath("incident-old")
	if err := os.WriteFile(path, []byte(`{"incident_id":"incident-old","closure":{"request":{"playbooks":["old"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := n.readLedger("incident-old")
	if err == nil {
		t.Fatal("a pre-cutover ledger must not load")
	}
	for _, want := range []string{path, "predates the ProtoJSON cutover", "archive or remove the ledger"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not mention %q", err, want)
		}
	}
}

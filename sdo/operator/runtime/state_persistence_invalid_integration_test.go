//go:build integration

package runtime

import (
	"context"
	"path/filepath"
	"sdo.dev/operator/broker"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/internal/lifecycleio"
)

// The recovered payload goes through the real broker, outcome derivation,
// receipt and queued reflection, rather than merely passing validation.
func TestInvalidStateRecoveryProducesFailedOutcomeAndReflection(t *testing.T) {
	ctx := context.Background()
	repository, err := broker.New(nativeRepository(t), filepath.Join(t.TempDir(), "proposals"), nil)
	if err != nil {
		t.Fatal(err)
	}
	reflected := 0
	native := NativeIncidentBroker{Repository: repository, LedgerRoot: t.TempDir(), Deriver: ProtoOutcomeDeriver{}, ResponderBackend: "test", ResponderModel: "test",
		Reflector: ReflectionWorkerFunc(func(_ context.Context, _ IncidentClosure, _ IncidentWorkspace, outcome *contracts.OutcomeRecord, _, _ string, _ ReflectionFeedback) (ReflectionDecision, error) {
			reflected++
			failure := outcome.GetStatePersistenceFailure()
			if outcome.GetClassification() != "failed" || failure.GetReason() != "state_persistence_invalid" || !strings.Contains(failure.GetDetail(), "repair_actions[0].failure_reason") || !strings.Contains(failure.GetDetail(), "action_reports[0].failure_reason") || len(outcome.GetRepairActions()) != 0 {
				t.Errorf("invalid evidence reached reflection: %v", outcome)
			}
			return ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "invalid controller evidence quarantined"}, nil
		})}
	workspace, err := native.PrepareIncident(ctx, "state-invalid")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1000, 0).UTC()
	c := newClosureRetryController(t, ClosureRetryPolicy{}, native, &now)
	closure := nativeClosure(workspace)
	closure.Result.ResponderSessionID = "responder-state-invalid"
	c.pendingClosure = &closure
	store := lifecycleio.NewConfigMapStateStore(fake.NewSimpleClientset(), "demo", "state")
	if err := c.AttachStateStore(ctx, store); err != nil {
		t.Fatal(err)
	}
	if err := c.PersistState(ctx); err != nil {
		t.Fatal(err)
	}
	injectLegacyDatabaseFailure(t, c)
	if err := c.PersistState(ctx); err != nil {
		t.Fatal(err)
	}
	saved, _, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	recovered := incidentClosureState(saved.GetPendingClosure())
	receipt, err := closeAndReflect(t, ctx, native, recovered)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.OutcomeCommit == "" || reflected != 1 {
		t.Fatalf("closure/reflection stalled: receipt=%v reflected=%d", receipt, reflected)
	}
}


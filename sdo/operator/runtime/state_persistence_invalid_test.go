package runtime

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/protobuf/proto"
	"path/filepath"
	"sdo.dev/operator/broker"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/runtime/internal/lifecycleio"
)

// Replay the real producer of the rejected database_authentication_rejected
// reason through both receipt projections and the production state store.
func TestDatabaseFailureProducerPersistsAndCloses(t *testing.T) {
	ctx := context.Background()
	result, err := applyDatabaseUser(t, &databaseFake{writeErr: &DatabaseOperationError{Step: DatabaseAuthentication, Reason: databaseRejected}})
	if err == nil {
		t.Fatal("expected authentication rejection")
	}
	now := time.Unix(1000, 0).UTC()
	c := newClosureRetryController(t, ClosureRetryPolicy{}, &rejectingIncidentBroker{}, &now)
	c.pendingClosure.Result.PlanApplications = []RecordedPlanApplication{{ID: "database", Result: result, Error: err.Error(), StartedAt: now, CompletedAt: now}}
	c.pendingClosure.Result.RepairActions = ProgramRepairReceipts(c.pendingClosure.Result.PlanApplications, nil)
	store := lifecycleio.NewConfigMapStateStore(fake.NewSimpleClientset(), "demo", "state")
	if err := c.AttachStateStore(ctx, store); err != nil {
		t.Fatal(err)
	}
	if err := c.PersistState(ctx); err != nil {
		t.Fatal(err)
	}
	saved, _, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(saved.GetPendingClosure().GetResult().GetPlanApplications()) != 1 {
		t.Fatal("producer evidence was discarded")
	}
	attemptClosure(t, c)
	if c.closureReceipt == nil {
		t.Fatal("no closure receipt")
	}
}

func TestInvalidStatePersistsFailedClosureAndRestarts(t *testing.T) {
	for _, corrupt := range []string{"failure_reason", "failure_reason_cold", "history"} {
		t.Run(corrupt, func(t *testing.T) {
			ctx := context.Background()
			now := time.Unix(1000, 0).UTC()
			c := newClosureRetryController(t, ClosureRetryPolicy{}, &rejectingIncidentBroker{}, &now)
			store := lifecycleio.NewConfigMapStateStore(fake.NewSimpleClientset(), "demo", "state")
			if err := c.AttachStateStore(ctx, store); err != nil {
				t.Fatal(err)
			}
			if corrupt != "failure_reason_cold" {
				persistDispatchCheckpoint(t, c)
			}
			if strings.HasPrefix(corrupt, "failure_reason") {
				injectLegacyDatabaseFailure(t, c)
			} else {
				c.history = []DetectorEvaluation{{DetectorID: "health", Status: DetectorEvaluationStatus("invalid")}}
			}
			if err := c.PersistState(ctx); err != nil {
				t.Fatalf("invalid state wedged: %v", err)
			}
			saved, _, err := store.Load(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := wire.Validate(saved); err != nil {
				t.Fatal(err)
			}
			closure := saved.GetPendingClosure()
			if closure.GetResult().GetStatus() != "failed" || !strings.Contains(closure.GetDispatchError(), "state_persistence_invalid") || !strings.Contains(closure.GetDispatchError(), strings.TrimSuffix(corrupt, "_cold")) {
				t.Fatalf("missing typed failure and offending field: %v", closure)
			}
			if err := c.RestoreState(saved); err != nil {
				t.Fatal(err)
			}
			attemptClosure(t, c)
			if c.closureReceipt == nil {
				t.Fatal("failed closure never produced receipt")
			}
		})
	}
}

func TestDatabaseFailureReasonsBelongToBothReceiptContracts(t *testing.T) {
	allowed := func(value string, values []string) bool {
		for _, v := range values {
			if v == value {
				return true
			}
		}
		return false
	}
	steps := []DatabaseOperationStep{DatabaseContainerLookup, DatabaseExecTransport, DatabaseClientBinary, DatabaseAuthentication, DatabaseCatalogRead, DatabaseCatalogParse, DatabaseMutation, "future-step"}
	reasons := []DatabaseFailureReason{DatabaseUnavailable, databaseForbidden, databaseTimeout, databaseNotFound, databaseClientFailed, databaseRejected, databaseIdentityChanged, databaseInvalidProjection, databaseAmbiguousAuth, "future-reason"}
	for _, step := range steps {
		for _, reason := range reasons {
			code := databaseFailureCode(&DatabaseOperationError{Step: step, Reason: reason})
			if !allowed(code, wire.StringIn(&contracts.ActionReport{}, "failure_reason")) || !allowed(code, wire.StringIn(&contracts.RepairActionReceipt{}, "failure_reason")) {
				t.Errorf("%s/%s produces unpersistable reason %q", step, reason, code)
			}
		}
	}
}

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

func injectLegacyDatabaseFailure(t *testing.T, c *Controller) {
	t.Helper()
	result, err := applyDatabaseUser(t, &databaseFake{writeErr: &DatabaseOperationError{Step: DatabaseAuthentication, Reason: databaseRejected}})
	if err == nil {
		t.Fatal("expected authentication rejection")
	}
	// Exact historical value captured from the failed live action report.
	result.ActionReports[0].FailureReason = "database_authentication_rejected"
	c.pendingClosure.Result.PlanApplications = []RecordedPlanApplication{{ID: "database", Result: result, Error: err.Error(), StartedAt: c.pendingClosure.DispatchedAt, CompletedAt: c.pendingClosure.ResponderCompletedAt}}
	c.pendingClosure.Result.RepairActions = ProgramRepairReceipts(c.pendingClosure.Result.PlanApplications, nil)
}

// The last saved live state was dispatch PENDING, with no closure payload.
func persistDispatchCheckpoint(t *testing.T, c *Controller) {
	t.Helper()
	closure := c.pendingClosure
	c.currentIncidentRequest = cloneIncidentRequest(&closure.Request)
	c.incidentDetectedAt = closure.DetectedAt
	c.incidentDispatchedAt = closure.DispatchedAt
	c.responderCompletedAt = closure.ResponderCompletedAt
	c.pendingClosure = nil
	c.closureState = contracts.ClosureState_CLOSURE_STATE_UNSPECIFIED
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_PENDING
	if err := c.PersistState(context.Background()); err != nil {
		t.Fatal(err)
	}
	c.pendingClosure = closure
	c.closureState = contracts.ClosureState_CLOSURE_STATE_PENDING
	c.currentIncidentResult = cloneIncidentResult(closure.Result)
}

func TestInvalidStateRecoveryRetainsCommittedReceiptAcrossRetries(t *testing.T) {
	for _, saveFailures := range []int{0, 2} {
		for _, phase := range []contracts.ClosureState{contracts.ClosureState_CLOSURE_STATE_COMMITTED, contracts.ClosureState_CLOSURE_STATE_ACKNOWLEDGING} {
			t.Run(fmt.Sprintf("%s/save-failures-%d", phase, saveFailures), func(t *testing.T) {
				ctx := context.Background()
				now := time.Unix(1000, 0).UTC()
				broker := &rejectingIncidentBroker{}
				c := newClosureRetryController(t, ClosureRetryPolicy{}, broker, &now)
				store := &retryFailingStateStore{StateStore: lifecycleio.NewConfigMapStateStore(fake.NewSimpleClientset(), "demo", "state")}
				if err := c.AttachStateStore(ctx, store); err != nil {
					t.Fatal(err)
				}
				if err := c.PersistState(ctx); err != nil {
					t.Fatal(err)
				}
				attemptClosure(t, c)
				c.closureState = phase
				if err := c.PersistState(ctx); err != nil {
					t.Fatal(err)
				}
				before, _, err := store.Load(ctx)
				if err != nil {
					t.Fatal(err)
				}
				// ExportState normally normalizes an in-flight ACK for restart.
				// Also exercise an ACKNOWLEDGING checkpoint written by a store.
				if phase == contracts.ClosureState_CLOSURE_STATE_ACKNOWLEDGING {
					before.ClosureState = phase
					_, revision, err := store.Load(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := store.Save(ctx, before, revision); err != nil {
						t.Fatal(err)
					}
					if err := c.AttachStateStore(ctx, store); err != nil {
						t.Fatal(err)
					}
				}
				store.failures = saveFailures
				for retry := 0; retry < 4; retry++ {
					c.history = []DetectorEvaluation{{DetectorID: "health", Status: DetectorEvaluationStatus("invalid")}}
					err := c.PersistState(ctx)
					if retry < saveFailures {
						if err == nil {
							t.Fatal("expected injected save failure")
						}
						if effect, ok := c.PendingClosureAcknowledgmentEffect(); ok {
							if err := c.ExecuteClosureAcknowledgmentEffect(ctx, effect); !errors.Is(err, ErrEffectNotDurable) {
								t.Fatalf("acknowledgment was not gated after failed save: %v", err)
							}
						}
					} else if err != nil {
						t.Fatalf("retry %d: %v", retry, err)
					}
					if c.closureReceipt == nil {
						t.Fatal("accepted receipt lost in memory")
					}
					saved, _, err := store.Load(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if !proto.Equal(saved.GetClosureReceipt(), before.GetClosureReceipt()) || saved.GetClosureState() != before.GetClosureState() {
						t.Fatalf("accepted receipt changed on retry %d: %v", retry, saved)
					}
				}
				// Restart through the real store, then consume the retained receipt.
				c = newClosureRetryController(t, ClosureRetryPolicy{}, broker, &now)
				if err := c.AttachStateStore(ctx, store); err != nil {
					t.Fatal(err)
				}
				if err := c.PersistState(ctx); err != nil {
					t.Fatal(err)
				}
				ack, ok := c.PendingClosureAcknowledgmentEffect()
				if !ok {
					t.Fatal("accepted receipt cannot be acknowledged")
				}
				if err := c.ExecuteClosureAcknowledgmentEffect(ctx, ack); err != nil {
					t.Fatal(err)
				}
				awaitBrokerResult(t, c.acknowledgmentResults)
				c.processBrokerCompletions()
				if err := c.PersistState(ctx); err != nil {
					t.Fatal(err)
				}
				if broker.processCalls() != 1 {
					t.Fatalf("closure processed %d times", broker.processCalls())
				}
			})
		}
	}
}

type retryFailingStateStore struct {
	StateStore
	failures int
}

func (s *retryFailingStateStore) Save(ctx context.Context, state *contracts.ControllerState, revision string) (string, error) {
	if s.failures > 0 {
		s.failures--
		return "", errors.New("injected persistence failure")
	}
	return s.StateStore.Save(ctx, state, revision)
}

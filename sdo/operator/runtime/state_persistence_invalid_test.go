package runtime

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"google.golang.org/protobuf/proto"
	"io/fs"
	"path/filepath"
	"sdo.dev/operator/broker"
	"strconv"
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
	for _, corrupt := range []string{"failure_reason", "history"} {
		t.Run(corrupt, func(t *testing.T) {
			ctx := context.Background()
			now := time.Unix(1000, 0).UTC()
			c := newClosureRetryController(t, ClosureRetryPolicy{}, &rejectingIncidentBroker{}, &now)
			store := lifecycleio.NewConfigMapStateStore(fake.NewSimpleClientset(), "demo", "state")
			if err := c.AttachStateStore(ctx, store); err != nil {
				t.Fatal(err)
			}
			if err := c.PersistState(ctx); err != nil {
				t.Fatal(err)
			}
			if corrupt == "failure_reason" {
				c.pendingClosure.Result.RepairActions = []RepairActionReceipt{{ActionID: "database", FailureReason: "database_authentication_rejected"}}
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
			if closure.GetResult().GetStatus() != "failed" || !strings.Contains(closure.GetDispatchError(), "state_persistence_invalid") || !strings.Contains(closure.GetDispatchError(), corrupt) {
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
			if outcome.GetClassification() != "failed" || failure.GetReason() != "state_persistence_invalid" || !strings.Contains(failure.GetDetail(), "failure_reason") || len(outcome.GetRepairActions()) != 0 {
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
	c.pendingClosure.Result.RepairActions = []RepairActionReceipt{{ActionID: "database", FailureReason: "database_authentication_rejected"}}
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

// Literal producers are checked structurally; computed database reasons are
// swept above. Descriptor rules, not another hand-maintained list, own values.
func TestFailureReasonProducersClosedEnums(t *testing.T) {
	allowed := map[string]bool{}
	for _, message := range []proto.Message{&contracts.ActionReport{}, &contracts.RepairActionReceipt{}, &contracts.AgentResult{}, &contracts.ReflectionDecision{}} {
		for _, value := range wire.StringIn(message, "failure_reason") {
			allowed[value] = true
		}
	}
	files, err := parser.ParseDir(token.NewFileSet(), ".", func(info fs.FileInfo) bool { return !strings.HasSuffix(info.Name(), "_test.go") }, 0)
	if err != nil {
		t.Fatal(err)
	}
	check := func(node ast.Expr) {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return
		}
		value, err := strconv.Unquote(literal.Value)
		if err != nil {
			t.Fatal(err)
		}
		if !allowed[value] {
			t.Errorf("failure_reason producer %q is absent from proto constraints", value)
		}
	}
	for _, pkg := range files {
		for _, file := range pkg.Files {
			ast.Inspect(file, func(node ast.Node) bool {
				switch typed := node.(type) {
				case *ast.AssignStmt:
					for i, left := range typed.Lhs {
						if selector, ok := left.(*ast.SelectorExpr); ok && selector.Sel.Name == "FailureReason" && i < len(typed.Rhs) {
							check(typed.Rhs[i])
						}
					}
				case *ast.KeyValueExpr:
					if key, ok := typed.Key.(*ast.Ident); ok && key.Name == "FailureReason" {
						check(typed.Value)
					}
				}
				return true
			})
		}
	}
}

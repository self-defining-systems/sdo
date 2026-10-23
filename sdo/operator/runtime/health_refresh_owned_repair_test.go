package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// RH3: the broker has already independently verified and committed a repair.
// A no-op health refresh must permit its durable ACK after restart, while an
// unrelated source/policy change or an uncommitted investigation remains stale.
func TestHealthRefreshAcknowledgesOnlyOwnCommittedRepair(t *testing.T) {
	for _, change := range []string{"none", "source", "objective", "policy", "receipt", "processing", "receipt-not-persisted", "ledger-incomplete"} {
		t.Run(change, func(t *testing.T) {
			ctx := context.Background()
			root, _, repository, _, coordinator := tracingFixture(t)
			old := tracingRefresh(t, root, coordinator)
			native := NativeIncidentBroker{Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), ResponderBackend: "test", ResponderModel: "test", Deriver: OutcomeDeriverFunc(func(_ context.Context, c IncidentClosure, _, _, _ string) (DerivedOutcome, error) {
				return testDerivedOutcome(c.Request.IncidentID, "success", true), nil
			}), Reflector: ReflectionWorkerFunc(func(context.Context, IncidentClosure, IncidentWorkspace, *contracts.OutcomeRecord, string, string, ReflectionFeedback) (ReflectionDecision, error) {
				return ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "already learned"}, nil
			})}
			workspace, err := native.PrepareIncident(ctx, "owned-repair")
			if err != nil {
				t.Fatal(err)
			}
			tracingWrite(t, workspace.Worktree, "README.md", "accepted source restoration annotation")
			closure := nativeClosure(workspace)
			receipt, err := native.ProcessClosure(ctx, closure)
			if err != nil {
				t.Fatal(err)
			}
			current := tracingRefresh(t, root, coordinator)
			switch change {
			case "source":
				tracingWrite(t, root, "README.md", "unrelated later source")
			case "objective":
				tracingWrite(t, root, ".sdo/goal.md", "---\nschema_version: 1\nowner: human\n---\n# Health objective\nNew objective.\n")
			case "policy":
				tracingWrite(t, root, ".sdo/diagnostics/detectors/health/health-objective/detector.go", "package objective\n// changed policy\n")
			}
			if change == "source" || change == "objective" || change == "policy" {
				nativeGit(t, root, "add", ".")
				nativeGit(t, root, "commit", "-qm", "unrelated accepted change")
				// The real judge retains identical policy for the source/objective cases.
				// For policy case, its rewrite restores the same bytes; replace a policy
				// input outside the generated detector so adoption still differs.
				if change == "policy" {
					tracingWrite(t, root, ".sdo/lib/changed.txt", "changed policy input")
					nativeGit(t, root, "add", ".")
					nativeGit(t, root, "commit", "-qm", "changed policy library")
				}
				current = tracingRefresh(t, root, coordinator)
			}
			if change == "receipt" {
				receipt.AckToken = "forged"
			}
			now := time.Now().UTC()
			cfg := testControllerConfig()
			cfg.SourceCommit = current.Commit
			c, err := NewController(cfg, []sdk.Detector{controllerDetector("health", time.Second, sdk.Finding{})}, staticProvider{}, &recordingDispatcher{}, now)
			if err != nil {
				t.Fatal(err)
			}
			c.pendingClosure = &closure
			c.closureState = contracts.ClosureState_CLOSURE_STATE_COMMITTED
			c.closureReceipt = &receipt
			if change == "ledger-incomplete" {
				ledger, err := native.readLedger(closure.Request.IncidentID)
				if err != nil {
					t.Fatal(err)
				}
				ledger.Receipt = nil
				if err := native.saveLedger(ledger); err != nil {
					t.Fatal(err)
				}
				c.closureState = contracts.ClosureState_CLOSURE_STATE_PROCESSING
				c.closureReceipt = nil
			}
			if change == "receipt-not-persisted" {
				c.closureState = contracts.ClosureState_CLOSURE_STATE_PROCESSING
				c.closureReceipt = nil
			}
			if change == "processing" {
				c.closureState = contracts.ClosureState_CLOSURE_STATE_PROCESSING
			}
			if err := c.SetIncidentBroker(native); err != nil {
				t.Fatal(err)
			}
			if change == "none" || change == "receipt-not-persisted" {
				state := exportState(t, c)
				restored, err := NewController(cfg, []sdk.Detector{controllerDetector("health", time.Second, sdk.Finding{})}, staticProvider{}, &recordingDispatcher{}, now)
				if err != nil {
					t.Fatal(err)
				}
				if err := restored.RestoreState(state); err != nil {
					t.Fatal(err)
				}
				if err := restored.SetIncidentBroker(native); err != nil {
					t.Fatal(err)
				}
				c = restored
			}
			rv6Guard(c, root, current.Commit)
			if err := c.PersistState(ctx); err != nil {
				t.Fatal(err)
			}
			// The request must remain bound to its original investigation.
			if c.pendingClosure.Request.SourceCommit != old.Commit {
				t.Fatal("fixture lost original evidence")
			}
			err = c.CheckHealth(ctx)
			if change != "none" && change != "receipt-not-persisted" {
				if !errors.Is(err, ErrStaleHealthInvestigation) {
					t.Fatalf("unrelated/stale change allowed: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("own committed repair cannot finish after unchanged policy adoption: %v", err)
			}
			if change == "receipt-not-persisted" {
				replay, ok := c.PendingClosureEffect()
				if !ok {
					t.Fatal("restored closure has no replayable broker effect")
				}
				before := nativeGit(t, root, "rev-list", "--count", "HEAD")
				if err := c.ExecuteClosureEffect(ctx, replay); err != nil {
					t.Fatal(err)
				}
				awaitBrokerResult(t, c.closureResults)
				c.processBrokerCompletions()
				if c.closureReceipt == nil || *c.closureReceipt != receipt {
					t.Fatal("broker replay changed receipt")
				}
				if nativeGit(t, root, "rev-list", "--count", "HEAD") != before {
					t.Fatal("broker replay republished repair or outcome")
				}

				if err := c.PersistState(ctx); err != nil {
					t.Fatal(err)
				}
			}
			effect, ok := c.PendingClosureAcknowledgmentEffect()
			if !ok {
				t.Fatal("missing durable ACK")
			}
			if err := c.ExecuteClosureAcknowledgmentEffect(ctx, effect); err != nil {
				t.Fatal(err)
			}
			awaitBrokerResult(t, c.acknowledgmentResults)
			c.processBrokerCompletions()
		})
	}
}

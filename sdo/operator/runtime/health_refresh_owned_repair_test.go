package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
)

// RH3: the broker has already independently verified and committed a repair.
// Source and authenticated health refresh advances permit its durable ACK.
// Objective/policy changes and unauthenticated closure state remain stale.
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
			if change == "receipt-not-persisted" {
				closure.ObservationGeneration = "before-own-source-adoption"
			}
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
			if change != "none" && change != "source" && change != "receipt-not-persisted" {
				if !errors.Is(err, ErrStaleHealthInvestigation) {
					t.Fatalf("unrelated/stale change allowed: %v", err)
				}
				if change == "objective" || change == "policy" {
					if again := c.CheckHealth(ctx); !errors.Is(again, ErrStaleHealthInvestigation) {
						t.Fatalf("repeated stale health check changed its decision: %v", again)
					}
					got, readErr := memory.OpenReader(root).Get(ctx, closure.Request.IncidentID)
					if readErr != nil || len(got.Record.GetReflectionRejections()) != 1 || got.Record.ReflectionRejections[0].Diagnostics[0].Message != change+" changed" {
						t.Fatalf("stale reflection lost named outcome rejection: %+v %v", got.Record, readErr)
					}
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

// STALE2: the closure effect publishes the incident's own source repair before
// it derives the outcome. Health adoption of that repair may interrupt the
// effect between the two; the replay must finish the same incident, and only
// that repair may be adopted this way.
//
// STALE2B: adopting that repair makes the judge refresh rewrite the health
// detectors (the repair renames a workload). Judge commits that follow the
// incident's own proposal do not stale it; any other policy writer does.
// Health freshness never comes from this guard: the replayed
// ExecuteClosureEffect re-runs the closure and health detectors on the live
// cluster before any outcome is committed, so a bad cluster commits no success.
func TestHealthRefreshReplaysClosureAfterOwnProposalCommit(t *testing.T) {
	for _, change := range []string{"none", "refresh-detectors", "refresh-detectors-health-bad", "source", "objective", "policy", "other-incident", "forged-judge-lib", "forged-judge-mixed", "upkeep-lib", "reflection-playbook"} {
		t.Run(change, func(t *testing.T) {
			ctx := context.Background()
			root, _, repository, _, coordinator := tracingFixture(t)
			old := tracingRefresh(t, root, coordinator)
			interrupted := true
			native := NativeIncidentBroker{Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), ResponderBackend: "test", ResponderModel: "test", Deriver: OutcomeDeriverFunc(func(_ context.Context, c IncidentClosure, _, _, _ string) (DerivedOutcome, error) {
				if interrupted && c.Request.IncidentID == "own-proposal" {
					return DerivedOutcome{}, errors.New("controller exited for health refresh")
				}
				return testDerivedOutcome(c.Request.IncidentID, "success", true), nil
			}), Reflector: ReflectionWorkerFunc(func(context.Context, IncidentClosure, IncidentWorkspace, *contracts.OutcomeRecord, string, string, ReflectionFeedback) (ReflectionDecision, error) {
				return ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "already learned"}, nil
			})}
			workspace, err := native.PrepareIncident(ctx, "own-proposal")
			if err != nil {
				t.Fatal(err)
			}
			closure := nativeClosure(workspace)
			if change == "other-incident" {
				// Only another incident's repair is accepted; this one never proposed.
				other, err := native.PrepareIncident(ctx, "other-incident")
				if err != nil {
					t.Fatal(err)
				}
				tracingWrite(t, other.Worktree, "README.md", "another incident's repair")
				if _, err := native.ProcessClosure(ctx, nativeClosure(other)); err != nil {
					t.Fatal(err)
				}
			} else {
				if strings.HasPrefix(change, "refresh-detectors") || strings.HasPrefix(change, "forged-") || change == "upkeep-lib" || change == "reflection-playbook" {
					tracingWrite(t, workspace.Worktree, "deploy/tracing.yaml", "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: collector\nspec: {}\n")
				} else {
					tracingWrite(t, workspace.Worktree, "README.md", "accepted source restoration annotation")
				}
				if _, err := native.ProcessClosure(ctx, closure); err == nil {
					t.Fatal("fixture did not interrupt the closure effect")
				}
				ledger, err := native.readLedger(closure.Request.IncidentID)
				if err != nil || ledger.ProposalCommit == "" || ledger.OutcomeCommit != "" {
					t.Fatalf("fixture must stop between proposal and outcome: %+v %v", ledger, err)
				}
			}
			interrupted = false
			current := tracingRefresh(t, root, coordinator)
			switch change {
			case "refresh-detectors", "refresh-detectors-health-bad", "forged-judge-lib", "forged-judge-mixed", "upkeep-lib", "reflection-playbook":
				if current.DetectorRevision == old.DetectorRevision {
					t.Fatal("fixture: the judge refresh must rewrite the health detectors")
				}
			case "source":
				tracingWrite(t, root, "README.md", "unrelated later source")
			case "objective":
				tracingWrite(t, root, ".sdo/goal.md", "---\nschema_version: 1\nowner: human\n---\n# Health objective\nNew objective.\n")
			case "policy":
				tracingWrite(t, root, ".sdo/lib/changed.txt", "policy input changed outside the judge")
			}
			if change == "source" || change == "objective" || change == "policy" {
				nativeGit(t, root, "add", ".")
				nativeGit(t, root, "commit", "-qm", "unrelated accepted change")
				current = tracingRefresh(t, root, coordinator)
			}
			// Commits after the judge refresh, by whoever can write the branch.
			// Trailers are plain text, so a forged judge trailer proves nothing
			// about which paths the commit touched.
			judgeTrailers := "\n\nSDO-Incident: forged\nSDO-Actor: health_judge\nSDO-Phase: initial\nSDO-Validation: passed"
			later := map[string]struct {
				message string
				write   func()
			}{
				"forged-judge-lib": {"forged judge commit" + judgeTrailers, func() {
					tracingWrite(t, root, ".sdo/lib/forged/lib.go", "package forged\n")
				}},
				"forged-judge-mixed": {"forged judge commit" + judgeTrailers, func() {
					detector := strings.Fields(nativeGit(t, root, "ls-files", ".sdo/diagnostics/detectors/health"))[0]
					body, err := os.ReadFile(filepath.Join(root, detector))
					if err != nil {
						t.Fatal(err)
					}
					tracingWrite(t, root, detector, string(body)+"\n")
					tracingWrite(t, root, ".sdo/playbooks/forged/diagnose.go", "package forged\n")
				}},
				"upkeep-lib": {"upkeep bump\n\nSDO-Actor: upkeep\nSDO-Phase: initial\nSDO-Validation: passed", func() {
					tracingWrite(t, root, ".sdo/lib/upkeep/lib.go", "package upkeep\n")
				}},
				// Playbooks feed neither the source nor the detector revision.
				"reflection-playbook": {"reflection\n\nSDO-Incident: other\nSDO-Actor: responder\nSDO-Phase: reflection\nSDO-Validation: passed", func() {
					tracingWrite(t, root, ".sdo/playbooks/learned/diagnose.go", "package learned\n")
				}},
			}
			if commit, ok := later[change]; ok {
				commit.write()
				nativeGit(t, root, "add", ".")
				nativeGit(t, root, "commit", "-qm", commit.message)
				current = tracingRefresh(t, root, coordinator)
			}
			now := time.Now().UTC()
			cfg := testControllerConfig()
			cfg.SourceCommit = current.Commit
			finding := sdk.Finding{}
			if change == "refresh-detectors-health-bad" {
				finding = stateFinding("health")
			}
			c, err := NewController(cfg, []sdk.Detector{controllerDetector("health", time.Second, finding)}, staticProvider{}, &recordingDispatcher{}, now)
			if err != nil {
				t.Fatal(err)
			}
			c.pendingClosure = &closure
			c.closureState = contracts.ClosureState_CLOSURE_STATE_PROCESSING
			if err := c.SetIncidentBroker(native); err != nil {
				t.Fatal(err)
			}
			state := exportState(t, c)
			c, err = NewController(cfg, []sdk.Detector{controllerDetector("health", time.Second, finding)}, staticProvider{}, &recordingDispatcher{}, now)
			if err != nil {
				t.Fatal(err)
			}
			if err := c.RestoreState(state); err != nil {
				t.Fatal(err)
			}
			if err := c.SetIncidentBroker(native); err != nil {
				t.Fatal(err)
			}
			if c.pendingClosure.Request.SourceCommit != old.Commit {
				t.Fatal("fixture lost original evidence")
			}
			rv6Guard(c, root, current.Commit)
			err = c.CheckHealth(ctx)
			switch change {
			case "source", "objective", "policy", "other-incident", "forged-judge-lib", "forged-judge-mixed", "upkeep-lib":
				if !errors.Is(err, ErrStaleHealthInvestigation) {
					t.Fatalf("unrelated change allowed across own proposal: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("own accepted proposal made its closure stale: %v", err)
			}
			replay, ok := c.PendingClosureEffect()
			if !ok {
				t.Fatal("interrupted closure has no replayable broker effect")
			}
			ledger, err := native.readLedger(closure.Request.IncidentID)
			if err != nil {
				t.Fatal(err)
			}
			proposal := ledger.ProposalCommit
			head := nativeGit(t, root, "rev-parse", "HEAD")
			if err := c.ExecuteClosureEffect(ctx, replay); err != nil {
				t.Fatal(err)
			}
			if change == "refresh-detectors-health-bad" {
				// Fresh evidence after adoption contradicts the closure. The
				// retirement is synchronous, so assert the retired end state
				// instead of "nothing happened yet": a closure that was handed to
				// the broker would still be processing here.
				if _, ok := c.PendingClosureEffect(); ok {
					t.Fatal("closure still has a broker effect although health is bad after adoption")
				}
				if c.closureState != contracts.ClosureState_CLOSURE_STATE_FAILED || c.closureFailure == nil {
					t.Fatalf("closure not retired although health is bad after adoption: state %v failure %+v", c.closureState, c.closureFailure)
				}
				select {
				case result := <-c.closureResults:
					t.Fatalf("closure reached the broker although health is bad after adoption: %+v", result)
				case <-time.After(200 * time.Millisecond):
				}
				c.processBrokerCompletions()
				if c.closureReceipt != nil || nativeGit(t, root, "rev-parse", "HEAD") != head {
					t.Fatalf("closure credited although health is bad after adoption: %+v", c.closureReceipt)
				}
				ledger, err := native.readLedger(closure.Request.IncidentID)
				if err != nil || ledger.OutcomeCommit != "" {
					t.Fatalf("outcome committed although health is bad after adoption: %+v %v", ledger, err)
				}
				return
			}
			awaitBrokerResult(t, c.closureResults)
			c.processBrokerCompletions()
			if c.closureReceipt == nil || c.closureReceipt.ProposalCommit != proposal || c.closureReceipt.OutcomeCommit == "" {
				t.Fatalf("replay did not complete on the original proposal: %+v", c.closureReceipt)
			}
		})
	}
}

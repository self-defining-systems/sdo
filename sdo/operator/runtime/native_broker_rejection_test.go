package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/validate"
)

type rejectionHarness struct {
	t         *testing.T
	target    string
	native    NativeIncidentBroker
	workspace IncidentWorkspace
	closure   IncidentClosure
	calls     []ReflectionFeedback
}

// newRejectionHarness builds a native broker whose reflection worker is
// scripted by behave(call) where call is the 1-based dispatch count.
func newRejectionHarness(t *testing.T, incident string, behave func(call int, w IncidentWorkspace) (ReflectionDecision, error)) *rejectionHarness {
	t.Helper()
	return newRejectionHarnessValidator(t, incident, behave, func(context.Context, string, []string) error { return nil })
}

func newRejectionHarnessValidator(t *testing.T, incident string, behave func(int, IncidentWorkspace) (ReflectionDecision, error), validator broker.Validator) *rejectionHarness {
	t.Helper()
	target := nativeRepository(t)
	nativeMemoryFixture(t, target)
	repository, err := broker.New(target, filepath.Join(t.TempDir(), "proposals"), validator)
	if err != nil {
		t.Fatal(err)
	}
	h := &rejectionHarness{t: t, target: target}
	h.native = NativeIncidentBroker{
		Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), ResponderBackend: "test", ResponderModel: "test",
		Deriver: OutcomeDeriverFunc(func(_ context.Context, c IncidentClosure, _, _, _ string) (DerivedOutcome, error) {
			return testDerivedOutcome(c.Request.IncidentID, "success", true), nil
		}),
	}
	h.setReflector(behave)
	h.workspace, err = h.native.PrepareIncident(context.Background(), incident)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(h.workspace.Worktree, "app.txt"), []byte("repair\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	h.closure = nativeClosure(h.workspace)
	return h
}

func (h *rejectionHarness) setReflector(behave func(call int, w IncidentWorkspace) (ReflectionDecision, error)) {
	h.native.Reflector = ReflectionWorkerFunc(func(_ context.Context, _ IncidentClosure, w IncidentWorkspace, _ *contracts.OutcomeRecord, _, _ string, feedback ReflectionFeedback) (ReflectionDecision, error) {
		h.calls = append(h.calls, feedback)
		return behave(len(h.calls), w)
	})
}

func writeBadPlaybook(w IncidentWorkspace) (ReflectionDecision, error) {
	return ReflectionDecision{LearningDecision: "updated", PlaybookIDs: []string{"base"}}, os.WriteFile(filepath.Join(w.Worktree, ".sdo", "playbooks", "base", "README.md"), []byte("bad playbook\n"), 0o644)
}

func writeGoodPlaybook(w IncidentWorkspace) (ReflectionDecision, error) {
	body := "---\nschema_version: 1\nowner: responder\nfault_class: base\noriginating_incident: initial\n---\nRepair <RESOURCE_NAME> carefully\n"
	return ReflectionDecision{LearningDecision: "updated", PlaybookIDs: []string{"base"}}, os.WriteFile(filepath.Join(w.Worktree, ".sdo", "playbooks", "base", "README.md"), []byte(body), 0o644)
}

func TestMaxValidationRetriesIsTwo(t *testing.T) {
	if broker.MaxValidationRetries != 2 {
		t.Fatalf("MaxValidationRetries = %d, want 2", broker.MaxValidationRetries)
	}
}

func TestNativeBrokerRedispatchesRejectedReflectionWithTypedDiagnostics(t *testing.T) {
	h := newRejectionHarness(t, "inc-retry", func(call int, w IncidentWorkspace) (ReflectionDecision, error) {
		if call < 3 {
			return writeBadPlaybook(w)
		}
		return writeGoodPlaybook(w)
	})
	receipt, err := h.native.ProcessClosure(context.Background(), h.closure)
	if err != nil {
		t.Fatal(err)
	}
	if len(h.calls) != 3 {
		t.Fatalf("dispatches = %d, want 3", len(h.calls))
	}
	if h.calls[0].Attempt != 1 || len(h.calls[0].Diagnostics) != 0 {
		t.Fatalf("first dispatch carries feedback: %+v", h.calls[0])
	}
	for i, call := range h.calls[1:] {
		if call.Attempt != uint32(i+2) || len(call.Diagnostics) == 0 {
			t.Fatalf("retry %d feedback = %+v", i+1, call)
		}
		for _, d := range call.Diagnostics {
			if d.GetRule() == "" || d.GetMessage() == "" || d.GetPath() == "" {
				t.Fatalf("diagnostic is not typed: %+v", d)
			}
		}
	}
	ledger, err := h.native.readLedger("inc-retry")
	if err != nil {
		t.Fatal(err)
	}
	if len(ledger.ReflectionRejections) != 2 || ledger.ReflectionRejected {
		t.Fatalf("ledger rejections = %v rejected=%v", ledger.ReflectionRejections, ledger.ReflectionRejected)
	}
	for i, rejection := range ledger.ReflectionRejections {
		if rejection.GetAttempt() != uint32(i+1) || len(rejection.GetDiagnostics()) == 0 || rejection.GetRejectedAt() == nil {
			t.Fatalf("rejection %d incomplete: %+v", i, rejection)
		}
	}
	if receipt.ReflectionCommit == "" {
		t.Fatal("accepted retry has no reflection commit")
	}
	if got := nativeGit(t, h.target, "show", "HEAD:.sdo/playbooks/base/README.md"); strings.Contains(got, "bad playbook") {
		t.Fatal("rejected reflection reached accepted state")
	}
}

func TestNativeBrokerRecordsTerminalRejectionAfterRetryBound(t *testing.T) {
	h := newRejectionHarness(t, "inc-bound", func(_ int, w IncidentWorkspace) (ReflectionDecision, error) {
		return writeBadPlaybook(w)
	})
	receipt, err := h.native.ProcessClosure(context.Background(), h.closure)
	if err != nil {
		t.Fatalf("terminal rejection must complete the closure: %v", err)
	}
	if want := 1 + broker.MaxValidationRetries; len(h.calls) != want {
		t.Fatalf("dispatches = %d, want %d", len(h.calls), want)
	}
	if receipt.ReflectionCommit != "" || receipt.OutcomeCommit == "" {
		t.Fatalf("receipt = %+v", receipt)
	}
	ledger, err := h.native.readLedger("inc-bound")
	if err != nil {
		t.Fatal(err)
	}
	if !ledger.ReflectionRejected || !ledger.ReflectionProcessed || len(ledger.ReflectionRejections) != 3 {
		t.Fatalf("terminal state not recorded: %+v", ledger)
	}
	for _, rejection := range ledger.ReflectionRejections {
		if len(rejection.GetDiagnostics()) == 0 {
			t.Fatalf("rejection without diagnostics: %+v", rejection)
		}
	}
	if _, err := h.native.Repository.ProposalInfo("inc-bound", broker.ReflectionPhase); err == nil {
		t.Fatal("rejected reflection checkout remained")
	}
	// A further call returns the stored receipt without dispatching again.
	again, err := h.native.ProcessClosure(context.Background(), h.closure)
	if err != nil || again != receipt || len(h.calls) != 3 {
		t.Fatalf("terminal rejection reopened: %+v %v calls=%d", again, err, len(h.calls))
	}
}

func TestNativeBrokerRestartKeepsRejectionAttempts(t *testing.T) {
	h := newRejectionHarness(t, "inc-restart", func(call int, w IncidentWorkspace) (ReflectionDecision, error) {
		if call == 2 {
			return ReflectionDecision{}, errors.New("worker pod evicted")
		}
		return writeBadPlaybook(w)
	})
	if _, err := h.native.ProcessClosure(context.Background(), h.closure); err == nil {
		t.Fatal("worker failure was swallowed")
	}
	// The controller restarts: a new broker value reads the same ledger.
	restarted := h.native
	h.native = restarted
	h.setReflector(func(_ int, w IncidentWorkspace) (ReflectionDecision, error) { return writeBadPlaybook(w) })
	receipt, err := h.native.ProcessClosure(context.Background(), h.closure)
	if err != nil {
		t.Fatal(err)
	}
	// Dispatch 2 died with its worker before the restart. A worker failure is
	// not a validation rejection, so the restart repeats attempt 2 with attempt
	// 1's diagnostics instead of starting over or skipping ahead.
	if h.calls[1].Attempt != 2 || len(h.calls[1].Diagnostics) == 0 || h.calls[2].Attempt != 2 || len(h.calls[2].Diagnostics) == 0 {
		t.Fatalf("restart reset the attempt: %+v", h.calls)
	}
	if h.calls[3].Attempt != 3 {
		t.Fatalf("attempts after restart = %+v", h.calls)
	}
	if len(h.calls) != 4 {
		t.Fatalf("total dispatches = %d, want 4 (bound spans the restart)", len(h.calls))
	}
	ledger, err := h.native.readLedger("inc-restart")
	if err != nil {
		t.Fatal(err)
	}
	if len(ledger.ReflectionRejections) != 3 || !ledger.ReflectionRejected || receipt.ReflectionCommit != "" {
		t.Fatalf("ledger = %+v receipt = %+v", ledger, receipt)
	}
	for i, rejection := range ledger.ReflectionRejections {
		if rejection.GetAttempt() != uint32(i+1) {
			t.Fatalf("attempt numbers duplicated or reset: %+v", ledger.ReflectionRejections)
		}
	}
}

func TestNativeBrokerRejectionRecordedBeforeProposalReset(t *testing.T) {
	// A crash after the rejection is durable but before the dirty checkout is
	// reset must not record the same rejection twice.
	h := newRejectionHarness(t, "inc-crash", func(_ int, w IncidentWorkspace) (ReflectionDecision, error) {
		return writeBadPlaybook(w)
	})
	if _, err := h.native.ProcessClosure(context.Background(), h.closure); err != nil {
		t.Fatal(err)
	}
	ledger, err := h.native.readLedger("inc-crash")
	if err != nil {
		t.Fatal(err)
	}
	// Rewind to the explicit reset checkpoint after attempt 1's rejection.
	// Recovery must reset the rejected tree before dispatching attempt 2.
	ledger.ReflectionRejections = ledger.ReflectionRejections[:1]
	ledger.ReflectionResults = ledger.ReflectionResults[:1]
	ledger.ReflectionPhase = contracts.ReflectionPhase_REFLECTION_PHASE_RESET
	ledger.ReflectionRejected, ledger.ReflectionProcessed, ledger.ReflectionDecision, ledger.Receipt = false, false, nil, nil
	if err := h.native.saveLedger(ledger); err != nil {
		t.Fatal(err)
	}
	proposal, err := h.native.Repository.CreateProposalForPhase(context.Background(), "inc-crash", broker.ReflectionPhase)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writeBadPlaybook(IncidentWorkspace{Worktree: proposal.Path}); err != nil {
		t.Fatal(err)
	}
	h.calls = nil
	if _, err := h.native.ProcessClosure(context.Background(), h.closure); err != nil {
		t.Fatalf("reset checkpoint did not recover: %v", err)
	}
	ledger, err = h.native.readLedger("inc-crash")
	if err != nil {
		t.Fatal(err)
	}
	if len(ledger.ReflectionRejections) != 3 {
		t.Fatalf("recovery lost the retry budget: %v", ledger.ReflectionRejections)
	}
	for i, rejection := range ledger.ReflectionRejections {
		if rejection.Attempt != uint32(i+1) {
			t.Fatalf("rejection duplicated by recovery: %v", ledger.ReflectionRejections)
		}
	}
	if _, err := h.native.ProcessClosure(context.Background(), h.closure); err != nil {
		t.Fatal(err)
	}
	if len(h.calls) == 0 || h.calls[0].Attempt != 2 {
		t.Fatalf("recovery dispatch = %+v", h.calls)
	}
}

func TestNativeBrokerBindsDurableReflectionIDsToAcceptedDiff(t *testing.T) {
	h := newRejectionHarness(t, "inc-result-ids", func(_ int, w IncidentWorkspace) (ReflectionDecision, error) {
		decision, err := writeGoodPlaybook(w)
		decision.PlaybookIDs = []string{"unrelated"}
		return decision, err
	})
	receipt, err := h.native.ProcessClosure(context.Background(), h.closure)
	if err != nil || receipt.ReflectionCommit != "" {
		t.Fatalf("mismatched decision: receipt=%+v err=%v", receipt, err)
	}
	ledger, err := h.native.readLedger("inc-result-ids")
	if err != nil {
		t.Fatal(err)
	}
	if len(ledger.ReflectionRejections) != 3 || !ledger.ReflectionRejected {
		t.Fatalf("decision rejection bypassed retry accounting: %+v", ledger)
	}
	for i, rejection := range ledger.ReflectionRejections {
		if rejection.Attempt != uint32(i+1) || rejection.Diagnostics[0].Rule != "reflection.decision" {
			t.Fatalf("lost decision feedback: %+v", rejection)
		}
	}
	if _, accepted, err := h.native.Repository.AcceptedCommit(context.Background(), "inc-result-ids", broker.ReflectionPhase); err != nil || accepted {
		t.Fatalf("mismatched reflection reached accepted state: accepted=%v error=%v", accepted, err)
	}
}

func TestNativeBrokerCompileRejectionAdvancesAttempt(t *testing.T) {
	checks := 0
	validator := func(_ context.Context, _ string, paths []string) error {
		for _, p := range paths {
			if strings.HasPrefix(p, ".sdo/playbooks/") {
				checks++
				if checks == 1 {
					ds, err := validate.ParseReport(`SDO_MEMORY_REPORT:{"diagnostics":[{"rule":"detector.compile","message":"undefined: missing","path":".sdo/diagnostics/detectors/incidents/cache/detector.go","line":4}]}`)
					if err != nil {
						return err
					}
					return validate.Err(ds)
				}
			}
		}
		return nil
	}
	h := newRejectionHarnessValidator(t, "inc-compile-retry", func(_ int, w IncidentWorkspace) (ReflectionDecision, error) { return writeGoodPlaybook(w) }, validator)
	receipt, err := h.native.ProcessClosure(context.Background(), h.closure)
	if err != nil || receipt.ReflectionCommit == "" {
		t.Fatalf("compile retry failed: %+v %v", receipt, err)
	}
	if len(h.calls) != 2 || h.calls[0].Attempt != 1 || h.calls[1].Attempt != 2 || len(h.calls[1].Diagnostics) != 1 || h.calls[1].Diagnostics[0].Rule != validate.RuleDetectorCompile {
		t.Fatalf("compile failure lost attempt/typed feedback: %+v", h.calls)
	}
	ledger, err := h.native.readLedger("inc-compile-retry")
	if err != nil {
		t.Fatal(err)
	}
	if len(ledger.ReflectionRejections) != 1 || ledger.ReflectionRejections[0].Attempt != 1 {
		t.Fatalf("compile rejection not durable: %+v", ledger.ReflectionRejections)
	}
}

//go:build integration

package runtime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/broker"
	"sdo.dev/operator/internal/capturefixture"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/validate"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/program"
)

// A prior accepted repair with identical receipts cannot bypass current fixed
// classification. Its earlier bound NoMatch cannot override a final Match,
// even when every reflection edit is rejected.
func TestFalseFixedMatchIsDurableBeforeReflectionRejectionAndNeverCredited(t *testing.T) {
	ctx := context.Background()
	target := nativeRepository(t)
	fixture := "../../../tests/fixtures/sdo/playbook_program_app/.sdo"
	copyFastloopTree(t, "../memory/validate/testdata/good/.sdo", filepath.Join(target, ".sdo"))
	module, err := os.ReadFile(filepath.Join(fixture, "diagnostics/go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, ".sdo/diagnostics/go.mod"), module, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(target, ".sdo/playbooks/base/scripts/fix.sh"), 0755); err != nil {
		t.Fatal(err)
	}
	testutil.CopyPortProgram(t, target, fixture)
	copyFastloopTree(t, filepath.Join(fixture, "corpus"), filepath.Join(target, ".sdo/corpus"))
	index := filepath.Join(target, ".sdo/playbooks/README.md")
	raw, _ := os.ReadFile(index)
	if err := os.WriteFile(index, append(raw, []byte("\n- [service-targetport-mismatch](service-targetport-mismatch/README.md)\n")...), 0644); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, target, "add", ".")
	nativeGit(t, target, "commit", "-qm", "accepted program")
	base := nativeGit(t, target, "rev-parse", "HEAD")
	revision, err := programDependencyRevision(ctx, target, base, repeatProgram)
	if err != nil {
		t.Fatal(err)
	}
	o := verifiedRepeat(t)
	o.SchemaVersion = 1
	o.SourceCommit = base
	o.IncidentId = "false-fixed-match"
	o.RepairActions[0].Resources = []*contracts.ObjectRef{repeatService}
	o.Findings = []*contracts.Finding{{DetectorId: "service-port", RuleId: "targetport", Status: "active", Severity: "warn", Summary: "port mismatch", Evidence: "controller observed target port mismatch", Fingerprint: "reservation-port", PrimaryResource: repeatService}}
	o.DetectorHistory = []*contracts.DetectorEvaluation{{DetectorId: "service-port", Status: "clear", EvaluatedAt: timestamppb.New(time.Unix(1_700_000_101, 0))}}
	for _, run := range o.ProgramRuns {
		run.ProgramRevision = revision
	}
	for _, call := range o.ProgramToolCalls {
		call.ResponseJson = strings.ReplaceAll(call.ResponseJson, repeatRevision, revision)
	}
	checks := 0
	repository, err := broker.New(target, filepath.Join(t.TempDir(), "proposals"), func(context.Context, string, []string) error {
		checks++
		return validate.Err([]validate.Diagnostic{{Rule: "corpus.replay", Message: "fixed capture still matches"}})
	})
	if err != nil {
		t.Fatal(err)
	}
	prior := proto.Clone(o).(*contracts.OutcomeRecord)
	prior.IncidentId = "previous-identical-repair"
	if _, err := repository.RecordOutcome(ctx, prior.IncidentId, prior); err != nil {
		t.Fatal(err)
	}
	session := t.TempDir()
	capture, err := memory.IncidentCorpusDir(session, o.IncidentId)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(capture, 0755); err != nil {
		t.Fatal(err)
	}
	empty := capturefixture.FromView(t, `{"namespace":"default"}`)
	for name, body := range map[string][]byte{"fixed.json": empty, "open@detect.json": empty, "open@detect-recordings.json": []byte("[]")} {
		if err := os.WriteFile(filepath.Join(capture, name), body, 0600); err != nil {
			t.Fatal(err)
		}
	}
	registry, err := program.NewRegistry([]program.Diagnoser{candidateProgram{spec: fastloopPortProgram{}.Spec(), run: func(context.Context, program.Bindings) program.Verdict { return program.Verdict{Result: program.Match} }}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	native := NativeIncidentBroker{Repository: repository, LedgerRoot: t.TempDir(), SessionRoot: session, Programs: registry, ResponderBackend: "test", ResponderModel: "test", Deriver: OutcomeDeriverFunc(func(context.Context, IncidentClosure, string, string, string) (DerivedOutcome, error) {
		return DerivedOutcome{Record: o, RequiresReflection: true}, nil
	}), Reflector: ReflectionWorkerFunc(func(_ context.Context, _ IncidentClosure, _ IncidentWorkspace, outcome *contracts.OutcomeRecord, _ string, _ string, feedback ReflectionFeedback) (ReflectionDecision, error) {
		calls++
		if outcome.GetReflectionEligibility().GetCauses()[0].GetItems()[0].GetDisposition() != "false_match" {
			t.Fatal("worker lost authoritative false match")
		}
		if len(feedback.Diagnostics) == 0 {
			t.Fatal("model ran before deterministic replay rejected")
		}
		if state := ProgramTrustStats([]*contracts.OutcomeRecord{outcome})[repeatProgram]; state.VerifiedSuccesses != 0 || (state.VerifiedSuccesses > 0 && state.Failures == 0) {
			t.Fatalf("false match credited before rejection: %+v", state)
		}
		return ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "cannot safely refine"}, nil
	})}
	workspace, err := native.PrepareIncident(ctx, o.IncidentId)
	if err != nil {
		t.Fatal(err)
	}
	closure := nativeClosure(workspace)
	closure.Request.RepairPolicy = "recorded-actions"
	if _, err := native.ProcessClosure(ctx, closure); err != nil {
		t.Fatal(err)
	}
	stored, err := memory.NewJSONStore(target).Get(ctx, o.IncidentId)
	if err != nil {
		t.Fatal(err)
	}
	item := stored.Record.GetReflectionEligibility().GetCauses()[0].GetItems()[0]
	if item.GetDisposition() != "false_match" || item.GetProgramRevision() != revision || checks != 1 || calls != broker.MaxValidationRetries {
		t.Fatalf("classification bypassed: %v checks=%d calls=%d", item, checks, calls)
	}
	ledger, err := native.readLedger(o.IncidentId)
	if err != nil || !ledger.GetReflectionRejected() {
		t.Fatalf("rejection missing: %v %v", ledger, err)
	}
	// The seeded regression case is a real change, so the replay check runs
	// once and rejects; a no-change decision over that diff is malformed after.
	if len(ledger.ReflectionRejections) == 0 || ledger.ReflectionRejections[0].Diagnostics[0].Rule != "corpus.replay" {
		t.Fatalf("missing replay rejection: %v", ledger.ReflectionRejections)
	}
	if state := ProgramTrustStats([]*contracts.OutcomeRecord{stored.Record})[repeatProgram]; state.VerifiedSuccesses != 0 || (state.VerifiedSuccesses > 0 && state.Failures == 0) {
		t.Fatalf("rejected reflection left credit: %+v", state)
	}
}

func TestSettlementUsesMatchedRevisionInsteadOfALaterRun(t *testing.T) {
	ctx := context.Background()
	root := nativeRepository(t)
	path := filepath.Join(root, ".sdo/playbooks", repeatProgram, "diagnose.go")
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("package accepted\n"), 0644); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, root, "add", ".")
	nativeGit(t, root, "commit", "-qm", "accepted revision")
	base := nativeGit(t, root, "rev-parse", "HEAD")
	revision, err := programDependencyRevision(ctx, root, base, repeatProgram)
	if err != nil {
		t.Fatal(err)
	}
	o := verifiedRepeat(t)
	o.IncidentId = "changed-program"
	o.RepairActions[0].Resources = []*contracts.ObjectRef{repeatService}
	o.Findings = []*contracts.Finding{{DetectorId: "service-port", RuleId: "targetport", Severity: "warn", Summary: "wrong target port", Evidence: "synthetic controller-observed target port mismatch", Status: "active", PrimaryResource: repeatService}}
	o.FinalHealthDetectorState = []*contracts.DetectorEvaluation{{DetectorId: "service-port", Status: "clear", EvaluatedAt: timestamppb.New(time.Unix(1_700_000_101, 0))}}
	o.ProgramRuns[0].ProgramRevision = "previous-program-revision"
	o.ProgramRuns[1].ProgramId = repeatProgram
	o.ProgramRuns[1].ProgramRevision = revision
	session := t.TempDir()
	capture, err := memory.IncidentCorpusDir(session, o.IncidentId)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(capture, 0755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string][]byte{"fixed.json": capturefixture.Raw(t, "empty fixed view: only recordings answer", []byte("{}")), "open@detect-recordings.json": []byte("[]")} {
		if err := os.WriteFile(filepath.Join(capture, name), body, 0644); err != nil {
			t.Fatal(err)
		}
	}
	registry, err := program.NewRegistry([]program.Diagnoser{candidateProgram{spec: fastloopPortProgram{}.Spec(), run: func(context.Context, program.Bindings) program.Verdict { return program.Verdict{Result: program.Match} }}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	native := NativeIncidentBroker{Programs: registry, SessionRoot: session}
	eligible := native.prepareCauseWork(ctx, o, IncidentWorkspace{IncidentID: o.IncidentId, Worktree: root, BaseCommit: base})
	item := eligible.GetCauses()[0].GetItems()[0]
	if item.GetDisposition() != "question" || item.GetProgramRevision() != "previous-program-revision" {
		t.Fatalf("later run replaced the matched revision: %v", item)
	}
}

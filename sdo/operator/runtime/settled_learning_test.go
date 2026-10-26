package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func TestAllUnboundWritesSurviveGroupingAndDuplicateReceipts(t *testing.T) {
	a := &contracts.ObjectRef{Kind: "ConfigMap", Namespace: "app", Name: "a"}
	b := &contracts.ObjectRef{Kind: "ConfigMap", Namespace: "app", Name: "b"}
	c := &contracts.ObjectRef{Kind: "ConfigMap", Namespace: "app", Name: "c"}
	d := &contracts.ObjectRef{Kind: "ConfigMap", Namespace: "app", Name: "d"}
	o := &contracts.OutcomeRecord{Classification: "success", ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Resources: []*contracts.ObjectRef{a, b, a}}}, RepairActions: []*contracts.RepairActionReceipt{{Success: true, Resources: []*contracts.ObjectRef{a, b, c}}, {Success: true, Resources: []*contracts.ObjectRef{b, c, d}}}}
	grouped := classifyCauseWork(o, nil)
	if len(grouped) != 1 || len(grouped[0].Items) != 2 {
		t.Fatalf("lost grouped writes: %v", grouped)
	}
	unassigned := unassignedReflectionQuestions(o, nil)
	if len(unassigned) != 2 || !proto.Equal(unassigned[0].Resources[0], c) || !proto.Equal(unassigned[1].Resources[0], d) {
		t.Fatalf("lost unassigned writes: %v", unassigned)
	}
}

type sharedProbeProgram struct{ id string }

func (p sharedProbeProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: p.id, Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}
}
func (p sharedProbeProgram) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	_, err := v.ProbeTCP(b.Objects["service"], 80)
	if err != nil {
		return program.Verdict{Result: program.Unknown}, err
	}
	return program.Verdict{Result: program.Match}, nil
}
func TestIndependentFixedDiagnosesShareImmutableRecordingWithoutConsumption(t *testing.T) {
	capture := t.TempDir()
	ref := sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}
	args, _ := json.Marshal([]any{ref, 80})
	calls, _ := json.Marshal([]sdktest.PrimitiveCall{{Phase: "fixed", Method: "ProbeTCP", Arguments: args, Result: json.RawMessage(`{}`)}})
	// A pre-apply-only observation must be available to the same classification.
	if err := os.WriteFile(filepath.Join(capture, "open@pre-apply-recordings.json"), calls, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(capture, "fixed.json"), capturefixture.Raw(t, "empty fixed view: only recordings answer", []byte(`{}`)), 0600); err != nil {
		t.Fatal(err)
	}
	registry, err := program.NewRegistry([]program.Diagnoser{sharedProbeProgram{"probe-a"}, sharedProbeProgram{"probe-b"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	o := &contracts.OutcomeRecord{}
	for _, id := range []string{"probe-a", "probe-b"} {
		o.ProgramRuns = append(o.ProgramRuns, programRunContract(ProgramRun{ProgramID: id, Bindings: program.Bindings{Objects: program.ObjectBindings{"service": ref}}, Verdict: program.Verdict{Result: program.Match}}))
	}
	for range 2 {
		verdicts := fixedCaptureVerdicts(context.Background(), o, registry, capture)
		for _, run := range o.ProgramRuns {
			if verdicts[matchedViewKey(run.ProgramId, program.Bindings{Objects: program.ObjectBindings{"service": ref}})] != program.Match {
				t.Fatalf("shared read consumed: %v", verdicts)
			}
		}
		slices.Reverse(o.ProgramRuns)
	}
}

type failingFinalSnapshot struct{}

func (failingFinalSnapshot) Snapshot(context.Context) (sdk.DetectionContext, error) {
	return nil, errors.New("final snapshot unavailable")
}
func TestUnavailableCompletionSnapshotClearsExistingFinalProof(t *testing.T) {
	loop, _ := trustedPortLoop(t, false, false, true)
	loop.applicationsLog = []RecordedPlanApplication{{Result: PlanApplicationResult{Applied: true, FinalEquivalenceVerified: proto.Bool(true)}}}
	loop.Provider = failingFinalSnapshot{}
	loop.CreditEquivalentRepairs(context.Background(), []RepairActionReceipt{{Success: true}})
	if loop.PlanApplications()[0].Result.FinalEquivalenceVerified != nil {
		t.Fatal("snapshot failure retained an affirmative proof")
	}
}

type countingCompletionClient struct {
	PlanObjectClient
	reads map[sdk.ObjectRef]int
}

func (c countingCompletionClient) Get(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	c.reads[ref]++
	return c.PlanObjectClient.Get(ctx, ref)
}
func TestCompletionReadsEachFinalObjectOnceAcrossApplications(t *testing.T) {
	loop, _ := trustedPortLoop(t, false, false, true)
	run := loop.ProgramRuns()[0]
	planned := loop.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: run.ProgramID, Bindings: run.Bindings})
	applied := loop.Handle(context.Background(), ToolRequest{Command: "apply", PlanIDs: []string{planned.PlanID}})
	if applied.Error != "" {
		t.Fatal(applied.Error)
	}
	// Repeated historical applications share a final object and its read error.
	app := loop.applicationsLog[0]
	loop.applicationsLog = append(loop.applicationsLog, app, app)
	counted := countingCompletionClient{PlanObjectClient: loop.Executor.Client, reads: map[sdk.ObjectRef]int{}}
	loop.Executor.Client = counted
	loop.CreditEquivalentRepairs(context.Background(), []RepairActionReceipt{{Success: true, Resources: []sdk.ObjectRef{run.Bindings.Objects["service"]}}})
	if len(counted.reads) != 1 {
		t.Fatalf("final objects: %v", counted.reads)
	}
	for _, n := range counted.reads {
		if n != 1 {
			t.Fatalf("repeated final GETs: %v", counted.reads)
		}
	}
}

func TestHistoricalOutcomeWithoutSettlementCannotCreateSuccessCredit(t *testing.T) {
	o := verifiedTrustOutcome(t, "targetport", "revision")
	o.ReflectionEligibility = nil
	if got := ProgramTrustStats([]*contracts.OutcomeRecord{o})["targetport"]; got.VerifiedSuccesses != 0 || (got.VerifiedSuccesses > 0 && got.Failures == 0) {
		t.Fatalf("historical compatibility created proof: %+v", got)
	}
}

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
	if item.GetDisposition() != "false_match" || item.GetProgramRevision() != revision || checks != 0 || calls != broker.MaxValidationRetries {
		t.Fatalf("classification bypassed: %v checks=%d calls=%d", item, checks, calls)
	}
	ledger, err := native.readLedger(o.IncidentId)
	if err != nil || !ledger.GetReflectionRejected() {
		t.Fatalf("rejection missing: %v %v", ledger, err)
	}
	if len(ledger.ReflectionRejections) == 0 || ledger.ReflectionRejections[0].Diagnostics[0].Rule != "reflection.program-required" {
		t.Fatalf("missing mandatory-learning rejection: %v", ledger.ReflectionRejections)
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

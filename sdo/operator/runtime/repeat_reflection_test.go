package runtime

import (
	"context"
	"encoding/json"
	"google.golang.org/protobuf/types/known/timestamppb"
	"os"
	"path/filepath"

	"strings"
	"testing"
	"time"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/validate"

	"sdo.dev/operator/sdk/program"
)

func TestCompositeRepairKeepsBoundSuccessWhileOtherFaultIsOpen(t *testing.T) {
	loop, _ := trustedPortLoop(t, true, false, false)
	loop.Executor.Verifier = PlanHealthVerifier{Detectors: loop.Detectors, Provider: loop.Provider}
	ctx := context.Background()
	bindings := program.Bindings{Objects: program.ObjectBindings{"service": loop.Request.Findings[0].PrimaryResource}}
	planned := loop.Handle(ctx, ToolRequest{Command: "plan", ProgramID: "service-targetport-mismatch", Bindings: bindings, FindingFingerprints: []string{FindingFingerprint(loop.Request.Findings[0])}})
	applied := loop.Handle(ctx, ToolRequest{Command: "apply", PlanIDs: []string{planned.PlanID}})
	if applied.Error != "" || applied.Healthy {
		t.Fatalf("expected other fault to remain: %+v", applied)
	}
	if !loop.PlanApplications()[0].Result.VerificationPassed {
		t.Fatal("unrelated fault invalidated bound repair")
	}
	for _, signal := range loop.FailureSignals() {
		if strings.HasPrefix(signal, "application_verification_failure:") || strings.HasPrefix(signal, "diagnosis_clear_detectors_firing:") {
			t.Fatalf("unrelated fault poisoned program trust: %s", signal)
		}
	}
}

func TestHermeticRepeatCorpusUsesBrokerWithoutReflectionModel(t *testing.T) {
	testHermeticRepeatBroker(t, false, false)
}

func TestRejectedRepeatCorpusFallsBackToReflectionWorker(t *testing.T) {
	testHermeticRepeatBroker(t, true, false)
}

func TestReflectionSettledRepeatStillResumesResponder(t *testing.T) {
	testHermeticRepeatBroker(t, false, true)
}

func testHermeticRepeatBroker(t *testing.T, rejectFirst, boundSession bool) {
	t.Helper()
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
	fastloopWriteDetector(t, target, "accepted-program")
	copyFastloopTree(t, filepath.Join(fixture, "corpus"), filepath.Join(target, ".sdo/corpus"))
	index := filepath.Join(target, ".sdo/playbooks/README.md")
	prior, _ := os.ReadFile(index)
	if err := os.WriteFile(index, append(prior, []byte("\n- [service-targetport-mismatch](service-targetport-mismatch/README.md)\n")...), 0644); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, target, "add", ".")
	nativeGit(t, target, "commit", "-qm", "accepted program")
	operatorRoot, _ := filepath.Abs("..")
	validations := 0
	rejected := false
	repository, err := broker.New(target, filepath.Join(t.TempDir(), "proposals"), func(ctx context.Context, root string, paths []string) error {
		validations++

		for _, path := range paths {
			if rejectFirst && !rejected && strings.HasPrefix(path, ".sdo/corpus/repeat-") {
				rejected = true
				return validate.Err([]validate.Diagnostic{{Path: path, Rule: "corpus.replay", Message: "repeat regression requires worker review"}})
			}
		}
		diagnostics, err := validate.CompileCandidate(ctx, validate.Request{Root: root, OperatorRoot: operatorRoot})
		if err != nil {
			return err
		}
		return validate.Err(diagnostics)
	})
	if err != nil {
		t.Fatal(err)
	}
	outcome := verifiedRepeat(t)
	outcome.IncidentId = "repeat-evidence"
	outcome.SchemaVersion = 1
	outcome.SourceCommit = nativeGit(t, target, "rev-parse", "HEAD")
	revision, err := programDependencyRevision(ctx, target, outcome.SourceCommit, repeatProgram)
	if err != nil {
		t.Fatal(err)
	}
	ref := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Service", Namespace: "demo", Name: "front"}
	bound := program.Bindings{Objects: program.ObjectBindings{"service": objectRefState(ref)}}
	outcome.RepairActions[0].Resources = []*contracts.ObjectRef{ref}
	outcome.Findings = []*contracts.Finding{{DetectorId: "service-port", RuleId: "targetport", Severity: "warn", Summary: "wrong target port", Evidence: "synthetic controller-observed target port mismatch", Status: "active", PrimaryResource: ref}}
	outcome.FinalHealthDetectorState = []*contracts.DetectorEvaluation{{DetectorId: "service-port", Status: "clear", EvaluatedAt: timestamppb.New(time.Unix(1_700_000_101, 0))}}
	outcome.ProgramRuns[0].Bindings[0].Object = ref
	for _, run := range outcome.ProgramRuns {
		run.ProgramRevision = revision
	}
	outcome.ConfirmedRootCauses[0].Resources = []*contracts.ObjectRef{ref}
	for _, call := range outcome.ProgramToolCalls {
		request, response, _ := parseProgramToolCall(call)
		if request.Command == "plan" {
			request.Bindings = bound
		}
		for index := range response.ProgramRuns {
			response.ProgramRuns[index].Bindings = bound
			response.ProgramRuns[index].ProgramRevision = revision
		}
		data, _ := json.Marshal(request)
		call.RequestJson = string(data)
		data, _ = json.Marshal(response)
		call.ResponseJson = string(data)
	}
	session := t.TempDir()
	capture, err := memory.IncidentCorpusDir(session, outcome.IncidentId)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(capture, 0755); err != nil {
		t.Fatal(err)
	}
	for destination, source := range map[string]string{"open@detect.json": "open.json", "fixed.json": "fixed.json", "open@detect-recordings.json": "recordings.json"} {
		data, err := os.ReadFile(filepath.Join(fixture, "corpus/targetport-mismatch", source))
		if err != nil {
			t.Fatal(err)
		}
		if destination == "fixed.json" {
			var fixed, healthy map[string]json.RawMessage
			if err := json.Unmarshal(data, &fixed); err != nil {
				t.Fatal(err)
			}
			raw, _ := os.ReadFile(filepath.Join(fixture, "corpus/targetport-mismatch/healthy.json"))
			if err := json.Unmarshal(raw, &healthy); err != nil {
				t.Fatal(err)
			}
			for key, raw := range healthy {
				var extra, original []json.RawMessage
				if json.Unmarshal(raw, &extra) == nil {
					_ = json.Unmarshal(fixed[key], &original)
					fixed[key], _ = json.Marshal(append(original, extra...))
				}
			}
			data, _ = json.Marshal(fixed)
		}
		if err := os.WriteFile(filepath.Join(capture, destination), data, 0644); err != nil {
			t.Fatal(err)
		}
	}
	modelCalls := 0
	registry, err := program.NewRegistry([]program.Diagnoser{fastloopPortProgram{}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	native := NativeIncidentBroker{Programs: registry, Repository: repository, LedgerRoot: t.TempDir(), SessionRoot: session,
		Reflector: ReflectionWorkerFunc(func(context.Context, IncidentClosure, IncidentWorkspace, *contracts.OutcomeRecord, string, string, ReflectionFeedback) (ReflectionDecision, error) {
			modelCalls++
			return ReflectionDecision{LearningDecision: "updated"}, nil
		}),
	}
	outcome.ReflectionEligibility = native.prepareCauseWork(ctx, outcome, IncidentWorkspace{IncidentID: outcome.IncidentId, Worktree: target, BaseCommit: outcome.SourceCommit})
	accepted, err := repository.RecordOutcome(ctx, outcome.IncidentId, outcome)
	if err != nil {
		t.Fatal(err)
	}
	closure, err := incidentClosureContract(nativeClosure(IncidentWorkspace{IncidentID: outcome.IncidentId, Worktree: target, BaseCommit: outcome.SourceCommit}))
	if err != nil {
		t.Fatal(err)
	}
	if boundSession {
		sessionID := "settled-responder"
		closure.Result.ResponderSessionId = &sessionID
	}
	ledger := &contracts.BrokerLedger{IncidentId: outcome.IncidentId, Closure: closure, OutcomeCommit: accepted.Commit, OutcomeDocument: outcome}
	paths, err := native.processReflection(ctx, ledger)
	if err != nil {
		t.Fatal(err)
	}
	expectedCalls := 0
	if rejectFirst || boundSession {
		expectedCalls = 1
	}
	if rejected != rejectFirst || modelCalls != expectedCalls || ledger.ReflectionCommit == "" || len(paths) != 5 || validations == 0 {
		t.Fatalf("model=%d paths=%v ledger=%v checks=%d", modelCalls, paths, ledger, validations)
	}
	// The protected open/fixed snapshots already contain the bound Service.
	// Primitive calls are not required for an object to count as observed.
	if ledger.ReflectionEvidenceGapCount == nil || ledger.GetReflectionEvidenceGapCount() != 0 {
		t.Fatalf("incident mislabeled captured open/fixed objects as missing observations: %v", ledger)
	}
	for _, path := range paths {
		if !strings.HasPrefix(path, ".sdo/corpus/repeat-") {
			t.Fatalf("unexpected repeat edit: %s", path)
		}
	}
	if state := ProgramTrustStats([]*contracts.OutcomeRecord{outcome})[repeatProgram]; !(state.VerifiedSuccesses > 0 && state.Failures == 0) {
		t.Fatalf("verified repeat earned no trust: %+v", state)
	}
	again, err := native.processReflection(ctx, ledger)
	if err != nil || len(again) != 5 || modelCalls != expectedCalls {
		t.Fatalf("restart duplicated work: %v %v calls=%d", again, err, modelCalls)
	}
}

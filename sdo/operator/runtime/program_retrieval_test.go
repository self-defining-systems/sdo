package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func unknownVerdict(reason string) program.Verdict {
	return program.Verdict{Result: program.Unknown, Evidence: []program.Evidence{{Fact: "diagnosis", Observed: reason, Expected: "successful diagnosis"}}}
}

// Every retrieval path a producer can emit is a value the ProgramRun contract accepts.
func TestEveryRetrievalPathIsAValidProgramRunContract(t *testing.T) {
	for _, retrieval := range RetrievalPaths {
		run := ProgramRun{ProgramID: "fix", Retrieval: retrieval, Verdict: program.Verdict{Result: program.Match}}
		if err := wire.Validate(programRunContract(run)); err != nil {
			t.Errorf("retrieval %q is rejected by the ProgramRun contract: %v", retrieval, err)
		}
	}
}

// A playbook a detector finding names is tried because of the detector; a role-less one nothing names is tried on
// every finding, by scan; one with object roles is tried because a finding's resources bound them.
func TestARunSaysHowThePlaybookCameToBeTried(t *testing.T) {
	finding := sdk.Finding{DetectorID: "d", RuleID: "r", Status: sdk.FindingActive, Fingerprint: "f",
		PrimaryResource: keyedDeployment("front")}
	named := finding
	named.Playbooks = []string{"named"}
	registry, err := program.NewRegistry([]program.Diagnoser{
		candidateProgram{spec: program.ProgramSpec{ID: "named"}, run: func(context.Context, program.Bindings) program.Verdict { return program.Verdict{Result: program.Match} }},
		candidateProgram{spec: program.ProgramSpec{ID: "scanned"}, run: func(context.Context, program.Bindings) program.Verdict { return program.Verdict{Result: program.Match} }},
		candidateProgram{spec: program.ProgramSpec{ID: "bound", Params: []program.ParamSpec{{Name: ComponentRole, Kind: "Deployment", Required: true}}}, run: func(context.Context, program.Bindings) program.Verdict { return program.Verdict{Result: program.Match} }},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, run := range SelectProgramCandidates(context.Background(), registry, keyedView("front"), []sdk.Finding{named}, time.Second) {
		got[run.ProgramID] = run.Retrieval
	}
	want := map[string]string{"named": RetrievalDetector, "scanned": RetrievalRoleLessScan, "bound": RetrievalRoleBound}
	for id, retrieval := range want {
		if got[id] != retrieval {
			t.Errorf("%s retrieval = %q, want %q (all: %v)", id, got[id], retrieval, got)
		}
	}
	// Another finding that does not name the playbook never downgrades a detector's attribution.
	other := finding
	other.Fingerprint = "g"
	got = map[string]string{}
	for _, run := range SelectProgramCandidates(context.Background(), registry, keyedView("front"), []sdk.Finding{other, named}, time.Second) {
		got[run.ProgramID] = run.Retrieval
	}
	if got["named"] != RetrievalDetector {
		t.Errorf("named retrieval with an unnamed finding first = %q", got["named"])
	}
}

// Every Match carries its retrieval path; a playbook whose diagnosis was Unknown is listed with why, and the
// count of outcomes it was Unknown in.
func TestCoverageNamesRetrievalOnMatchesAndListsUnresolvedPlaybooks(t *testing.T) {
	finding := sdk.Finding{DetectorID: "d", RuleID: "r", Status: sdk.FindingActive, PrimaryResource: keyedDeployment("front")}
	finding.Fingerprint = FindingFingerprint(finding)
	runs := []ProgramRun{
		{ProgramID: "seen", ProgramRevision: "r1", FindingFingerprint: finding.Fingerprint, Retrieval: RetrievalRoleLessScan, Verdict: program.Verdict{Result: program.Match}},
		{ProgramID: "blind", ProgramRevision: "r2", FindingFingerprint: finding.Fingerprint, Retrieval: RetrievalDetector, Verdict: unknownVerdict("object not captured: v1/ConfigMap app/settings")},
		{ProgramID: "other", ProgramRevision: "r3", FindingFingerprint: finding.Fingerprint, Verdict: program.Verdict{Result: program.NoMatch}},
	}
	coverage := buildPlaybookCoverage([]sdk.Finding{finding}, runs, nil, map[string]ProgramTrust{"blind": {UnknownOnRecurrence: 2}})
	if len(coverage.GetMatches()) != 1 || coverage.GetMatches()[0].GetRetrieval() != RetrievalRoleLessScan {
		t.Fatalf("matches = %v, want the role-less scan attribution", coverage.GetMatches())
	}
	unresolved := coverage.GetUnresolved()
	if len(unresolved) != 1 || unresolved[0].GetPlaybookId() != "blind" || unresolved[0].GetResult() != "unknown" ||
		!strings.Contains(unresolved[0].GetReason(), "object not captured") || unresolved[0].GetUnknownOnRecurrence() != 2 || unresolved[0].GetRetrieval() != RetrievalDetector {
		t.Fatalf("unresolved = %v", unresolved)
	}
}

// A playbook that is only ever Unknown on a recurrence keeps a counter, never a failure.
func TestTrustCountsAnUnknownRecurrenceWithoutDebitingTheRecord(t *testing.T) {
	outcome := func(at int64, results ...contracts.Verdict_Result) *contracts.OutcomeRecord {
		record := &contracts.OutcomeRecord{Timestamps: &contracts.OutcomeTimestamps{CompletedAt: timestamppb.New(time.Unix(at, 0))}}
		for _, result := range results {
			record.ProgramRuns = append(record.ProgramRuns, &contracts.ProgramRun{ProgramId: "blind", ProgramRevision: "r1", Verdict: &contracts.Verdict{Result: result}})
		}
		return record
	}
	stats := ProgramTrustStats([]*contracts.OutcomeRecord{
		outcome(1, contracts.Verdict_RESULT_UNKNOWN, contracts.Verdict_RESULT_UNKNOWN),
		outcome(2, contracts.Verdict_RESULT_NO_MATCH),
		outcome(3, contracts.Verdict_RESULT_UNKNOWN),
	})
	if got := stats["blind"]; got.UnknownOnRecurrence != 2 || got.Failures != 0 || got.VerifiedSuccesses != 0 {
		t.Fatalf("trust = %+v, want two outcomes with an Unknown, no failure", got)
	}
}

// The stage reports every diagnosis it made, matched or not, once per playbook and revision.
func TestStageDiagnosesReportEveryTriedPlaybook(t *testing.T) {
	runs := []ProgramRun{
		{ProgramID: "seen", ProgramRevision: "r1", Retrieval: RetrievalDetector, Verdict: program.Verdict{Result: program.Match}},
		{ProgramID: "blind", ProgramRevision: "r2", Retrieval: RetrievalRoleLessScan, Verdict: unknownVerdict("unrecorded logs call")},
		{ProgramID: "blind", ProgramRevision: "r2", Retrieval: RetrievalRoleLessScan, Verdict: unknownVerdict("unrecorded logs call")},
		{ProgramID: SourceRestoreID, Verdict: program.Verdict{Result: program.NoMatch}},
	}
	got := stageDiagnoses(runs)
	if len(got) != 2 || got[0].GetPlaybookId() != "blind" || got[0].GetResult() != "unknown" || got[0].GetReason() != "unrecorded logs call" ||
		got[1].GetPlaybookId() != "seen" || got[1].GetResult() != "match" || got[1].GetRetrieval() != RetrievalDetector {
		t.Fatalf("diagnoses = %v", got)
	}
	for _, verdict := range got {
		if err := wire.Validate(verdict); err != nil {
			t.Errorf("%s: %v", verdict.GetPlaybookId(), err)
		}
	}
}

// The agent's page says, as a fact, what SDO tried and what came of it.
func TestThePageStatesWhatTheStageTried(t *testing.T) {
	stages := []*contracts.StageReport{{Stage: "deterministic", Diagnoses: []*contracts.PlaybookVerdict{
		{PlaybookId: "seen", Result: "match", Retrieval: RetrievalRoleLessScan},
		{PlaybookId: "blind", Result: "unknown", Reason: "object not captured: v1/ConfigMap app/settings"},
		{PlaybookId: "quiet", Result: "no_match", AskedAt: timestamppb.New(time.Unix(1, 0))},
	}}}
	lines := strings.Join(diagnosisLines(stages), "\n")
	for _, want := range []string{"tried by sdo: seen -> match (role_less_scan)", "tried by sdo: blind -> unknown (object not captured: v1/ConfigMap app/settings)", "tried by sdo: quiet -> no_match (asked 00:00:01Z)"} {
		if !strings.Contains(lines, want) {
			t.Fatalf("lines lack %q:\n%s", want, lines)
		}
	}
}

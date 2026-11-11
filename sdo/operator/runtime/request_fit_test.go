package runtime

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/worker_registry"
)

func syntheticRun(id string, evidenceBytes int, duration time.Duration) *contracts.ProgramRun {
	ref := &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}
	return &contracts.ProgramRun{
		ProgramId: id, Duration: durationpb.New(duration), FindingFingerprint: "finding-" + id,
		Bindings: []*contracts.Binding{{Name: "deployment", Object: ref}},
		Verdict: &contracts.Verdict{Result: contracts.Verdict_RESULT_MATCH, Evidence: []*contracts.Evidence{{
			Resource: ref, Fact: "live object differs from accepted source", Observed: strings.Repeat("d", evidenceBytes), Expected: "accepted source",
		}}},
	}
}

func reflectionTaskBuilder(request *contracts.ReflectionRequest) func() (*contracts.AgentTask, error) {
	return func() (*contracts.AgentTask, error) {
		payload, err := anypb.New(request)
		if err != nil {
			return nil, err
		}
		return &contracts.AgentTask{SchemaVersion: worker_registry.SchemaVersion, TaskId: "reflection", AgentId: "agent", TaskType: "reflection", Request: payload}, nil
	}
}

// A repeat whose stage applied a source-drift playbook records a handful of runs, each quoting the whole diff as
// evidence. Fewer runs than the run cap must not make the request unsendable.
func TestReflectionRequestFitsWhenEveryRunQuotesALargeDiff(t *testing.T) {
	outcome := &contracts.OutcomeRecord{IncidentId: "incident-1"}
	for index := range 14 {
		outcome.ProgramRuns = append(outcome.ProgramRuns, syntheticRun("source-drift", 40<<10, time.Duration(index)*time.Millisecond))
	}
	outcome.PriorStages = []*contracts.StageReport{{Mitigations: []*contracts.StageMitigation{{PlaybookId: "source-drift", PostApplyDiagnose: syntheticRun("source-drift", 40<<10, 0)}}}}
	request := &contracts.ReflectionRequest{Outcome: outcome}

	fits, steps, err := fitReflectionRequest(request, reflectionTaskBuilder(request))
	if err != nil {
		t.Fatal(err)
	}
	if !fits {
		t.Fatalf("request did not fit after %s", describeFit(steps))
	}
	if len(request.GetOutcome().GetProgramRuns()) != 14 {
		t.Fatalf("runs = %d; bounding evidence must keep every run", len(request.GetOutcome().GetProgramRuns()))
	}
	if !strings.Contains(strings.Join(request.GetCompaction(), ","), "clip_program_evidence") {
		t.Fatalf("compaction = %v, want the evidence step recorded", request.GetCompaction())
	}
	if observed := request.GetOutcome().GetPriorStages()[0].GetMitigations()[0].GetPostApplyDiagnose().GetVerdict().GetEvidence()[0].GetObserved(); len(observed) > keptEvidenceBytes+len("…") {
		t.Fatalf("stage evidence kept %d bytes", len(observed))
	}
}

// When nothing the ladder may remove is large, the failure names every step it tried with the size it left, and
// the fields that remain largest.
func TestReflectionRequestTooLargeRecordsEveryReductionStep(t *testing.T) {
	outcome := &contracts.OutcomeRecord{IncidentId: "incident-1", Findings: []*contracts.Finding{{DetectorId: "detector", Evidence: strings.Repeat("f", worker_registry.MaxRequestBytes)}}}
	outcome.ProgramRuns = []*contracts.ProgramRun{syntheticRun("source-drift", 8<<10, 0)}
	request := &contracts.ReflectionRequest{Outcome: outcome, PriorOutcomes: []*contracts.OutcomeRecord{{IncidentId: "earlier"}}}
	build := reflectionTaskBuilder(request)

	fits, steps, err := fitReflectionRequest(request, build)
	if err != nil {
		t.Fatal(err)
	}
	if fits {
		t.Fatal("an irreducible request must not fit")
	}
	task, err := build()
	if err != nil {
		t.Fatal(err)
	}
	detail := requestTooLargeDetail(steps, task)
	for _, rung := range []string{"drop_prior_incident_history", "trim_memory_to_touched", "clip_memory_documents", "clip_program_evidence", "cap_program_runs_and_tool_calls"} {
		if !strings.Contains(detail, rung) {
			t.Fatalf("detail %q does not name step %s", detail, rung)
		}
	}
	if !strings.Contains(detail, "nothing to remove") || !strings.Contains(detail, "largest remaining: outcome=") {
		t.Fatalf("detail %q must say which steps had nothing to remove and what remains largest", detail)
	}
}

// The agent's loop starts from the stage's runs, so merging the stage's receipts into the agent's result must
// not record the same evaluation twice.
func TestStageRunsAreRecordedOnceWhenTheAgentContinues(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}
	run := func(fingerprint string) ProgramRun {
		return ProgramRun{ProgramID: "source-drift", FindingFingerprint: fingerprint, Bindings: program.Bindings{Objects: program.ObjectBindings{"deployment": ref}}, Verdict: program.Verdict{Result: program.Match}, Duration: time.Second}
	}
	initial, postApply, agent := run("initial"), run(""), run("agent")
	state := &IncidentState{
		Request:     IncidentRequest{IncidentID: "incident-1", InitialProgramRuns: []ProgramRun{initial}},
		PriorStages: []*contracts.StageReport{{Mitigations: []*contracts.StageMitigation{{PlaybookId: "source-drift", PostApplyDiagnose: programRunContract(postApply)}}}},
		Result:      &IncidentResult{IncidentID: "incident-1", Status: IncidentCompleted, ProgramRuns: []ProgramRun{initial, postApply, agent}},
	}
	result, err := responsePipelineResult(state, time.Unix(0, 0), time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.ProgramRuns) != 3 {
		t.Fatalf("runs = %d, want the initial, post-apply and agent runs once each", len(result.ProgramRuns))
	}
	for index, want := range []ProgramRun{initial, postApply, agent} {
		if !proto.Equal(programRunContract(result.ProgramRuns[index]), programRunContract(want)) {
			t.Fatalf("run %d = %+v, want %+v", index, result.ProgramRuns[index], want)
		}
	}
}

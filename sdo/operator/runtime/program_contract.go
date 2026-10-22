package runtime

import (
	"sort"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/program"
)

func programRunContract(run ProgramRun) *contracts.ProgramRun {
	result := &contracts.ProgramRun{CanMitigate: run.CanMitigate, ProgramId: run.ProgramID, FindingFingerprint: run.FindingFingerprint, BindingSource: run.BindingSource, ProgramRevision: run.ProgramRevision, Duration: durationpb.New(run.Duration), Verdict: verdictContract(run.Verdict)}
	keys := make([]string, 0, len(run.Bindings))
	for key := range run.Bindings {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		result.Bindings = append(result.Bindings, &contracts.Binding{Name: key, Object: objectRefContract(run.Bindings[key])})
	}
	for _, evidence := range run.Verdict.Evidence {
		if evidence.Resource.Kind == "" {
			result.Error = evidence.Observed
			break
		}
	}
	return result
}

func relationCoveredFindingContract(covered RelationCoveredFinding) *contracts.RelationCoveredFinding {
	result := &contracts.RelationCoveredFinding{FindingFingerprint: covered.FindingFingerprint, ProgramId: covered.ProgramID}
	keys := make([]string, 0, len(covered.Bindings))
	for key := range covered.Bindings {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		result.Bindings = append(result.Bindings, &contracts.Binding{Name: key, Object: objectRefContract(covered.Bindings[key])})
	}
	return result
}

func relationCoveredFindingState(covered *contracts.RelationCoveredFinding) RelationCoveredFinding {
	result := RelationCoveredFinding{FindingFingerprint: covered.GetFindingFingerprint(), ProgramID: covered.GetProgramId(), Bindings: program.Bindings{}}
	for _, binding := range covered.GetBindings() {
		result.Bindings[binding.GetName()] = objectRefState(binding.GetObject())
	}
	return result
}

func verdictContract(verdict program.Verdict) *contracts.Verdict {
	result := &contracts.Verdict{Question: verdict.Question}
	switch verdict.Result {
	case program.Match:
		result.Result = contracts.Verdict_RESULT_MATCH
	case program.NoMatch:
		result.Result = contracts.Verdict_RESULT_NO_MATCH
	case program.NeedsJudgement:
		result.Result = contracts.Verdict_RESULT_NEEDS_JUDGEMENT
	default:
		result.Result = contracts.Verdict_RESULT_UNKNOWN
	}
	for _, evidence := range verdict.Evidence {
		if evidence.Resource.Kind == "" {
			continue
		}
		result.Evidence = append(result.Evidence, &contracts.Evidence{Resource: objectRefContract(evidence.Resource), Fact: evidence.Fact, Observed: evidence.Observed, Expected: evidence.Expected})
	}
	return result
}

func programRunState(run *contracts.ProgramRun) ProgramRun {
	bindings := program.Bindings{}
	for _, binding := range run.GetBindings() {
		bindings[binding.GetName()] = objectRefState(binding.GetObject())
	}
	verdict := program.Verdict{Question: run.GetVerdict().GetQuestion()}
	switch run.GetVerdict().GetResult() {
	case contracts.Verdict_RESULT_MATCH:
		verdict.Result = program.Match
	case contracts.Verdict_RESULT_NO_MATCH:
		verdict.Result = program.NoMatch
	case contracts.Verdict_RESULT_NEEDS_JUDGEMENT:
		verdict.Result = program.NeedsJudgement
	default:
		verdict.Result = program.Unknown
	}
	for _, evidence := range run.GetVerdict().GetEvidence() {
		verdict.Evidence = append(verdict.Evidence, program.Evidence{Resource: objectRefState(evidence.GetResource()), Fact: evidence.GetFact(), Observed: evidence.GetObserved(), Expected: evidence.GetExpected()})
	}
	if run.GetError() != "" {
		verdict.Evidence = append(verdict.Evidence, program.Evidence{Fact: "diagnosis", Observed: run.GetError()})
	}
	return ProgramRun{CanMitigate: run.CanMitigate, FindingFingerprint: run.GetFindingFingerprint(), BindingSource: run.GetBindingSource(), ProgramID: run.GetProgramId(), ProgramRevision: run.GetProgramRevision(), Bindings: bindings, Verdict: verdict, Duration: run.GetDuration().AsDuration()}
}

func toolCallContract(call ToolCall) *contracts.ProgramToolCall {
	return &contracts.ProgramToolCall{Command: call.Command, At: timestamppb.New(call.At), RequestJson: call.RequestJSON, ResponseJson: call.ResponseJSON, Error: call.Error, Summary: call.Summary}
}

func toolCallState(call *contracts.ProgramToolCall) ToolCall {
	return ToolCall{Command: call.GetCommand(), At: controllerTime(call.GetAt()), RequestJSON: call.GetRequestJson(), ResponseJSON: call.GetResponseJson(), Error: call.GetError(), Summary: call.GetSummary()}
}

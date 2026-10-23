package runtime

import (
	"google.golang.org/protobuf/types/known/structpb"
	"sort"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/program"
)

func programRunContract(run ProgramRun) *contracts.ProgramRun {
	result := &contracts.ProgramRun{CanMitigate: run.CanMitigate, ProgramId: run.ProgramID, FindingFingerprint: run.FindingFingerprint, BindingSource: run.BindingSource, ProgramRevision: run.ProgramRevision, Duration: durationpb.New(run.Duration), Verdict: verdictContract(run.Verdict)}
	result.Bindings = programBindingsContract(run.Bindings)
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
	result.Bindings = programBindingsContract(covered.Bindings)
	return result
}

func relationCoveredFindingState(covered *contracts.RelationCoveredFinding) RelationCoveredFinding {
	result := RelationCoveredFinding{FindingFingerprint: covered.GetFindingFingerprint(), ProgramID: covered.GetProgramId(), Bindings: program.Bindings{Objects: program.ObjectBindings{}}}
	result.Bindings = programBindingsState(covered.GetBindings())
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
	bindings := programBindingsState(run.GetBindings())
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

func programBindingsContract(bindings program.Bindings) []*contracts.Binding {
	keys := make([]string, 0, len(bindings.Objects)+len(bindings.Values))
	for key := range bindings.Objects {
		keys = append(keys, key)
	}
	for key := range bindings.Values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]*contracts.Binding, 0, len(keys))
	for _, key := range keys {
		binding := &contracts.Binding{Name: key}
		if ref, ok := bindings.Objects[key]; ok {
			binding.Object = objectRefContract(ref)
		} else {
			binding.Value, _ = structpb.NewValue(bindings.Values[key])
		}
		result = append(result, binding)
	}
	return result
}
func programBindingsState(encoded []*contracts.Binding) program.Bindings {
	bindings := program.Bindings{Objects: program.ObjectBindings{}}
	for _, binding := range encoded {
		if binding.GetValue() != nil {
			if bindings.Values == nil {
				bindings.Values = map[string]any{}
			}
			bindings.Values[binding.GetName()] = binding.GetValue().AsInterface()
		} else {
			bindings.Objects[binding.GetName()] = objectRefState(binding.GetObject())
		}
	}
	return bindings
}

package runtime

import (
	"sort"
	"strings"

	"google.golang.org/protobuf/types/known/structpb"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/program"
)

func programRunContract(run ProgramRun) *contracts.ProgramRun {
	result := &contracts.ProgramRun{CanMitigate: run.CanMitigate, ProgramId: run.ProgramID, FindingFingerprint: run.FindingFingerprint, BindingSource: run.BindingSource, Retrieval: run.Retrieval, ProgramRevision: run.ProgramRevision, Duration: durationpb.New(run.Duration), Verdict: verdictContract(run.Verdict)}
	result.Bindings = programBindingsContract(run.Bindings)
	for _, step := range run.Verdict.Reads {
		result.ReadTrace = append(result.ReadTrace, &contracts.ReadStep{Op: step.Op, Target: step.Target, Outcome: step.Outcome})
	}
	var reasons []string
	for _, evidence := range run.Verdict.Evidence {
		if evidence.Resource.Kind != "" || (evidence.Fact != "" && run.Verdict.Result != program.Unknown) {
			continue
		}
		reason := evidence.Observed
		// Wire Evidence requires a resource. Binding and relation failures
		// often have none; the existing error channel must carry their facts
		// instead of converting a reasoned Unknown into an unexplained one.
		if evidence.Fact != "" && evidence.Fact != "diagnosis" {
			reason = evidence.Fact + ": " + reason
			if evidence.Expected != "" {
				reason += " (expected " + evidence.Expected + ")"
			}
		}
		if reason != "" {
			reasons = append(reasons, reason)
		}
	}
	result.Error = strings.Join(reasons, "\n")
	return result
}

func verdictContract(verdict program.Verdict) *contracts.Verdict {
	result := &contracts.Verdict{Question: verdict.Question, CauseClass: verdict.CauseClass, Reach: verdict.Reach}
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
	verdict := program.Verdict{Question: run.GetVerdict().GetQuestion(), CauseClass: run.GetVerdict().GetCauseClass(), Reach: run.GetVerdict().GetReach()}
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
	for _, step := range run.GetReadTrace() {
		verdict.Reads = append(verdict.Reads, program.ReadStep{Op: step.GetOp(), Target: step.GetTarget(), Outcome: step.GetOutcome()})
	}
	return ProgramRun{CanMitigate: run.CanMitigate, FindingFingerprint: run.GetFindingFingerprint(), BindingSource: run.GetBindingSource(), Retrieval: run.GetRetrieval(), ProgramID: run.GetProgramId(), ProgramRevision: run.GetProgramRevision(), Bindings: bindings, Verdict: verdict, Duration: run.GetDuration().AsDuration()}
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

// verdictResultName is a diagnosis result as PlaybookVerdict.result states it.
func verdictResultName(result program.Result) string {
	switch result {
	case program.Match:
		return "match"
	case program.NoMatch:
		return "no_match"
	case program.NeedsJudgement:
		return "needs_judgement"
	}
	return "unknown"
}

// verdictReason is why a diagnosis was not a Match, short: the error that made it Unknown, or the facts it
// observed. Empty for a Match.
func verdictReason(run ProgramRun) string {
	if run.Verdict.Result == program.Match {
		return ""
	}
	if run.Verdict.Result == program.Unknown {
		return programRunContract(run).GetError()
	}
	var facts []string
	for _, evidence := range run.Verdict.Evidence {
		if len(facts) == 3 {
			break
		}
		facts = append(facts, strings.TrimSpace(evidence.Fact+": "+evidence.Observed))
	}
	return strings.Join(facts, "; ")
}

// playbookVerdictContract is one diagnosis as the stage report and coverage state it.
func playbookVerdictContract(run ProgramRun) *contracts.PlaybookVerdict {
	reason := verdictReason(run)
	if len(reason) > 300 {
		reason = reason[:300]
	}
	return &contracts.PlaybookVerdict{PlaybookId: run.ProgramID, ProgramRevision: run.ProgramRevision, Result: verdictResultName(run.Verdict.Result), Reason: reason, Retrieval: run.Retrieval}
}

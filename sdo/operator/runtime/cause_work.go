package runtime

import (
	"context"
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// classifyCauseWork preserves the responder's groups. Absence of exact evidence
// creates a question; it never infers a cause, a merge, a split, or a credit.
func classifyCauseWork(outcome *contracts.OutcomeRecord, fixed map[string]program.Result) []*contracts.ReflectionCauseWork {
	revisions := programRunRevisions(outcome)
	credited := map[string][]program.Bindings{}
	for _, app := range outcome.GetPlanApplications() {
		if !app.GetApplied() || !app.GetVerificationCompleted() || !app.GetVerificationPassed() || app.GetVerification().GetResult() != contracts.Verdict_RESULT_NO_MATCH || app.GetRolledBack() || app.GetDryRun() || app.GetError() != "" || app.GetRollbackError() != "" ||
			!app.GetFinalEquivalenceVerified() {
			continue
		}
		for _, id := range app.GetProgramIds() {
			failed := outcome.GetClassification() != "success" || outcome.RepairCommit != nil || outcome.GetResponseLoopExhausted()
			for _, signal := range outcome.GetProgramFailureSignals() {
				if strings.HasSuffix(signal, ":"+id) {
					failed = true
				}
				failedID, _, ok := parseProgramTrustFailureSignal(signal)
				if ok && failedID == id {
					failed = true
				}
			}
			if failed {
				continue
			}
			if boundPostApplyNoMatch(outcome, app, id, revisions[id]) {
				credited[id] = append(credited[id], appliedPlanBindings(outcome, app, id)...)
			}
		}
	}
	written := func(ref sdk.ObjectRef) bool {
		for _, action := range outcome.GetRepairActions() {
			if action.GetSuccess() {
				for _, resource := range action.GetResources() {
					if sameResource(ref, objectRefState(resource)) {
						return true
					}
				}
			}
		}
		return false
	}
	result := []*contracts.ReflectionCauseWork{}
	for index, cause := range outcome.GetConfirmedRootCauses() {
		work := &contracts.ReflectionCauseWork{CauseIndex: uint32(index)}
		for _, ref := range cause.GetResources() {
			work.Resources = append(work.Resources, proto.Clone(ref).(*contracts.ObjectRef))
		}
		seen := map[string]bool{}
		boundResources := []sdk.ObjectRef{}
		for _, run := range outcome.GetProgramRuns() {
			if run.GetVerdict().GetResult() != contracts.Verdict_RESULT_MATCH {
				continue
			}
			bindings := program.Bindings{}
			relevant := false
			touched := false
			for _, binding := range run.GetBindings() {
				ref := objectRefState(binding.GetObject())
				bindings[binding.GetName()] = ref
				touched = touched || written(ref)
				for _, resource := range cause.GetResources() {
					relevant = relevant || sameResource(ref, objectRefState(resource))
				}
			}
			key := matchedViewKey(run.GetProgramId(), bindings)
			if !relevant || seen[key] {
				continue
			}
			seen[key] = true
			for _, ref := range bindings {
				boundResources = append(boundResources, ref)
			}
			item := &contracts.ReflectionWorkItem{ProgramId: run.GetProgramId(), ProgramRevision: run.GetProgramRevision(), Bindings: programRunContract(ProgramRun{Bindings: bindings}).Bindings, Disposition: "question", Reason: "uncertain", Question: "What reusable relation, if any, does the evidence support for this cause?"}
			switch fixed[key] {
			case program.Match:
				if outcome.GetClassification() != "success" {
					break
				}
				item.Disposition, item.Reason = "false_match", "fixed_capture_match"
				item.Question = "The accepted program still matches the verified fixed capture. Refine it to pass the captured NoMatch regression."
			case program.NoMatch:
				equivalent := slices.ContainsFunc(credited[run.GetProgramId()], func(candidate program.Bindings) bool { return maps.Equal(candidate, bindings) })
				if equivalent && touched {
					item.Disposition, item.Reason, item.Question = "covered", "verified_equivalent_repair", ""
				} else if fixed[key] == program.NoMatch && !touched {
					item.Reason, item.Question = "binding_not_written", "This matched binding was never written and now NoMatches. Explain it without assuming the program repaired it."
				} else if fixed[key] == program.NoMatch {
					item.Reason, item.Question = "repair_equivalence_question", "This binding was written but exact plan equivalence is unproven. Decide what the different or incomplete repair evidence teaches."
				}
			}
			work.Items = append(work.Items, item)
		}
		seenWrites := map[sdk.ObjectRef]bool{}
		for _, resource := range cause.GetResources() {
			ref := objectRefState(resource)
			ref.APIVersion = ""
			if !seenWrites[ref] && written(ref) && !slices.ContainsFunc(boundResources, func(bound sdk.ObjectRef) bool { return sameResource(bound, ref) }) {
				seenWrites[ref] = true
				work.Items = append(work.Items, &contracts.ReflectionWorkItem{Resources: []*contracts.ObjectRef{proto.Clone(resource).(*contracts.ObjectRef)}, Disposition: "question", Reason: "unbound_write", Question: "A repaired object in this confirmed cause had no matched program binding. What reusable relation is supported?"})
			}
		}
		if len(work.Items) == 0 {
			work.Items = append(work.Items, &contracts.ReflectionWorkItem{Disposition: "question", Reason: "uncertain", Question: "No exact existing-program case settles this confirmed cause. Evaluate the captured evidence."})
		}
		result = append(result, work)
	}
	return result
}

// fixedCaptureVerdicts uses only the accepted deterministic registry and actual
// fixed primitive observations. Unknown reads remain questions.
func fixedCaptureVerdicts(ctx context.Context, outcome *contracts.OutcomeRecord, registry program.ProgramRegistry, captures string, origins ...map[string]string) map[string]program.Result {
	result := map[string]program.Result{}
	if registry == nil {
		return result
	}
	raw, err := os.ReadFile(filepath.Join(captures, "fixed.json"))
	if err != nil {
		return result
	}
	var snapshot sdktest.Snapshot
	if json.Unmarshal(raw, &snapshot) != nil {
		return result
	}
	type capturedCalls struct {
		phase string
		calls []sdktest.PrimitiveCall
	}
	recordings := []capturedCalls{}
	for _, phase := range []string{"detect", "pre-apply"} {
		data, err := os.ReadFile(filepath.Join(captures, "open@"+phase+"-recordings.json"))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return result
		}
		var calls []sdktest.PrimitiveCall
		if json.Unmarshal(data, &calls) != nil {
			return result
		}
		recordings = append(recordings, capturedCalls{phase, calls})
	}
	for _, run := range outcome.GetProgramRuns() {
		if run.GetVerdict().GetResult() != contracts.Verdict_RESULT_MATCH {
			continue
		}
		bindings := program.Bindings{}
		for _, binding := range run.GetBindings() {
			bindings[binding.GetName()] = objectRefState(binding.GetObject())
		}
		key := matchedViewKey(run.GetProgramId(), bindings)
		if _, exists := result[key]; exists {
			continue
		}
		if diagnoser, ok := registry.Diagnoser(run.GetProgramId()); ok {
			preferredOpen := false
			for _, recording := range recordings {
				corpus := sdktest.CorpusCase{Recordings: recording.calls}
				view := &sdktest.ReplayReadView{Snapshot: snapshot, Calls: corpus.PhaseRecordings("fixed")}
				verdict := program.RunDiagnose(ctx, diagnoser, view, bindings, 10*time.Second).Result
				// A definite fixed Match takes precedence. Otherwise prefer an
				// independently replayable open Match for a positive corpus case.
				selected := result[key] == "" || (verdict == program.NoMatch && result[key] != program.Match && !preferredOpen) || verdict == program.Match
				if selected {
					result[key] = verdict
					if len(origins) > 0 {
						origins[0][key] = recording.phase
					}
				}
				if verdict == program.Match {
					break
				}
				if verdict == program.NoMatch {
					data, err := os.ReadFile(filepath.Join(captures, "open@"+recording.phase+".json"))
					var open sdktest.Snapshot
					if err == nil && json.Unmarshal(data, &open) == nil && program.RunDiagnose(ctx, diagnoser, &sdktest.ReplayReadView{Snapshot: open, Calls: corpus.PhaseRecordings("open")}, bindings, 10*time.Second).Result == program.Match {
						result[key] = verdict
						if len(origins) > 0 {
							origins[0][key] = recording.phase
						}
						preferredOpen = true
					}
				}
			}
		}
	}
	return result
}

func causeWorkNeedsModel(work []*contracts.ReflectionCauseWork) bool {
	if len(work) == 0 {
		return true
	}
	for _, cause := range work {
		for _, item := range cause.GetItems() {
			if item.GetDisposition() == "question" {
				return true
			}
		}
	}
	return false
}

func (n NativeIncidentBroker) prepareCauseWork(ctx context.Context, outcome *contracts.OutcomeRecord, workspace IncidentWorkspace) *contracts.ReflectionEligibility {
	capture := ""
	if n.SessionRoot != "" {
		capture, _ = memory.IncidentCorpusDir(n.SessionRoot, workspace.IncidentID)
	}
	origins := map[string]string{}
	fixed := fixedCaptureVerdicts(ctx, outcome, n.Programs, capture, origins)
	work := classifyCauseWork(outcome, fixed)
	unassigned := unassignedReflectionQuestions(outcome, fixed)
	allItems := append([]*contracts.ReflectionWorkItem{}, unassigned...)
	for _, cause := range work {
		allItems = append(allItems, cause.Items...)
	}
	for _, item := range allItems {
		bindings := program.Bindings{}
		for _, binding := range item.Bindings {
			bindings[binding.GetName()] = objectRefState(binding.GetObject())
		}
		item.CaptureOpenPhase = origins[matchedViewKey(item.ProgramId, bindings)]
		if item.Disposition == "question" {
			continue
		}
		revision, err := programDependencyRevision(ctx, workspace.Worktree, workspace.BaseCommit, item.ProgramId)
		if err != nil || revision != item.GetProgramRevision() {
			item.Disposition, item.Reason, item.Question = "question", "uncertain", "The accepted program revision differs from the incident evidence. Evaluate this cause without credit."
			continue
		}
	}
	return &contracts.ReflectionEligibility{Version: 1, Causes: work, Unassigned: unassigned}
}

// Corpus publication and trust consume the same settled dispositions.
func settledProgramCases(eligibility *contracts.ReflectionEligibility) (map[string][]program.Bindings, map[string][]program.Bindings) {
	positive, negative := map[string][]program.Bindings{}, map[string][]program.Bindings{}
	if eligibility.GetVersion() != 1 {
		return positive, negative
	}
	items := append([]*contracts.ReflectionWorkItem{}, eligibility.GetUnassigned()...)
	for _, cause := range eligibility.GetCauses() {
		items = append(items, cause.GetItems()...)
	}
	for _, item := range items {
		bindings := program.Bindings{}
		for _, binding := range item.GetBindings() {
			bindings[binding.GetName()] = objectRefState(binding.GetObject())
		}
		if item.GetDisposition() == "covered" {
			positive[item.GetProgramId()] = append(positive[item.GetProgramId()], bindings)
		}
		if item.GetDisposition() == "false_match" {
			negative[item.GetProgramId()] = append(negative[item.GetProgramId()], bindings)
		}
	}
	return positive, negative
}

func unassignedReflectionQuestions(outcome *contracts.OutcomeRecord, fixed map[string]program.Result) []*contracts.ReflectionWorkItem {
	grouped := func(ref sdk.ObjectRef) bool {
		for _, cause := range outcome.GetConfirmedRootCauses() {
			for _, resource := range cause.GetResources() {
				if sameResource(ref, objectRefState(resource)) {
					return true
				}
			}
		}
		return false
	}
	items := []*contracts.ReflectionWorkItem{}
	seen := map[string]bool{}
	for _, run := range outcome.GetProgramRuns() {
		if run.GetVerdict().GetResult() != contracts.Verdict_RESULT_MATCH {
			continue
		}
		relevant := false
		bindings := program.Bindings{}
		for _, binding := range run.GetBindings() {
			ref := objectRefState(binding.GetObject())
			bindings[binding.GetName()] = ref
			relevant = relevant || grouped(ref)
		}
		key := matchedViewKey(run.GetProgramId(), bindings)
		if relevant || seen[key] {
			continue
		}
		seen[key] = true
		item := &contracts.ReflectionWorkItem{ProgramId: run.GetProgramId(), ProgramRevision: run.GetProgramRevision(), Bindings: run.Bindings, Disposition: "question", Reason: "uncertain", Question: "This matched binding is outside the responder's confirmed cause resources. Review it without inferring another cause group or granting credit."}
		if fixed[key] == program.Match && outcome.GetClassification() == "success" {
			item.Disposition, item.Reason, item.Question = "false_match", "fixed_capture_match", "This unassigned program still matches the verified fixed capture. Correct the required NoMatch regression without inferring a cause group."
		}
		items = append(items, item)
	}
	seenWrites := map[sdk.ObjectRef]bool{}
	for _, action := range outcome.GetRepairActions() {
		if action.GetSuccess() {
			for _, resource := range action.GetResources() {
				ref := objectRefState(resource)
				ref.APIVersion = ""
				if !grouped(ref) && !seenWrites[ref] {
					seenWrites[ref] = true
					items = append(items, &contracts.ReflectionWorkItem{Resources: []*contracts.ObjectRef{proto.Clone(resource).(*contracts.ObjectRef)}, Disposition: "question", Reason: "unbound_write", Question: "A repaired resource is outside confirmed cause groups. Review the write without inventing a cause or granting credit."})
				}
			}
		}
	}
	return items
}

func reflectionItemsNeedModel(items []*contracts.ReflectionWorkItem) bool {
	for _, item := range items {
		if item.GetDisposition() == "question" {
			return true
		}
	}
	return false
}

func settledCaptureOrigins(eligibility *contracts.ReflectionEligibility) map[string]string {
	origins := map[string]string{}
	items := append([]*contracts.ReflectionWorkItem{}, eligibility.GetUnassigned()...)
	for _, cause := range eligibility.GetCauses() {
		items = append(items, cause.GetItems()...)
	}
	for _, item := range items {
		bindings := program.Bindings{}
		for _, binding := range item.GetBindings() {
			bindings[binding.GetName()] = objectRefState(binding.GetObject())
		}
		origins[memory.CapturedInstanceKey(item.GetProgramId(), bindings)] = item.GetCaptureOpenPhase()
	}
	return origins
}

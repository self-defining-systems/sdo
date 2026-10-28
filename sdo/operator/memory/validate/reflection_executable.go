package validate

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/layout"
	programvalidation "sdo.dev/operator/memory/validate/program"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// Executable coverage uses original receipt identities and positive binding
// metadata, never replay verdicts or the availability of captured snapshots.
// Every performed hand repair is encodable as a typed plan or RunScript;
// SDK gaps can describe missing observations, but cannot replace that step.
func recordedRepairProgramDiagnostics(root string, outcome *contracts.OutcomeRecord, repairs []*contracts.RepairActionReceipt, changed map[string]bool) []Diagnostic {
	if outcome.GetClassification() != "success" {
		// Reuse existing independent learning authority. Unverified peer
		// commands cannot authorize programs or make proven steps all-or-nothing.
		causes := VerifiedReflectionCauses(outcome)
		repairs = slices.DeleteFunc(append([]*contracts.RepairActionReceipt(nil), repairs...), func(receipt *contracts.RepairActionReceipt) bool {
			if len(receipt.GetResources()) == 0 {
				return true
			}
			for _, ref := range receipt.GetResources() {
				if !slices.ContainsFunc(causes, func(cause *contracts.ReflectionCauseWork) bool {
					return slices.ContainsFunc(cause.GetResources(), func(bound *contracts.ObjectRef) bool {
						return equalCauseResource(ref, bound) || slices.ContainsFunc(receipt.GetOwners(), func(owner *contracts.ResourceOwners) bool {
							return equalCauseResource(owner.GetResource(), ref) && slices.ContainsFunc(owner.GetOwnerChain(), func(parent *contracts.ObjectRef) bool { return equalCauseResource(parent, bound) })
						})
					})
				}) {
					return true
				}
			}
			return false
		})
	}
	if len(repairs) == 0 {
		return nil
	}
	moduleData, _ := os.ReadFile(filepath.Join(root, layout.DiagnosticsGoModFile))
	module := ""
	for _, line := range strings.Split(string(moduleData), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "module" {
			module = fields[1]
			break
		}
	}
	specs, inspectErr := programvalidation.InspectMemory(filepath.Join(root, layout.Dir), module)
	var cases []sdktest.CorpusCase
	if inspectErr == nil {
		for _, spec := range specs {
			if spec.Mitigator == "" || !changed[spec.ID] {
				continue
			}
			for _, id := range spec.Cases {
				c, err := sdktest.LoadCorpusMetadata(filepath.Join(root, layout.CorpusDir, id))
				if err == nil && slices.Contains(c.RootCauseProgramIDs, spec.ID) {
					cases = append(cases, c)
				}
			}
		}
	}
	var ds []Diagnostic
	for _, receipt := range repairs {
		linked := false
		covered := make([]bool, len(receipt.GetResources()))
		for _, c := range cases {
			if !caseHasRepairReceipt(c, outcome.GetIncidentId(), receipt) {
				continue
			}
			linked = true
			for index, ref := range receipt.GetResources() {
				for _, bound := range c.Bindings.Objects {
					candidate := &contracts.ObjectRef{Kind: bound.Kind, Namespace: bound.Namespace, Name: bound.Name}
					covered[index] = covered[index] || equalCauseResource(ref, candidate) || slices.ContainsFunc(receipt.GetOwners(), func(owner *contracts.ResourceOwners) bool {
						return equalCauseResource(owner.GetResource(), ref) && slices.ContainsFunc(owner.GetOwnerChain(), func(parent *contracts.ObjectRef) bool { return equalCauseResource(parent, candidate) })
					})
				}
			}
		}
		if linked && !slices.Contains(covered, false) {
			continue
		}
		ds = append(ds, Diagnostic{Rule: RuleReflectionProgramRequired, Message: fmt.Sprintf("recorded hand repair %q (%s) requires an executable typed SDK or RunScript step; README text and SDK gaps do not encode a performed repair", receipt.GetActionId(), receipt.GetTarget()), Hint: programRequiredHint(outcome.GetIncidentId(), receipt, linked, covered)})
	}
	return ds
}

// programRequiredHint states the exact corpus change that links a receipt:
// the recordings entry is the one caseHasRepairReceipt accepts, so pasting it
// verbatim cannot miss a field. Replay stays informational.
func programRequiredHint(incident string, receipt *contracts.RepairActionReceipt, linked bool, covered []bool) string {
	var unbound []string
	for index, ref := range receipt.GetResources() {
		if !linked || !covered[index] {
			// The exact ref to bind: cluster-scoped objects carry no namespace.
			exact, _ := json.Marshal(sdk.ObjectRef{APIVersion: ref.GetApiVersion(), Kind: ref.GetKind(), Namespace: ref.GetNamespace(), Name: ref.GetName()})
			unbound = append(unbound, string(exact))
		}
	}
	bind := ""
	if len(unbound) > 0 {
		bind = " and bind " + strings.Join(unbound, ", ") + " (or a recorded owner) as a role under labels.json bindings.objects"
	}
	if linked {
		return "a case already links this receipt; in that case, bind " + strings.Join(unbound, ", ") + " (or a recorded owner) as a role under labels.json bindings.objects, exactly as written; replay results stay informational"
	}
	link := program.ScriptOriginReceipt{ID: receipt.GetActionId(), IncidentID: incident, Success: true, StartedAt: receipt.GetStartedAt().AsTime(), CompletedAt: receipt.GetCompletedAt().AsTime(), ScriptDigest: receipt.GetScriptDigest()}
	for _, ref := range receipt.GetResources() {
		link.Resources = append(link.Resources, sdk.ObjectRef{APIVersion: ref.GetApiVersion(), Kind: ref.GetKind(), Namespace: ref.GetNamespace(), Name: ref.GetName()})
	}
	result, _ := json.Marshal(link)
	entry, _ := json.Marshal(sdktest.PrimitiveCall{Method: "RepairReceipt", Arguments: json.RawMessage("{}"), Result: result})
	return "encode it in a mitigate.go of a playbook listed in proposed_artifact_ids; in a corpus case (.sdo/corpus/<case>/) whose labels.json root_cause_program_ids lists that playbook" + bind + ", append this entry to its recordings.json: " + string(entry)
}

func caseHasRepairReceipt(c sdktest.CorpusCase, incident string, receipt *contracts.RepairActionReceipt) bool {
	if receipt.GetStartedAt() == nil || receipt.GetCompletedAt() == nil {
		return false
	}
	for _, call := range c.Recordings {
		if call.Method != "RepairReceipt" || call.Error != "" {
			continue
		}
		var link program.ScriptOriginReceipt
		if json.Unmarshal(call.Result, &link) != nil || link.ID != receipt.GetActionId() || link.IncidentID != incident || !link.Success || !link.StartedAt.Equal(receipt.GetStartedAt().AsTime()) || !link.CompletedAt.Equal(receipt.GetCompletedAt().AsTime()) || link.ScriptDigest != receipt.GetScriptDigest() {
			continue
		}
		if len(link.Resources) != len(receipt.GetResources()) {
			continue
		}
		if slices.ContainsFunc(receipt.GetResources(), func(ref *contracts.ObjectRef) bool {
			return !slices.ContainsFunc(link.Resources, func(other sdk.ObjectRef) bool {
				return equalCauseResource(ref, &contracts.ObjectRef{Kind: other.Kind, Namespace: other.Namespace, Name: other.Name})
			})
		}) {
			continue
		}
		return true
	}
	return false
}

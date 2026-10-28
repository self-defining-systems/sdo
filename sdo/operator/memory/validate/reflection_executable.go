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
		ds = append(ds, Diagnostic{Rule: RuleReflectionProgramRequired, Message: fmt.Sprintf("recorded hand repair %q (%s) requires an executable typed SDK or RunScript step; README text and SDK gaps do not encode a performed repair", receipt.GetActionId(), receipt.GetTarget()), Hint: "return updated with the changed program ID; link its positive case to this original RepairReceipt and bind the repaired resources. Replay snapshots and verdicts remain informational"})
	}
	return ds
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

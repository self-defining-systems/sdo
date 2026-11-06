package validate

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/planfile"
)

// Deployment class rules: patch.* (shared strict patch parser), deployment.invalid
// (plan.yaml decode), the patch owner header, and the deployer's advance grant.

const (
	gatePatchDir      = ".sdo/deployment/patches/"
	gatePatchReason   = "The chart's db-migrate Job has no imagePullPolicy and fails against the local registry."
	gatePatchDiff     = "diff --git a/charts/x/job.yaml b/charts/x/job.yaml\nindex 1111111111111111111111111111111111111111..2222222222222222222222222222222222222222 100644\n--- a/charts/x/job.yaml\n+++ b/charts/x/job.yaml\n@@ -1,3 +1,4 @@\n a\n+b\n c\n d\n"
	gateValidPlan     = "{\"source\":{\"rendererId\":\"raw\",\"config\":{\"@type\":\"type.googleapis.com/sdodev.contracts.v1alpha1.RawRenderConfig\",\"paths\":[\".sdo/deployment/manifests\"]}}}\n"
	gateDeployerPatch = "SDO-Patch: 1\nOwner: deployer\nReason: " + gatePatchReason + "\n\n" + gatePatchDiff
)

func gateResponderPatch(extra string) string {
	return "SDO-Patch: 1\nOwner: responder\nIncident: inc-1\nReason: " + gatePatchReason + "\n" + extra + "\n" + gatePatchDiff
}

func gateDeployerPatchWith(extra string) string {
	return "SDO-Patch: 1\nOwner: deployer\nReason: " + gatePatchReason + "\n" + extra + "\n" + gatePatchDiff
}

func gateHeadUpstreamBase(t *testing.T, r string) string {
	t.Helper()
	return gitIn(t, r, "rev-parse", "HEAD")
}

func deploymentCheckRow(name string, actor Actor, phase Phase, edit func(*testing.T, string), rules ...string) gateRow {
	return gateRow{name: name, kind: gateCheck, actor: actor, phase: phase, rules: rules, edit: edit}
}

var deploymentRuleRows = []gateRow{
	// patch.invalid: the strict header, reason, content and series caps.
	{name: "valid plan and patch pass the static rules", kind: gateStatic, edit: func(t *testing.T, r string) {
		write(t, r, layout.DeploymentPlanFile, gateValidPlan)
		write(t, r, gatePatchDir+"0001-pull-policy.patch", gateDeployerPatch)
	}},
	{name: "patch with an unknown header key", kind: gateStatic, rules: []string{RulePatchInvalid}, path: gatePatchDir + "0001-x.patch",
		edit: gateWrite(gatePatchDir+"0001-x.patch", gateDeployerPatchWith("Colour: red\n"))},
	{name: "patch without a reason", kind: gateStatic, rules: []string{RulePatchInvalid}, path: gatePatchDir + "0001-x.patch",
		edit: gateWrite(gatePatchDir+"0001-x.patch", "SDO-Patch: 1\nOwner: deployer\n\n"+gatePatchDiff)},
	{name: "patch whose reason is a path list", kind: gateStatic, rules: []string{RulePatchInvalid}, path: gatePatchDir + "0001-x.patch",
		edit: gateWrite(gatePatchDir+"0001-x.patch", "SDO-Patch: 1\nOwner: deployer\nReason: charts/x/job.yaml, charts/x/other.yaml, charts/x/third.yaml\n\n"+gatePatchDiff)},
	{name: "patch touching operational memory", kind: gateStatic, rules: []string{RulePatchInvalid}, path: gatePatchDir + "0001-x.patch",
		edit: gateWrite(gatePatchDir+"0001-x.patch", strings.ReplaceAll(gateDeployerPatch, "charts/x/job.yaml", ".sdo/goal.md"))},
	{name: "patch larger than the per-patch cap", kind: gateStatic, rules: []string{RulePatchInvalid, RuleTooLarge}, path: gatePatchDir + "0001-x.patch",
		edit: gateWrite(gatePatchDir+"0001-x.patch", gateDeployerPatch+strings.Repeat("x", 300<<10))},
	{name: "two patches share a number", kind: gateStatic, rules: []string{RulePatchInvalid}, edit: func(t *testing.T, r string) {
		write(t, r, gatePatchDir+"0001-a.patch", gateDeployerPatch)
		write(t, r, gatePatchDir+"0001-b.patch", gateDeployerPatch)
	}},
	{name: "more patches than the series cap", kind: gateStatic, rules: []string{RulePatchInvalid}, edit: func(t *testing.T, r string) {
		for i := 1; i <= 65; i++ {
			write(t, r, fmt.Sprintf("%s%04d-p.patch", gatePatchDir, i), gateDeployerPatch)
		}
	}},

	// patch.upstream-base: a stated base must be a commit of this repository.
	deploymentCheckRow("patch names an upstream base that is not a commit here", Responder, ProposalPhase,
		gateWrite(gatePatchDir+"0001-x.patch", gateResponderPatch("Upstream-Base: "+strings.Repeat("a", 40)+"\n")), RulePatchUpstreamBase),
	deploymentCheckRow("patch names an upstream base that is a commit here", Responder, ProposalPhase, func(t *testing.T, r string) {
		write(t, r, gatePatchDir+"0001-x.patch", gateResponderPatch("Upstream-Base: "+gateHeadUpstreamBase(t, r)+"\n"))
	}),

	// patch.owner: the header names the writing actor.
	// The initial phase is closed once detectors exist, so the deployer row
	// also carries lifecycle.bootstrap-closed, and a deployer's patch is source
	// that needs a verified tree; the owner rule is the point.
	deploymentCheckRow("deployer writes a patch owned by the responder", Deployer, InitialPhase,
		gateWrite(gatePatchDir+"0001-x.patch", gateResponderPatch("")), RuleBootstrapClosed, RuleVerifiedSource, RulePatchOwner),
	deploymentCheckRow("responder writes a patch owned by the deployer", Responder, ProposalPhase,
		gateWrite(gatePatchDir+"0001-x.patch", gateDeployerPatch), RulePatchOwner),
	deploymentCheckRow("responder writes its own patch", Responder, ProposalPhase,
		gateWrite(gatePatchDir+"0001-x.patch", gateResponderPatch(""))),

	// deployment.invalid: plan.yaml decodes strictly.
	{name: "plan with an unknown field", kind: gateStatic, rules: []string{RuleDeploymentInvalid}, path: layout.DeploymentPlanFile,
		edit: gateWrite(layout.DeploymentPlanFile, strings.Replace(gateValidPlan, "{\"source\"", "{\"colour\":1,\"source\"", 1))},
	{name: "plan without a source", kind: gateStatic, rules: []string{RuleDeploymentInvalid}, path: layout.DeploymentPlanFile,
		edit: gateWrite(layout.DeploymentPlanFile, "expectedWorkloadIds: [a]\n")},
	{name: "plan that is not YAML", kind: gateStatic, rules: []string{RuleDeploymentInvalid}, path: layout.DeploymentPlanFile,
		edit: gateWrite(layout.DeploymentPlanFile, "source: [unclosed\n")},
	{name: "plan with duplicate keys", kind: gateStatic, rules: []string{RuleDeploymentInvalid}, path: layout.DeploymentPlanFile,
		edit: gateWrite(layout.DeploymentPlanFile, "expectedWorkloadIds: [a]\nexpectedWorkloadIds: [b]\n")},
	{name: "plan that sets the retired strategy", kind: gateStatic, rules: []string{RuleDeploymentInvalid}, path: layout.DeploymentPlanFile,
		edit: gateWrite(layout.DeploymentPlanFile, strings.Replace(gateValidPlan, "{\"source\"", "{\"strategyId\":\"raw\",\"source\"", 1))},
	{name: "plan with a config field its message does not have", kind: gateStatic, rules: []string{RuleDeploymentInvalid}, path: layout.DeploymentPlanFile,
		edit: gateWrite(layout.DeploymentPlanFile, strings.Replace(gateValidPlan, "paths", "pathz", 1))},

	// Owner-header rule: only the owner changes or deletes a patch.
	{name: "deployer modifies its own accepted patch", kind: gateOwnership, actor: Deployer, phase: InitialPhase,
		prepare: gateWrite(gatePatchDir+"0001-x.patch", gateDeployerPatch),
		edit:    gateWrite(gatePatchDir+"0001-x.patch", gateDeployerPatchWith("Upstream-Fix: issue 7\n"))},
	{name: "deployer deletes its own accepted patch", kind: gateOwnership, actor: Deployer, phase: InitialPhase,
		prepare: gateWrite(gatePatchDir+"0001-x.patch", gateDeployerPatch),
		edit:    func(t *testing.T, r string) { remove(t, r, gatePatchDir+"0001-x.patch") }},
	{name: "deployer cannot modify a responder patch", kind: gateOwnership, actor: Deployer, phase: InitialPhase, rules: []string{RuleOwnershipDenied}, path: gatePatchDir + "0001-x.patch",
		prepare: gateWrite(gatePatchDir+"0001-x.patch", gateResponderPatch("")),
		edit:    gateWrite(gatePatchDir+"0001-x.patch", gateResponderPatch("Upstream-Fix: issue 7\n"))},
	{name: "deployer cannot delete a responder patch", kind: gateOwnership, actor: Deployer, phase: InitialPhase, rules: []string{RuleOwnershipDenied}, path: gatePatchDir + "0001-x.patch",
		prepare: gateWrite(gatePatchDir+"0001-x.patch", gateResponderPatch("")),
		edit:    func(t *testing.T, r string) { remove(t, r, gatePatchDir+"0001-x.patch") }},
	{name: "deployer cannot take over an accepted patch it cannot prove it owns", kind: gateOwnership, actor: Deployer, phase: InitialPhase, rules: []string{RuleOwnershipDenied}, path: gatePatchDir + "0001-x.patch",
		prepare: gateWrite(gatePatchDir+"0001-x.patch", "not a patch\n"),
		edit:    gateWrite(gatePatchDir+"0001-x.patch", gateDeployerPatch)},
	{name: "deployer may add a patch next to a responder patch", kind: gateOwnership, actor: Deployer, phase: InitialPhase,
		prepare: gateWrite(gatePatchDir+"0001-x.patch", gateResponderPatch("")),
		edit:    gateWrite(gatePatchDir+"0002-y.patch", gateDeployerPatch)},
	{name: "human may modify any patch", kind: gateOwnership, actor: Human, phase: InitialPhase,
		prepare: gateWrite(gatePatchDir+"0001-x.patch", gateResponderPatch("")),
		edit:    gateWrite(gatePatchDir+"0001-x.patch", gateResponderPatch("Upstream-Fix: issue 7\n"))},

	// {Deployer, AdvancePhase}: the deployment plan, files and patches.
	{name: "deployer advance writes plan, files and patches", kind: gateOwnership, actor: Deployer, phase: AdvancePhase, edit: func(t *testing.T, r string) {
		write(t, r, layout.DeploymentPlanFile, gateValidPlan)
		write(t, r, ".sdo/deployment/values/prod.yaml", "replicas: 2\n")
		write(t, r, ".sdo/deployment/manifests/app/deploy.yaml", "kind: Deployment\n")
		write(t, r, ".sdo/deployment/build/Dockerfile", "FROM scratch\n")
		write(t, r, gatePatchDir+"0001-x.patch", gateDeployerPatch)
	}},
	{name: "deployer advance refreshes its own patch", kind: gateOwnership, actor: Deployer, phase: AdvancePhase,
		prepare: gateWrite(gatePatchDir+"0001-x.patch", gateDeployerPatch),
		edit:    gateWrite(gatePatchDir+"0001-x.patch", gateDeployerPatchWith("Upstream-Fix: issue 7\n"))},
	{name: "deployer advance cannot refresh a responder patch", kind: gateOwnership, actor: Deployer, phase: AdvancePhase, rules: []string{RuleOwnershipDenied}, path: gatePatchDir + "0001-x.patch",
		prepare: gateWrite(gatePatchDir+"0001-x.patch", gateResponderPatch("")),
		edit:    gateWrite(gatePatchDir+"0001-x.patch", gateResponderPatch("Upstream-Fix: issue 7\n"))},
	{name: "deployer advance cannot write the telemetry overlay", kind: gateOwnership, actor: Deployer, phase: AdvancePhase, rules: []string{RuleOwnershipDenied}, path: ".sdo/deployment/overlays/telemetry/o.yaml",
		edit: gateWrite(".sdo/deployment/overlays/telemetry/o.yaml", "a: b\n")},
	{name: "deployer advance cannot change memory", kind: gateOwnership, actor: Deployer, phase: AdvancePhase, rules: []string{RuleOwnershipDenied}, path: layout.GoalFile,
		edit: gateWrite(layout.GoalFile, "---\nschema_version: 1\nowner: human\n---\nNew goal\n")},
}

func init() { gateRows = append(gateRows, deploymentRuleRows...) }

// An accepted tree is validated whole: the deployment rules run on a checked
// repository without a base, with the catalogs the caller wires.
func TestPlanCatalogsRejectUnregisteredRenderers(t *testing.T) {
	root := goodTree(t)
	write(t, root, layout.DeploymentPlanFile, strings.Replace(gateValidPlan, "\"raw\"", "\"jsonnet\"", 1))
	request := Request{Root: root, Layers: Structural | Semantic, PlanCatalogs: planCatalogsFor("raw")}
	if got := ruleSet(diagnose(t, request)); !same(got, []string{RuleDeploymentInvalid}) {
		t.Fatalf("rules = %v", got)
	}
	write(t, root, layout.DeploymentPlanFile, gateValidPlan)
	if got := ruleSet(diagnose(t, request)); len(got) != 0 {
		t.Fatalf("rules = %v", got)
	}
}

type idDecoder []string

func (d idDecoder) Decode(id string, config *anypb.Any) (proto.Message, error) {
	if !slices.Contains(d, id) {
		return nil, fmt.Errorf("unknown renderer %q (registered: %v)", id, []string(d))
	}
	return nil, nil
}

func planCatalogsFor(renderers ...string) planfile.Catalogs {
	return planfile.Catalogs{Renderers: idDecoder(renderers)}
}

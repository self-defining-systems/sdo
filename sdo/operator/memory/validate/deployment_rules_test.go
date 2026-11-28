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

// Deployment class rules: deployment.invalid (the optional override decodes
// strictly) and who may change app files and the override.

const gateValidPlan = "{\"source\":{\"rendererId\":\"raw\",\"config\":{\"@type\":\"type.googleapis.com/sdodev.contracts.v1alpha1.RawRenderConfig\",\"paths\":[\"deploy/sdo/manifests\"]}}}\n"

var deploymentRuleRows = []gateRow{
	{name: "valid plan passes the static rules", kind: gateStatic, edit: gateWrite(layout.DeploymentPlanFile, gateValidPlan)},

	// deployment.invalid: plan.yaml decodes strictly.
	{name: "plan with an unknown field", kind: gateStatic, rules: []string{RuleDeploymentInvalid}, path: layout.DeploymentPlanFile,
		edit: gateWrite(layout.DeploymentPlanFile, strings.Replace(gateValidPlan, "{\"source\"", "{\"colour\":1,\"source\"", 1))},
	// A plan without a source is an override that names only images: the
	// renderer is derived from the files.
	{name: "plan without a source", kind: gateStatic, rules: nil,
		edit: gateWrite(layout.DeploymentPlanFile, "images: []\n")},
	{name: "plan that is not YAML", kind: gateStatic, rules: []string{RuleDeploymentInvalid}, path: layout.DeploymentPlanFile,
		edit: gateWrite(layout.DeploymentPlanFile, "source: [unclosed\n")},
	{name: "plan with duplicate keys", kind: gateStatic, rules: []string{RuleDeploymentInvalid}, path: layout.DeploymentPlanFile,
		edit: gateWrite(layout.DeploymentPlanFile, "images: []\nimages: []\n")},
	{name: "plan that sets the retired strategy", kind: gateStatic, rules: []string{RuleDeploymentInvalid}, path: layout.DeploymentPlanFile,
		edit: gateWrite(layout.DeploymentPlanFile, strings.Replace(gateValidPlan, "{\"source\"", "{\"strategyId\":\"raw\",\"source\"", 1))},
	{name: "plan that lists the retired expected workloads", kind: gateStatic, rules: []string{RuleDeploymentInvalid}, path: layout.DeploymentPlanFile,
		edit: gateWrite(layout.DeploymentPlanFile, strings.Replace(gateValidPlan, "{\"source\"", "{\"expectedWorkloadIds\":[\"Deployment/a/b\"],\"source\"", 1))},
	{name: "plan with a config field its message does not have", kind: gateStatic, rules: []string{RuleDeploymentInvalid}, path: layout.DeploymentPlanFile,
		edit: gateWrite(layout.DeploymentPlanFile, strings.Replace(gateValidPlan, "paths", "pathz", 1))},

	// The deployer commits app files like an engineer and may write the override.
	{name: "deployer commits app files and the override", kind: gateOwnership, actor: Deployer, phase: InitialPhase, edit: func(t *testing.T, r string) {
		write(t, r, layout.DeploymentPlanFile, gateValidPlan)
		write(t, r, "deploy/sdo/manifests/app.yaml", "kind: Deployment\n")
		write(t, r, "Dockerfile.api", "FROM scratch\n")
	}},
	// .sdo/deployment holds only the override: the retired views are unclassified.
	{name: "deployer cannot write a retired deployment view", kind: gateOwnership, actor: Deployer, phase: InitialPhase, rules: []string{RuleOwnershipDenied}, path: ".sdo/deployment/values/prod.yaml",
		edit: gateWrite(".sdo/deployment/values/prod.yaml", "replicas: 2\n")},
	{name: "deployer cannot write a patch", kind: gateOwnership, actor: Deployer, phase: InitialPhase, rules: []string{RuleOwnershipDenied}, path: ".sdo/deployment/patches/0001-x.patch",
		edit: gateWrite(".sdo/deployment/patches/0001-x.patch", "SDO-Patch: 1\n")},
	{name: "deployer cannot change memory", kind: gateOwnership, actor: Deployer, phase: InitialPhase, rules: []string{RuleOwnershipDenied}, path: layout.GoalFile,
		edit: gateWrite(layout.GoalFile, "---\nschema_version: 1\nowner: human\n---\nNew goal\n")},
	{name: "human writes the override", kind: gateOwnership, actor: Human, phase: InitialPhase, edit: gateWrite(layout.DeploymentPlanFile, gateValidPlan)},
	// Go commits a program's source proposal in the reflection checkout as the responder.
	{name: "reflection commits an app file", kind: gateOwnership, actor: Responder, phase: ReflectionPhase, edit: gateWrite("k8s/web.yaml", "kind: Deployment\n")},
	{name: "the health judge cannot change app files", kind: gateOwnership, actor: HealthJudge, phase: InitialPhase, rules: []string{RuleOwnershipDenied}, path: "k8s/web.yaml",
		edit: gateWrite("k8s/web.yaml", "kind: Deployment\n")},
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

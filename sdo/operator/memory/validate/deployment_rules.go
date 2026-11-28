package validate

import (
	"os"
	"path/filepath"

	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/planfile"
)

// deploymentRules checks the optional deployment override with the parser the
// deployment pipeline uses, so a candidate that passes here is one the
// deployer can read.
func (s *run) deploymentRules() {
	data, err := os.ReadFile(filepath.Join(s.req.Root, filepath.FromSlash(layout.DeploymentPlanFile)))
	if err != nil {
		return
	}
	if _, err := planfile.Parse(data, s.req.PlanCatalogs); err != nil {
		s.add(Semantic, Diagnostic{Rule: RuleDeploymentInvalid, Path: layout.DeploymentPlanFile, Message: err.Error(),
			Hint: "plan.yaml is the optional deployment override, a DeploymentPlan with only source (rendererId, config with @type) and images; delete it to derive the deployment from the repository's files"})
	}
}

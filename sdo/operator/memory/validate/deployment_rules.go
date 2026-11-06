package validate

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"sdo.dev/operator/memory/internal/memoryio"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/patchset"
	"sdo.dev/operator/planfile"
)

// deploymentRules checks the deployment class with the same parsers the
// deployment pipeline uses: plan.yaml decodes through planfile and patches
// parse through patchset, so a candidate that passes here is one the effective
// tree can read.
func (s *run) deploymentRules(ctx context.Context) error {
	s.planRule()
	return s.patchRules(ctx)
}

func (s *run) planRule() {
	data, err := os.ReadFile(filepath.Join(s.req.Root, filepath.FromSlash(layout.DeploymentPlanFile)))
	if err != nil {
		return
	}
	if _, err := planfile.Parse(data, s.req.PlanCatalogs); err != nil {
		s.add(Semantic, Diagnostic{Rule: RuleDeploymentInvalid, Path: layout.DeploymentPlanFile, Message: err.Error(),
			Hint: "plan.yaml is a DeploymentPlan: source (rendererId, config with @type), images and expectedWorkloadIds; the plan is written by SDO, so rerun the deployment step rather than editing it"})
	}
}

func (s *run) patchRules(ctx context.Context) error {
	entries, err := os.ReadDir(filepath.Join(s.req.Root, filepath.FromSlash(layout.DeploymentPatchesDir)))
	if err != nil {
		return nil
	}
	var files []patchset.File
	var parsed []patchset.Patch
	failed := false
	for _, entry := range entries {
		rel := layout.DeploymentPatchesDir + "/" + entry.Name()
		if kind, _, ok := layout.Classify(rel); !ok || kind != layout.KindDeploymentPatch || !entry.Type().IsRegular() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(s.req.Root, filepath.FromSlash(rel)))
		if err != nil {
			return err
		}
		files = append(files, patchset.File{Name: entry.Name(), Data: data})
		patch, err := patchset.Parse(entry.Name(), data)
		if err != nil {
			failed = true
			s.add(Semantic, Diagnostic{Rule: RulePatchInvalid, Path: rel, Message: err.Error(), Hint: patchHint})
			continue
		}
		parsed = append(parsed, patch)
	}
	if !failed {
		if _, err := patchset.ParseSeries(files); err != nil {
			at := layout.DeploymentPatchesDir
			var pe *patchset.PatchError
			if errors.As(err, &pe) && pe.Patch != "" {
				at += "/" + pe.Patch
			}
			s.add(Semantic, Diagnostic{Rule: RulePatchInvalid, Path: at, Message: err.Error(), Hint: patchHint})
		}
	}
	for _, patch := range parsed {
		rel := layout.DeploymentPatchesDir + "/" + patch.Name
		if s.req.Base != "" && !slices.Contains(s.changed, rel) {
			continue
		}
		if actor := s.req.Actor; (actor == Deployer || actor == Responder) && string(patch.Header.Owner) != string(actor) {
			s.add(Semantic, Diagnostic{Rule: RulePatchOwner, Path: rel, Message: fmt.Sprintf("actor %s wrote a patch whose Owner header is %s", actor, patch.Header.Owner), Hint: fmt.Sprintf("set `Owner: %s` in the patch header", actor)})
		}
		if base := patch.Header.UpstreamBase; base != "" {
			known, err := s.knownCommit(ctx, base)
			if err != nil {
				return err
			}
			if !known {
				s.add(Semantic, Diagnostic{Rule: RulePatchUpstreamBase, Path: rel, Message: fmt.Sprintf("Upstream-Base %s is not a commit of this repository", base), Hint: "name the upstream revision the patch was written against, or drop the Upstream-Base line and let SDO fill it"})
			}
		}
	}
	return nil
}

const patchHint = "a patch is a header (SDO-Patch: 1, Owner, Reason of 40 to 2000 characters, Incident for responders) followed by `git diff` output for files outside .sdo/; see `sdo-operator patch create`"

// knownCommit reports whether sha names a commit in the checkout. A git exit
// status means the object is absent; any other failure is infrastructure.
func (s *run) knownCommit(ctx context.Context, sha string) (bool, error) {
	_, err := git(ctx, s.req.Root, "rev-parse", "--verify", "--quiet", sha+"^{commit}")
	if err == nil {
		return true, nil
	}
	if memoryio.Exited(err) {
		return false, nil
	}
	return false, err
}

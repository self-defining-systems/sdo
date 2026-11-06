// Transitional: translates a new-form DeploymentPlan down to the legacy
// RawManifestStrategy so the worker can switch plans before deployment moves
// to the render seam. Deleted by S3-13b.
//
//archcheck:ratchet R5 count=1 until=S3-13b
//archcheck:allow R4 until=S3-13b reason="core decodes and builds plugin config types to translate a new-form plan to the legacy strategy"
package lifecycle

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/types/known/anypb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

const (
	legacyRawStrategyID = "raw-manifests-v1"
	rawRendererID       = "raw"
	buildkitSourceID    = "buildkit"
	prebuiltSourceID    = "prebuilt"
)

// admitPlan returns the plan the legacy deployment registry executes. A plan
// without source is the legacy form and passes through. A plan with source is
// translated down; renderers and image sources that have no legacy equivalent
// are rejected with feedback that says what is supported.
func admitPlan(plan *contracts.DeploymentPlan) (*contracts.DeploymentPlan, error) {
	if plan == nil {
		return nil, errors.New("deployment plan is missing")
	}
	if plan.GetSource() == nil {
		if plan.GetStrategyId() == "" || plan.GetStrategyConfig() == nil {
			return nil, errors.New("deployment plan names neither source nor strategy")
		}
		return plan, nil
	}
	if plan.GetStrategyId() != "" || plan.GetStrategyConfig() != nil {
		return nil, errors.New("deployment plan sets both source and strategy; use source")
	}
	if id := plan.GetSource().GetRendererId(); id != rawRendererID {
		return nil, fmt.Errorf("deployment renderer %q is not supported yet: the renderer must be %q", id, rawRendererID)
	}
	if plan.GetSource().GetConfig() == nil {
		return nil, errors.New("deployment source config is required")
	}
	raw := &contracts.RawRenderConfig{}
	if err := plan.GetSource().GetConfig().UnmarshalTo(raw); err != nil {
		return nil, fmt.Errorf("decode raw render config: %w", err)
	}
	if len(raw.GetPaths()) == 0 {
		return nil, errors.New("raw deployment source names no paths")
	}
	strategy := &contracts.RawManifestStrategy{ManifestPaths: append([]string(nil), raw.GetPaths()...), ExpectedWorkloadIds: append([]string(nil), plan.GetExpectedWorkloadIds()...)}
	for _, image := range plan.GetImages() {
		switch id := image.GetSourceId(); id {
		case buildkitSourceID:
			if image.GetConfig() == nil {
				return nil, fmt.Errorf("image %q: buildkit config is required", image.GetImage())
			}
			build := &contracts.BuildKitImageConfig{}
			if err := image.GetConfig().UnmarshalTo(build); err != nil {
				return nil, fmt.Errorf("decode buildkit config for image %q: %w", image.GetImage(), err)
			}
			strategy.ImageBuilds = append(strategy.ImageBuilds, &contracts.ImageBuild{Image: image.GetImage(), ContextPath: build.GetContextPath(), DockerfilePath: build.GetDockerfilePath()})
		case prebuiltSourceID:
		default:
			return nil, fmt.Errorf("image %q: image source %q is not supported yet: the source must be %q or %q", image.GetImage(), id, buildkitSourceID, prebuiltSourceID)
		}
	}
	config, err := anypb.New(strategy)
	if err != nil {
		return nil, err
	}
	return &contracts.DeploymentPlan{StrategyId: legacyRawStrategyID, StrategyConfig: config}, nil
}

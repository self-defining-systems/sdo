package rendered

import (
	"fmt"
	"path"

	"google.golang.org/protobuf/types/known/anypb"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/render"
)

// LegacyRawSource converts the transitional --deployed-manifest-path list into
// the raw DeploymentSource it always meant. No paths is no source (nil), which
// reads report as SourceUnconfigured. Transitional: removed with the path flag
// (S3-11c), together with legacyReadSource.
func LegacyRawSource(paths []string) (*contracts.DeploymentSource, error) {
	if len(paths) == 0 {
		return nil, nil
	}
	config, err := anypb.New(&contracts.RawRenderConfig{Paths: append([]string(nil), paths...)})
	if err != nil {
		return nil, fmt.Errorf("encode raw deployment source: %w", err)
	}
	return &contracts.DeploymentSource{RendererId: "raw", Config: config}, nil
}

// legacyReadSource keeps reads of a path-list source that names exactly one
// kustomize root (a kustomization file, or a directory holding one) working
// until every path list is replaced by a DeploymentSource: the old reader
// built such a root with kustomize, and SREGym's hotelReservation selects
// kubernetes/, which holds a kustomization. Every other source is returned
// unchanged, so a raw selection mixing a root with other manifests still fails
// with "use renderer kustomize" (D3). Reads only: deploy never translates.
func legacyReadSource(fsys render.FS, src *contracts.DeploymentSource) (*contracts.DeploymentSource, error) {
	if src.GetRendererId() != "raw" {
		return src, nil
	}
	var config contracts.RawRenderConfig
	if err := src.GetConfig().UnmarshalTo(&config); err != nil || len(config.GetPaths()) != 1 {
		return src, nil
	}
	selected := config.GetPaths()[0]
	dir := selected
	if !kustomizationName(path.Base(selected)) {
		found := false
		for _, name := range []string{"kustomization.yaml", "kustomization.yml", "Kustomization"} {
			// The tree listing is loaded, so Stat does no I/O; an invalid
			// selection is left for the raw renderer to reject.
			if _, err := fsys.Stat(path.Join(selected, name)); err == nil {
				found = true
				break
			}
		}
		if !found {
			return src, nil
		}
	} else {
		dir = path.Dir(selected)
	}
	if dir == "." {
		dir = ""
	}
	kustomize, err := anypb.New(&contracts.KustomizeRenderConfig{Path: dir})
	if err != nil {
		return nil, fmt.Errorf("encode kustomize deployment source: %w", err)
	}
	return &contracts.DeploymentSource{RendererId: "kustomize", Config: kustomize}, nil
}

func kustomizationName(base string) bool {
	return base == "kustomization.yaml" || base == "kustomization.yml" || base == "Kustomization"
}

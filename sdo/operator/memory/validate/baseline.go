package validate

import (
	"context"
	"path/filepath"

	"sdo.dev/operator/memory/layout"
)

func candidateHasIncidentDetector(candidate string) (bool, error) {
	detectors, err := layout.ReadDetectors(filepath.Join(candidate, layout.DiagnosticsDir))
	if err != nil {
		return false, err
	}
	for _, detector := range detectors {
		if detector.Class == layout.ClassIncident {
			return true, nil
		}
	}
	return false, nil
}

// BaselineGateRequired selects the healthy replay gate for both transports.
func BaselineGateRequired(candidate string, changedPaths []string) (bool, error) {
	// Promoted detectors execute playbook programs and shared libraries too.
	// Reuse rollout dependency classification so editing those files cannot
	// silently drop the healthy replay input on either acceptance transport.
	if layout.RequiresControllerRollout(changedPaths) {
		return candidateHasIncidentDetector(candidate)
	}
	return false, nil
}

func selectCompileBaseline(ctx context.Context, request Request) (Request, error) {
	if request.HealthyBaseline != "" {
		paths, err := ChangedPaths(ctx, request.Root, request.Base)
		if err != nil {
			return Request{}, err
		}
		needed, err := BaselineGateRequired(request.Root, paths)
		if err != nil {
			return Request{}, err
		}
		if !needed {
			request.HealthyBaseline = ""
		}
	}
	return request, nil
}

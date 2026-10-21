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
	for _, path := range changedPaths {
		if kind, _, ok := layout.Classify(path); ok && (kind == layout.KindIncidentDetectorManifest || kind == layout.KindIncidentDetectorSource) {
			return candidateHasIncidentDetector(candidate)
		}
	}
	return false, nil
}

func compileRequest(ctx context.Context, request Request) (Request, error) {
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

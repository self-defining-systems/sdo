package validate

import (
	"context"
	"os"
	"path/filepath"

	"sdo.dev/operator/memory"
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
		return request, nil
	}
	if request.Actor == Responder && request.Phase == ReflectionPhase {
		capture := request.CaptureDirectory
		if capture == "" && request.CaptureRoot != "" {
			capture, err = memory.IncidentCorpusDir(request.CaptureRoot, request.IncidentID)
			if err != nil {
				return Request{}, err
			}
		}
		request.HealthyBaseline = filepath.Join(capture, "healthy")
		if capture == "" {
			return Request{}, &Failure{Diagnostics: []Diagnostic{healthyCaptureDiagnostic("controller pre-incident healthy capture is unavailable")}}
		}
		if _, err := os.Stat(filepath.Join(request.HealthyBaseline, "healthy.json")); err != nil {
			return Request{}, &Failure{Diagnostics: []Diagnostic{healthyCaptureDiagnostic("controller pre-incident healthy capture is unavailable")}}
		}
	}
	return request, nil
}

func healthyCaptureDiagnostic(message string) Diagnostic {
	return Diagnostic{Rule: RuleDetectorHealthyBaseline, Message: message, Hint: "the controller records this sample before the incident, or at a closure it verified healthy; agent-authored healthy.json cannot supply it"}
}

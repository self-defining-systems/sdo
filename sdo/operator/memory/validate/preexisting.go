package validate

import (
	"context"
	"slices"
	"strings"

	"sdo.dev/operator/memory/layout"
)

// preexistingHint tells the proposing agent the fault is not its work.
const preexistingHint = "pre-existing in accepted memory: this candidate changed neither the detector nor .sdo/lib or other shared diagnostics code, so it neither caused nor can fix this; recorded as a note, not a rejection; do not edit the detector to silence it"

// separatePreexisting moves healthy-baseline faults of detectors the candidate
// did not change into non-blocking notes. Non-test incident detector code may
// import only .sdo/lib within memory, so an unchanged detector directory, an
// unchanged .sdo/lib and unchanged shared diagnostics files (go.mod, traffic)
// mean the accepted base runs the same bytes on the same recorded sample and
// fails the same way: the fault belongs to accepted memory or to the sample,
// and no edit by this candidate can fix it. Every other diagnostic, including
// a fault on a detector the candidate changed, still rejects.
func separatePreexisting(ctx context.Context, request Request, diagnostics []Diagnostic) ([]Diagnostic, error) {
	if !slices.ContainsFunc(diagnostics, attributableToDetector) || request.Base == "" {
		return diagnostics, nil
	}
	changed, err := ChangedPaths(ctx, request.Root, request.Base)
	if err != nil {
		return nil, err
	}
	blocking := make([]Diagnostic, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		if !attributableToDetector(diagnostic) || candidateAffectsDetector(changed, diagnostic.Path) {
			blocking = append(blocking, diagnostic)
			continue
		}
		diagnostic.Message = "pre-existing (unchanged since " + request.Base + "): " + diagnostic.Message
		diagnostic.Hint = preexistingHint
		request.noteGaps(diagnostic)
	}
	return blocking, nil
}

func attributableToDetector(diagnostic Diagnostic) bool {
	return diagnostic.Rule == RuleDetectorHealthyBaseline && strings.HasPrefix(diagnostic.Path, layout.DetectorsDir+"/")
}

// candidateAffectsDetector reports whether a changed path can change what the
// detector in directory computes: its own files, the shared library, or any
// diagnostics file outside the per-detector directories.
func candidateAffectsDetector(changed []string, detectorDir string) bool {
	for _, path := range changed {
		switch {
		case strings.HasPrefix(path, detectorDir+"/"),
			strings.HasPrefix(path, layout.LibrariesDir+"/"),
			strings.HasPrefix(path, layout.DiagnosticsDir+"/") && !strings.HasPrefix(path, layout.DetectorsDir+"/"):
			return true
		}
	}
	return false
}

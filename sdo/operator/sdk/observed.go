package sdk

import (
	"context"
	"fmt"
)

// DetectObserved prevents an unreadable declared dependency from clearing a
// finding, even if the detector ignores an accessor error.
func DetectObserved(ctx context.Context, detector Detector, view DetectionContext) ([]Finding, error) {
	source, ok := view.(ScopedObservationSource)
	for _, w := range detector.Spec().Watches {
		cluster := w.Kind == "MutatingWebhookConfiguration" || w.Kind == "ValidatingWebhookConfiguration"
		if !cluster && (w.Namespace == "" || w.Namespace == view.Namespace()) {
			continue
		}
		var statuses []ObservationStatus
		if ok {
			statuses = source.ObservationStatuses()
		}
		matched := false
		for _, status := range statuses {
			if status.Target.APIVersion == w.APIVersion && status.Target.Kind == w.Kind && status.Target.Namespace == w.Namespace {
				matched = true
				if status.State != ObservationObserved {
					return nil, fmt.Errorf("observation dependency unavailable: %s", status.Target.Key())
				}
			}
		}
		if !matched {
			return nil, fmt.Errorf("observation dependency undeclared: %s/%s/%s", w.APIVersion, w.Kind, w.Namespace)
		}
	}
	return detector.Detect(ctx, view)
}

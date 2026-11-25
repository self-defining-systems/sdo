package sdk

import "strings"

// HeldAlertSource is implemented by detection contexts that record the alert
// keys of the incident open when the snapshot was taken. A key is the
// detector ID and the finding fingerprint joined by NUL. Capture records the
// keys, so replay judges a held finding exactly as the controller did.
type HeldAlertSource interface {
	HeldAlertKeys() []string
}

// HeldFingerprints returns the fingerprints of the detector's findings that
// hold the open incident (its opening and joined alerts); empty when no
// incident is open or the context carries none.
func HeldFingerprints(ctx DetectionContext, detectorID string) map[string]bool {
	source, ok := ctx.(HeldAlertSource)
	if !ok {
		return nil
	}
	var held map[string]bool
	for _, key := range source.HeldAlertKeys() {
		id, fingerprint, ok := strings.Cut(key, "\x00")
		if !ok || id != detectorID {
			continue
		}
		if held == nil {
			held = map[string]bool{}
		}
		held[fingerprint] = true
	}
	return held
}

// MetaFirstFailureAt is the finding metadata key (RFC 3339) of the first
// failure observed in the episode a user-facing alert reports.
const MetaFirstFailureAt = "first_failure_at"

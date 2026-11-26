package sdktest

import (
	"fmt"
	"slices"
	"strings"
)

// MaxLateCaptures bounds the open captures the controller takes after an
// incident opened, when a new finding shows that something changed.
const MaxLateCaptures = 5

// LateCapturePhases are the later open captures in the order they are taken.
func LateCapturePhases() []string {
	phases := make([]string, 0, MaxLateCaptures)
	for i := 1; i <= MaxLateCaptures; i++ {
		phases = append(phases, fmt.Sprintf("open@late-%d", i))
	}
	return phases
}

// OpenCapturePhases are every open capture of an incident, opening ones first:
// at detection, after settle, before the first plan application, then later.
func OpenCapturePhases() []string {
	return append([]string{"open@detect", "open@settle", "open@pre-apply"}, LateCapturePhases()...)
}

// IsOpenCapturePhase reports whether phase (with or without its .json suffix)
// names one of the incident's open captures.
func IsOpenCapturePhase(phase string) bool {
	return slices.Contains(OpenCapturePhases(), strings.TrimSuffix(phase, ".json"))
}

// IsLateCapture reports whether name (a phase or its file) is a later capture.
func IsLateCapture(name string) bool {
	return slices.Contains(LateCapturePhases(), strings.TrimSuffix(name, ".json"))
}

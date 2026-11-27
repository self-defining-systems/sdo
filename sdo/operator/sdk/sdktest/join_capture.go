package sdktest

import (
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// joinCapturePhase is the open state an alert joined an open incident in: the
// capture of the nth join pass is open@join-n. A fault that starts while the
// incident is open shows only there.
var joinCapturePhase = regexp.MustCompile(`^join-[1-9][0-9]*$`)

// IsJoinCapturePhase reports whether phase (without the "open@" prefix) names
// an alert-join capture.
func IsJoinCapturePhase(phase string) bool { return joinCapturePhase.MatchString(phase) }

// JoinCapturePhase is the phase of the nth alert-join capture.
func JoinCapturePhase(n int) string { return "join-" + strconv.Itoa(n) }

// JoinCapturePhases lists the alert-join captures in an incident capture
// directory, in the order they were taken.
func JoinCapturePhases(dir string) []string {
	paths, _ := filepath.Glob(filepath.Join(dir, "open@join-*.json"))
	var phases []string
	for _, path := range paths {
		phase := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(path), "open@"), ".json")
		if IsJoinCapturePhase(phase) {
			phases = append(phases, phase)
		}
	}
	sort.Slice(phases, func(i, j int) bool {
		a, _ := strconv.Atoi(strings.TrimPrefix(phases[i], "join-"))
		b, _ := strconv.Atoi(strings.TrimPrefix(phases[j], "join-"))
		return a < b
	})
	return phases
}

// JoinCaptureContext is what Go knew when it took an alert-join capture
// (open@join-n). Unlike the opening captures, a join capture can follow SDO's
// or the agent's writes, so a Diagnose that matches it may be matching state a
// write made rather than the fault. It is evidence for readers, never a gate.
type JoinCaptureContext struct {
	// JoinedKeys are the alerts that joined the incident in this pass.
	JoinedKeys []string `json:"joined_keys"`
	// KnownWritesBefore is true when a write Go knows of preceded the
	// capture. False means none known, not none made: with the agent recording
	// off, the agent's direct Kubernetes writes are invisible.
	KnownWritesBefore bool `json:"known_writes_before"`
	// WriteSources name where the known writes came from: "automatic_stage"
	// (the deterministic stage's plan applications), "responder_plan" (plans
	// the responder applied through SDO), "agent_recording" (the agent's
	// recorded Kubernetes writes) and "repair_action" (a successful repair
	// receipt in the responder's reported result).
	WriteSources []string `json:"write_sources,omitempty"`
	// Recording is whether the agent's recorded Kubernetes writes were
	// readable for this capture (the recording on and a responder running).
	Recording bool `json:"recording"`
}

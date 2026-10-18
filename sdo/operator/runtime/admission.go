package runtime

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Load-aware incident admission gives the controller explicit, deterministic
// behavior under resource pressure. Without it a ready finding batch always
// opened an incident immediately and raced the (possibly congested) close-out /
// worktree-drain of the prior incident; under sustained host load that race
// widened into strict-receipt and stranded-worktree anomalies. Admission instead
// *defers* a ready batch when the system is over a threshold -- it holds the
// incident (which keeps its identity and still coalesces later findings) rather
// than dropping it -- and resumes deterministically once pressure clears, with
// hysteresis so the decision cannot flap. Every decision is observable through
// the incident lifecycle stream (admission_deferred / admission_resumed).
//
// The gate reads two independent pressure terms: an injected host-load gauge and
// the controller's own stranded-worktree release backlog. Both are optional and
// default off, so a controller with no admission config admits exactly as before.

// admissionDeferredState is the dispatch state of an incident that has been
// opened but is held before its worktree is prepared or its responder is
// dispatched, because admission is deferred under load. It is a stable state (no
// goroutine is in flight), so it persists across a restart unchanged and the
// controller re-samples on the next step.
const admissionDeferredState = "admission_deferred"

// Admission-decision reasons recorded on the lifecycle events. They name a
// controller-internal load fact, never a benchmark verdict.
const (
	admissionReasonLoad    = "load_above_high_watermark"
	admissionReasonBacklog = "release_backlog_above_high_watermark"
	admissionReasonCleared = "below_low_watermark"
)

// LoadSample is one reading from a LoadGauge. Pressure is in the gauge's own
// units; AdmissionConfig watermarks are compared against it directly
// (ProcPressureGauge reports Linux PSI "some avg10" as a 0-100 percentage).
type LoadSample struct {
	Pressure float64
}

// LoadGauge reports host resource pressure. It is the controller's one seam onto
// host load; a test injects a scripted gauge and a fake clock so an admission
// trajectory is fully deterministic. Sample must not call an LLM or read any
// benchmark state.
type LoadGauge interface {
	Sample() (LoadSample, error)
}

// AdmissionConfig configures load-aware incident admission. The zero value is
// disabled, which admits every ready batch immediately -- identical to the
// controller's pre-admission behavior.
type AdmissionConfig struct {
	// Enabled turns the gate on. When false every other field is ignored.
	Enabled bool
	// HighWatermark / LowWatermark bound the host-load term. A ready batch is
	// deferred at or above HighWatermark and a deferred incident resumes below
	// LowWatermark. Zero HighWatermark disables the load term (the backlog term
	// may still apply). LowWatermark must not exceed HighWatermark.
	HighWatermark float64
	LowWatermark  float64
	// ReleaseBacklogHighWatermark / ReleaseBacklogLowWatermark apply the same
	// hysteresis to the controller's stranded-worktree release backlog
	// (len(pendingReleases)), so a congested drain path defers new admission.
	// Zero high watermark disables the backlog term.
	ReleaseBacklogHighWatermark int
	ReleaseBacklogLowWatermark  int
	// RecheckInterval bounds how long a deferred incident waits before admission
	// is re-evaluated against a fresh sample. Must be positive when enabled.
	RecheckInterval time.Duration
}

// validate enforces the admission invariants, raising on a misconfiguration
// rather than silently admitting or wedging. Called from NewController.
func (cfg AdmissionConfig) validate() error {
	if !cfg.Enabled {
		return nil
	}
	if cfg.HighWatermark < 0 || cfg.LowWatermark < 0 {
		return fmt.Errorf("admission watermarks must not be negative")
	}
	if cfg.ReleaseBacklogHighWatermark < 0 || cfg.ReleaseBacklogLowWatermark < 0 {
		return fmt.Errorf("admission release-backlog watermarks must not be negative")
	}
	loadTerm := cfg.HighWatermark > 0
	backlogTerm := cfg.ReleaseBacklogHighWatermark > 0
	if !loadTerm && !backlogTerm {
		return fmt.Errorf("admission control is enabled but no high watermark is set")
	}
	if loadTerm && cfg.LowWatermark > cfg.HighWatermark {
		return fmt.Errorf("admission low watermark %v must not exceed high watermark %v", cfg.LowWatermark, cfg.HighWatermark)
	}
	if backlogTerm && cfg.ReleaseBacklogLowWatermark > cfg.ReleaseBacklogHighWatermark {
		return fmt.Errorf(
			"admission release-backlog low watermark %d must not exceed high watermark %d",
			cfg.ReleaseBacklogLowWatermark, cfg.ReleaseBacklogHighWatermark,
		)
	}
	if cfg.RecheckInterval <= 0 {
		return fmt.Errorf("admission recheck interval must be positive when admission control is enabled")
	}
	return nil
}

// admissionAssessment is one admission decision plus the facts that drove it, so
// the caller can record a self-describing lifecycle event.
type admissionAssessment struct {
	// Defer is true when a ready batch must be held (or a deferred incident kept
	// held); false means admit (or, for a deferred incident, resume).
	Defer bool
	// Pressure and ReleaseBacklog are the readings the decision used.
	Pressure       float64
	ReleaseBacklog int
	// Threshold is the governing load watermark when the load term decided; it is
	// zero when the backlog term decided (ReleaseBacklog then tells the story).
	Threshold float64
	// Reason names the governing term.
	Reason string
}

// assess is the pure admission decision. currentlyDeferred selects the watermark:
// a not-yet-deferred batch is held at the high watermark; a deferred incident
// resumes only when every enabled term is below its low watermark (hysteresis).
func (cfg AdmissionConfig) assess(currentlyDeferred bool, sample LoadSample, releaseBacklog int) admissionAssessment {
	result := admissionAssessment{Pressure: sample.Pressure, ReleaseBacklog: releaseBacklog}
	if !cfg.Enabled {
		return result
	}
	loadTerm := cfg.HighWatermark > 0
	backlogTerm := cfg.ReleaseBacklogHighWatermark > 0
	if currentlyDeferred {
		loadClear := !loadTerm || sample.Pressure < cfg.LowWatermark
		backlogClear := !backlogTerm || releaseBacklog < cfg.ReleaseBacklogLowWatermark
		if loadClear && backlogClear {
			result.Reason = admissionReasonCleared
			if loadTerm {
				result.Threshold = cfg.LowWatermark
			}
			return result
		}
		result.Defer = true
		if loadTerm && sample.Pressure >= cfg.LowWatermark {
			result.Threshold = cfg.LowWatermark
			result.Reason = admissionReasonLoad
		} else {
			result.Reason = admissionReasonBacklog
		}
		return result
	}
	if loadTerm && sample.Pressure >= cfg.HighWatermark {
		result.Defer = true
		result.Threshold = cfg.HighWatermark
		result.Reason = admissionReasonLoad
		return result
	}
	if backlogTerm && releaseBacklog >= cfg.ReleaseBacklogHighWatermark {
		result.Defer = true
		result.Reason = admissionReasonBacklog
		return result
	}
	return result
}

// ProcPressureGauge reads a Linux PSI pressure file (default /proc/pressure/cpu)
// and reports the "some avg10" field -- the share of the last 10 seconds some
// task was stalled on the resource, a 0-100 percentage. It is deterministic given
// a file (tests point Path at a temp file), reads no benchmark state, and calls
// no LLM. On a kernel without PSI the file is absent; Sample then reports zero
// pressure so admission never defers on a host that cannot measure load.
type ProcPressureGauge struct {
	// Path is the PSI file to read; empty means /proc/pressure/cpu.
	Path string
}

const defaultPressurePath = "/proc/pressure/cpu"

func (g ProcPressureGauge) Sample() (LoadSample, error) {
	path := g.Path
	if path == "" {
		path = defaultPressurePath
	}
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return LoadSample{}, nil
	}
	if err != nil {
		return LoadSample{}, fmt.Errorf("open pressure file %s: %w", path, err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "some ") {
			continue
		}
		value, err := parsePressureAvg10(line)
		if err != nil {
			return LoadSample{}, err
		}
		return LoadSample{Pressure: value}, nil
	}
	if err := scanner.Err(); err != nil {
		return LoadSample{}, fmt.Errorf("read pressure file %s: %w", path, err)
	}
	return LoadSample{}, nil
}

// parsePressureAvg10 extracts the avg10 field from a PSI "some" line, e.g.
// "some avg10=1.23 avg60=0.45 avg300=0.12 total=1234".
func parsePressureAvg10(line string) (float64, error) {
	for _, field := range strings.Fields(line) {
		name, value, ok := strings.Cut(field, "=")
		if !ok || name != "avg10" {
			continue
		}
		parsed, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return 0, fmt.Errorf("parse pressure avg10 %q: %w", value, err)
		}
		return parsed, nil
	}
	return 0, fmt.Errorf("pressure line missing avg10: %q", line)
}

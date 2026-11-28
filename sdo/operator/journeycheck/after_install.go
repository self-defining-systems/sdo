package journeycheck

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/sdk/traffic"
)

// The installed soak is the healthy run done by the controller after the
// onboarding session ended, instead of by the session: the installed
// controller's prober runs the committed probes on their schedule while its
// startup period holds every opener, and the run is judged with Soaked, the
// same judgement as `onboarding journeys --soak`. Only on red does the
// onboarding session get one follow-up turn, SoakFollowUp.

// SoakConfigMap is the control-namespace ConfigMap holding the install's
// SoakRecord under SoakDataKey. It is written once: an install soaks once.
const (
	SoakConfigMap = "sdo-onboarding-soak"
	SoakDataKey   = "soak.json"
)

// SoakRecord is the result of an install's soak.
type SoakRecord struct {
	BeganAt  time.Time        `json:"began_at"`
	EndedAt  time.Time        `json:"ended_at"`
	Duration traffic.Duration `json:"duration"`
	// Results has one healthy-run line per scenario sampled, sorted, then
	// the full journey checks run beside the soak.
	Results []Result `json:"results"`
}

// Red is the record's red lines.
func (r SoakRecord) Red() []Result {
	var red []Result
	for _, result := range r.Results {
		if result.Status == Red {
			red = append(red, result)
		}
	}
	return red
}

// InstalledSoak collects every health-probe sample from began on, once,
// from the controller's bounded and sliding prober windows.
type InstalledSoak struct {
	began    time.Time
	duration time.Duration
	windows  map[string]*soakedWorkload
}

type soakedWorkload struct {
	workload  traffic.Workload
	scenarios map[string]*soakScenario
}

type soakScenario struct {
	observed               traffic.ScenarioObservations
	seen                   map[soakKey]bool
	seenConfirming         map[soakKey]bool
	samples, confirmations []traffic.Sample
}

type soakKey struct {
	at        time.Time
	iteration uint64
}

// NewInstalledSoak starts a soak at began that lasts duration.
func NewInstalledSoak(began time.Time, duration time.Duration) *InstalledSoak {
	return &InstalledSoak{began: began, duration: duration, windows: map[string]*soakedWorkload{}}
}

// Until is when the soak ends.
func (s *InstalledSoak) Until() time.Time { return s.began.Add(s.duration) }

// Due reports whether the soak's time is up at now.
func (s *InstalledSoak) Due(now time.Time) bool { return !now.Before(s.Until()) }

// Observe adds the samples of the health-probe windows taken in the soak and
// not seen before.
func (s *InstalledSoak) Observe(windows map[string]traffic.Window) {
	for name, window := range windows {
		if window.Workload.Purpose != traffic.PurposeHealthProbe {
			continue
		}
		w := s.windows[name]
		if w == nil {
			w = &soakedWorkload{scenarios: map[string]*soakScenario{}}
			s.windows[name] = w
		}
		w.workload = window.Workload
		for _, o := range window.Scenarios {
			scenario := w.scenarios[o.Scenario.ID]
			if scenario == nil {
				scenario = &soakScenario{seen: map[soakKey]bool{}, seenConfirming: map[soakKey]bool{}}
				w.scenarios[o.Scenario.ID] = scenario
			}
			scenario.observed = o
			scenario.samples = s.add(scenario.samples, scenario.seen, o.Samples)
			scenario.confirmations = s.add(scenario.confirmations, scenario.seenConfirming, o.Confirming)
		}
	}
}

func (s *InstalledSoak) add(kept []traffic.Sample, seen map[soakKey]bool, samples []traffic.Sample) []traffic.Sample {
	for _, sample := range samples {
		key := soakKey{sample.At, sample.Iteration}
		if sample.At.Before(s.began) || seen[key] {
			continue
		}
		seen[key] = true
		kept = append(kept, sample)
	}
	sort.SliceStable(kept, func(i, j int) bool { return kept[i].At.Before(kept[j].At) })
	return kept
}

// Judge judges every sample collected, as Soaked judges a healthy run.
func (s *InstalledSoak) Judge(now time.Time) SoakRecord {
	windows := map[string]traffic.Window{}
	for name, w := range s.windows {
		window := traffic.Window{Workload: w.workload, ObservedAt: now}
		for _, scenario := range w.scenarios {
			o := scenario.observed
			o.Samples, o.Confirming = scenario.samples, scenario.confirmations
			o.Qualified = false
			for _, sample := range o.Samples {
				o.Qualified = o.Qualified || sample.Outcome == traffic.OutcomeOK
			}
			window.Scenarios = append(window.Scenarios, o)
		}
		windows[name] = window
	}
	return SoakRecord{BeganAt: s.began, EndedAt: now, Duration: traffic.Duration(s.duration), Results: Soaked(windows)}
}

// soakFollowUpBudget bounds the red lines of the follow-up message, in bytes.
const soakFollowUpBudget = 1536

// SoakFollowUp is the one follow-up turn the onboarding session gets after a
// red soak, or "" when the soak was green.
func SoakFollowUp(record SoakRecord) string {
	red := record.Red()
	if len(red) == 0 {
		return ""
	}
	var lines []string
	size := 0
	for i, result := range red {
		line := fmt.Sprintf("- %s: %s", result.Subject, result.Detail)
		if result.Item != ItemSoak {
			line = fmt.Sprintf("- %s: %s: %s", result.Subject, result.Item, result.Detail)
		}
		if size+len(line)+1 > soakFollowUpBudget {
			lines = append(lines, fmt.Sprintf("+%d more red scenarios", len(red)-i))
			break
		}
		lines = append(lines, line)
		size += len(line) + 1
	}
	return fmt.Sprintf("SDO installed your export, ran every probe's full checks (three passes, a second prober, altered answers) "+
		"and ran the probes on their schedule for %s while the application was healthy. These were red:\n%s\n\n"+
		"Fix each one, or remove a probe you cannot make reliable and say so. Re-check what you changed with "+
		"`onboarding journeys --only ID` (add `--alter` for an altered-answers red), commit, pass `sdo-operator memory check`, and reply with one line. "+
		"This is the only follow-up: SDO does not soak again.", record.Duration.Duration(), strings.Join(lines, "\n"))
}

// ParseSoakConfigMap decodes the record a SoakConfigMap's data holds.
func ParseSoakConfigMap(namespace string, data map[string]string) (SoakRecord, error) {
	payload, ok := data[SoakDataKey]
	if !ok {
		return SoakRecord{}, fmt.Errorf("onboarding soak ConfigMap %s/%s has no %s", namespace, SoakConfigMap, SoakDataKey)
	}
	var record SoakRecord
	if err := json.Unmarshal([]byte(payload), &record); err != nil {
		return SoakRecord{}, fmt.Errorf("decode onboarding soak record: %w", err)
	}
	return record, nil
}

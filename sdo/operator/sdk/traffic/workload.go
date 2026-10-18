package traffic

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"time"
)

const (
	APIVersion   = "sdo.dev/v1alpha1"
	WorkloadKind = "TrafficWorkload"

	DefaultRatePerSecond     = 4.0
	MaxRatePerSecond         = 20.0
	DefaultTimeout           = 2 * time.Second
	MaxTimeout               = 10 * time.Second
	DefaultIterationTimeout  = 10 * time.Second
	MaxIterationTimeout      = 30 * time.Second
	MaxBurstDuration         = 60 * time.Second
	MaxInFlight              = 8
	DefaultWindow            = 5
	DefaultMinSamples        = 3
	DefaultMaxAge            = 30 * time.Second
	DefaultMaxErrorRate      = 0.5
	DefaultMaxTimeoutRate    = 0.5
	DefaultLatencyPercentile = 90
	DefaultMaxLatency        = 1500 * time.Millisecond
)

// Purpose says what a workload is for.
type Purpose string

const (
	// PurposeHealthProbe runs continuously at a low rate and feeds the
	// application's traffic health detectors.
	PurposeHealthProbe Purpose = "health-probe"
	// PurposeVerifyBurst runs on demand for a few seconds over the scenarios an
	// incident affects, to verify a repair quickly.
	PurposeVerifyBurst Purpose = "verify-burst"
	// PurposeJourney runs on demand for a bounded time, for longer user flows.
	PurposeJourney Purpose = "journey"
	// PurposeLinkProbe runs continuously and feeds link reachability
	// detectors: it dials declared service-to-service edges afresh (link.go).
	PurposeLinkProbe Purpose = "link-probe"
)

// Arrival is the inter-arrival pattern of iterations.
type Arrival string

const (
	ArrivalUniform Arrival = "uniform"
	ArrivalPoisson Arrival = "poisson"
)

// Duration is a time.Duration written as a Go duration string ("2s").
type Duration time.Duration

func (d Duration) Duration() time.Duration { return time.Duration(d) }

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

func (d *Duration) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return fmt.Errorf("duration must be a string such as \"2s\": %w", err)
	}
	parsed, err := time.ParseDuration(text)
	if err != nil {
		return err
	}
	*d = Duration(parsed)
	return nil
}

// SLO bounds a scenario's recent behaviour. Zero fields inherit the
// workload's SLO, and then the package defaults.
type SLO struct {
	Window            int      `json:"window,omitempty"`
	MinSamples        int      `json:"minSamples,omitempty"`
	MaxAge            Duration `json:"maxAge,omitempty"`
	MaxErrorRate      float64  `json:"maxErrorRate,omitempty"`
	MaxTimeoutRate    float64  `json:"maxTimeoutRate,omitempty"`
	LatencyPercentile int      `json:"latencyPercentile,omitempty"`
	MaxLatency        Duration `json:"maxLatency,omitempty"`
}

// WorkloadScenario selects one catalog scenario with a weight and optional
// SLO override.
type WorkloadScenario struct {
	ID     string `json:"id"`
	Weight int    `json:"weight,omitempty"`
	SLO    *SLO   `json:"slo,omitempty"`
}

// Workload is a validated workload profile. Use ParseWorkloadJSON.
type Workload struct {
	APIVersion       string             `json:"apiVersion"`
	Kind             string             `json:"kind"`
	Name             string             `json:"name"`
	Description      string             `json:"description,omitempty"`
	Purpose          Purpose            `json:"purpose"`
	Arrival          Arrival            `json:"arrival,omitempty"`
	RatePerSecond    float64            `json:"ratePerSecond,omitempty"`
	Duration         Duration           `json:"duration,omitempty"`
	Timeout          Duration           `json:"timeout,omitempty"`
	IterationTimeout Duration           `json:"iterationTimeout,omitempty"`
	Seed             uint64             `json:"seed,omitempty"`
	SLO              SLO                `json:"slo,omitempty"`
	Scenarios        []WorkloadScenario `json:"scenarios,omitempty"`
	// Links, Interval and Failures configure a link-probe workload: the
	// dependency edges to dial afresh, how often, and how many consecutive
	// failed dials report an edge.
	Links    []Link   `json:"links,omitempty"`
	Interval Duration `json:"interval,omitempty"`
	Failures int      `json:"failures,omitempty"`
}

// ParseWorkloadJSON strictly decodes a workload, applies defaults, and
// validates it on its own; ValidateAgainst checks it against a catalog.
func ParseWorkloadJSON(data []byte) (Workload, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var workload Workload
	if err := decoder.Decode(&workload); err != nil {
		return Workload{}, fmt.Errorf("decode traffic workload: %w", err)
	}
	workload = workload.WithDefaults()
	if err := workload.Validate(); err != nil {
		return Workload{}, err
	}
	return workload, nil
}

// WithDefaults fills unset fields.
func (w Workload) WithDefaults() Workload {
	if w.Arrival == "" {
		w.Arrival = ArrivalUniform
	}
	if w.RatePerSecond == 0 {
		w.RatePerSecond = DefaultRatePerSecond
	}
	if w.Timeout == 0 {
		w.Timeout = Duration(DefaultTimeout)
		if w.Purpose == PurposeLinkProbe {
			w.Timeout = Duration(DefaultLinkTimeout)
		}
	}
	if w.Purpose == PurposeLinkProbe {
		if w.Interval == 0 {
			w.Interval = Duration(DefaultLinkInterval)
		}
		if w.Failures == 0 {
			w.Failures = DefaultLinkFailures
		}
	}
	if w.IterationTimeout == 0 {
		w.IterationTimeout = Duration(DefaultIterationTimeout)
	}
	if w.Seed == 0 {
		hash := fnv.New64a()
		_, _ = hash.Write([]byte(w.Name))
		w.Seed = hash.Sum64()
	}
	w.SLO = mergeSLO(defaultSLO(), w.SLO)
	scenarios := make([]WorkloadScenario, len(w.Scenarios))
	for index, scenario := range w.Scenarios {
		if scenario.Weight == 0 {
			scenario.Weight = 1
		}
		scenarios[index] = scenario
	}
	w.Scenarios = scenarios
	return w
}

// Validate reports the first problem that makes the workload unsafe.
func (w Workload) Validate() error {
	if w.APIVersion != APIVersion {
		return fmt.Errorf("traffic workload apiVersion must be %s", APIVersion)
	}
	if w.Kind != WorkloadKind {
		return fmt.Errorf("traffic workload kind must be %s", WorkloadKind)
	}
	if !namePattern.MatchString(w.Name) {
		return fmt.Errorf("traffic workload name %q must be a lowercase DNS label", w.Name)
	}
	switch w.Purpose {
	case PurposeHealthProbe:
		if w.Duration != 0 {
			return fmt.Errorf("traffic workload %s: a health-probe runs continuously and takes no duration", w.Name)
		}
	case PurposeLinkProbe:
		if w.Duration != 0 {
			return fmt.Errorf("traffic workload %s: a link-probe runs continuously and takes no duration", w.Name)
		}
	case PurposeVerifyBurst, PurposeJourney:
		if w.Duration.Duration() <= 0 || w.Duration.Duration() > MaxBurstDuration {
			return fmt.Errorf("traffic workload %s: duration must be in (0, %s]", w.Name, MaxBurstDuration)
		}
	default:
		return fmt.Errorf("traffic workload %s: purpose must be %q, %q, %q, or %q",
			w.Name, PurposeHealthProbe, PurposeVerifyBurst, PurposeJourney, PurposeLinkProbe)
	}
	if w.Arrival != ArrivalUniform && w.Arrival != ArrivalPoisson {
		return fmt.Errorf("traffic workload %s: arrival must be %q or %q", w.Name, ArrivalUniform, ArrivalPoisson)
	}
	if w.RatePerSecond <= 0 || w.RatePerSecond > MaxRatePerSecond {
		return fmt.Errorf("traffic workload %s: ratePerSecond must be in (0, %g]", w.Name, MaxRatePerSecond)
	}
	if w.Timeout.Duration() <= 0 || w.Timeout.Duration() > MaxTimeout {
		return fmt.Errorf("traffic workload %s: timeout must be in (0, %s]", w.Name, MaxTimeout)
	}
	if w.IterationTimeout.Duration() < w.Timeout.Duration() || w.IterationTimeout.Duration() > MaxIterationTimeout {
		return fmt.Errorf("traffic workload %s: iterationTimeout must be in [timeout, %s]", w.Name, MaxIterationTimeout)
	}
	if err := validateSLO(w.SLO); err != nil {
		return fmt.Errorf("traffic workload %s: %w", w.Name, err)
	}
	if err := w.validateLinks(); err != nil {
		return err
	}
	if w.Purpose == PurposeLinkProbe {
		return nil
	}
	if len(w.Scenarios) == 0 {
		return fmt.Errorf("traffic workload %s: at least one scenario is required", w.Name)
	}
	seen := make(map[string]bool, len(w.Scenarios))
	for _, scenario := range w.Scenarios {
		if !scenarioPattern.MatchString(scenario.ID) {
			return fmt.Errorf("traffic workload %s: scenario id %q is invalid", w.Name, scenario.ID)
		}
		if seen[scenario.ID] {
			return fmt.Errorf("traffic workload %s: scenario %q is listed twice", w.Name, scenario.ID)
		}
		seen[scenario.ID] = true
		if scenario.Weight < 1 || scenario.Weight > 100 {
			return fmt.Errorf("traffic workload %s: scenario %s weight must be in [1, 100]", w.Name, scenario.ID)
		}
		if scenario.SLO != nil {
			if err := validateSLO(w.ScenarioSLO(scenario.ID)); err != nil {
				return fmt.Errorf("traffic workload %s: scenario %s: %w", w.Name, scenario.ID, err)
			}
		}
	}
	return nil
}

// ValidateAgainst checks that every scenario the workload names exists in the
// catalog, and that a health probe includes at least one read scenario.
func (w Workload) ValidateAgainst(catalog Catalog) error {
	if w.Purpose == PurposeLinkProbe {
		return nil
	}
	reads := 0
	for _, selected := range w.Scenarios {
		scenario, ok := catalog.Scenario(selected.ID)
		if !ok {
			return fmt.Errorf("traffic workload %s: scenario %q is not provided by the generators", w.Name, selected.ID)
		}
		if scenario.SideEffect == SideEffectRead {
			reads++
		}
	}
	if w.Purpose == PurposeHealthProbe && reads == 0 {
		return fmt.Errorf("traffic workload %s: a health probe needs at least one read scenario", w.Name)
	}
	return nil
}

// SLO returns the workload's effective SLO, defaults included.
func (w Window) SLO() SLO { return mergeSLO(defaultSLO(), w.Workload.SLO) }

// ScenarioSLO is the effective SLO of one scenario of the workload.
func (w Workload) ScenarioSLO(id string) SLO {
	slo := mergeSLO(defaultSLO(), w.SLO)
	for _, scenario := range w.Scenarios {
		if scenario.ID == id && scenario.SLO != nil {
			slo = mergeSLO(slo, *scenario.SLO)
		}
	}
	return slo
}

func defaultSLO() SLO {
	return SLO{
		Window: DefaultWindow, MinSamples: DefaultMinSamples, MaxAge: Duration(DefaultMaxAge),
		MaxErrorRate: DefaultMaxErrorRate, MaxTimeoutRate: DefaultMaxTimeoutRate,
		LatencyPercentile: DefaultLatencyPercentile, MaxLatency: Duration(DefaultMaxLatency),
	}
}

func mergeSLO(base SLO, override SLO) SLO {
	if override.Window != 0 {
		base.Window = override.Window
	}
	if override.MinSamples != 0 {
		base.MinSamples = override.MinSamples
	}
	if override.MaxAge != 0 {
		base.MaxAge = override.MaxAge
	}
	if override.MaxErrorRate != 0 {
		base.MaxErrorRate = override.MaxErrorRate
	}
	if override.MaxTimeoutRate != 0 {
		base.MaxTimeoutRate = override.MaxTimeoutRate
	}
	if override.LatencyPercentile != 0 {
		base.LatencyPercentile = override.LatencyPercentile
	}
	if override.MaxLatency != 0 {
		base.MaxLatency = override.MaxLatency
	}
	return base
}

func validateSLO(slo SLO) error {
	switch {
	case slo.Window < 1 || slo.Window > 100:
		return fmt.Errorf("slo window must be in [1, 100] samples")
	case slo.MinSamples < 1 || slo.MinSamples > slo.Window:
		return fmt.Errorf("slo minSamples must be in [1, window]")
	case slo.MaxAge.Duration() <= 0 || slo.MaxAge.Duration() > 10*time.Minute:
		return fmt.Errorf("slo maxAge must be in (0, 10m]")
	case slo.MaxErrorRate <= 0 || slo.MaxErrorRate > 1:
		return fmt.Errorf("slo maxErrorRate must be in (0, 1]")
	case slo.MaxTimeoutRate <= 0 || slo.MaxTimeoutRate > 1:
		return fmt.Errorf("slo maxTimeoutRate must be in (0, 1]")
	case slo.LatencyPercentile < 1 || slo.LatencyPercentile > 100:
		return fmt.Errorf("slo latencyPercentile must be in [1, 100]")
	case slo.MaxLatency.Duration() <= 0:
		return fmt.Errorf("slo maxLatency must be positive")
	}
	return nil
}

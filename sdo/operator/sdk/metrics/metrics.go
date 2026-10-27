// Package metrics lets deterministic detectors judge high-level application
// metrics (for example from Prometheus) without doing any I/O. A detector
// declares the queries it needs; the controller runtime evaluates each once
// per evaluation cycle at a single timestamp and freezes the results into the
// detection snapshot, exactly as it does for synthetic traffic.
package metrics

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"sdo.dev/operator/sdk"
)

// APIVersion is the API group of the pseudo-kinds the SDK adds for non-Kubernetes inputs.
const APIVersion = "sdo.dev/v1alpha1"

// Watch is the pseudo-kind metrics detectors declare. The controller runtime
// never emits it: metric results are fetched when a detector is due, so a
// metrics detector runs on its Interval.
var Watch = sdk.WatchKind{APIVersion: APIVersion, Kind: "Metrics"}

const (
	// MaxQueries bounds the distinct queries one controller evaluates per cycle.
	MaxQueries = 32
	// MaxExprBytes bounds one PromQL expression.
	MaxExprBytes = 4096
	// MaxRange and MaxRangePoints bound a range query's window and sample count.
	MaxRange       = 24 * time.Hour
	MaxRangePoints = 1000
	// MaxSeries and MaxSamples bound what one query result may freeze into a snapshot.
	MaxSeries  = 1000
	MaxSamples = MaxSeries * 100
	// MaxAlerts bounds the alerts one evaluation cycle may freeze.
	MaxAlerts = 5000
)

// Query is one PromQL query a detector consumes. Without Range it is an
// instant query evaluated at the cycle's timestamp; with Range it is a range
// query over the Range ending at that timestamp.
type Query struct {
	ID   string `json:"id"`
	Expr string `json:"expr"`
	// Range, when positive, makes the query a range query.
	Range time.Duration `json:"range,omitempty"`
	// Step is the range query resolution; zero derives it from Range.
	Step time.Duration `json:"step,omitempty"`
}

// EffectiveStep is Step, or for a range query without one a sixtieth of Range
// (at least a second).
func (q Query) EffectiveStep() time.Duration {
	if q.Range <= 0 {
		return 0
	}
	if q.Step > 0 {
		return q.Step
	}
	return max(q.Range/60, time.Second)
}

// Validate checks one query in isolation.
func (q Query) Validate() error {
	if strings.TrimSpace(q.ID) == "" {
		return errors.New("metric query id is required")
	}
	if q.ID != strings.TrimSpace(q.ID) {
		return fmt.Errorf("metric query id %q must not contain surrounding whitespace", q.ID)
	}
	if strings.TrimSpace(q.Expr) == "" {
		return fmt.Errorf("metric query %q has an empty expression", q.ID)
	}
	if len(q.Expr) > MaxExprBytes {
		return fmt.Errorf("metric query %q expression exceeds %d bytes", q.ID, MaxExprBytes)
	}
	if q.Range < 0 || q.Step < 0 {
		return fmt.Errorf("metric query %q range and step must not be negative", q.ID)
	}
	if q.Range == 0 && q.Step > 0 {
		return fmt.Errorf("metric query %q sets a step without a range", q.ID)
	}
	if q.Range > MaxRange {
		return fmt.Errorf("metric query %q range exceeds %s", q.ID, MaxRange)
	}
	if q.Range > 0 && q.Range/q.EffectiveStep() > MaxRangePoints {
		return fmt.Errorf("metric query %q would return more than %d points per series", q.ID, MaxRangePoints)
	}
	return nil
}

// ValidateQueries checks a set of queries for one controller: each valid,
// IDs unique, and the count bounded.
func ValidateQueries(queries []Query) error {
	if len(queries) > MaxQueries {
		return fmt.Errorf("%d metric queries exceed the limit of %d", len(queries), MaxQueries)
	}
	seen := make(map[string]struct{}, len(queries))
	for _, query := range queries {
		if err := query.Validate(); err != nil {
			return err
		}
		if _, ok := seen[query.ID]; ok {
			return fmt.Errorf("duplicate metric query id %q", query.ID)
		}
		seen[query.ID] = struct{}{}
	}
	return nil
}

// Sample is one value at one time. Values may be NaN or infinite, which JSON
// carries as strings, as Prometheus does.
type Sample struct {
	At    time.Time
	Value float64
}

type sampleJSON struct {
	At    time.Time `json:"at"`
	Value jsonFloat `json:"value"`
}

func (s Sample) MarshalJSON() ([]byte, error) {
	return json.Marshal(sampleJSON{At: s.At, Value: jsonFloat(s.Value)})
}

func (s *Sample) UnmarshalJSON(data []byte) error {
	var decoded sampleJSON
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*s = Sample{At: decoded.At, Value: float64(decoded.Value)}
	return nil
}

type jsonFloat float64

func (f jsonFloat) MarshalJSON() ([]byte, error) {
	value := float64(f)
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return json.Marshal(FormatValue(value))
	}
	return json.Marshal(value)
}

func (f *jsonFloat) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err == nil {
		value, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return fmt.Errorf("metric sample value %q: %w", text, err)
		}
		*f = jsonFloat(value)
		return nil
	}
	var value float64
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*f = jsonFloat(value)
	return nil
}

// FormatValue renders a sample value the way Prometheus does ("NaN", "+Inf").
func FormatValue(value float64) string {
	switch {
	case math.IsNaN(value):
		return "NaN"
	case math.IsInf(value, 1):
		return "+Inf"
	case math.IsInf(value, -1):
		return "-Inf"
	}
	return strconv.FormatFloat(value, 'g', -1, 64)
}

// Series is one labelled time series. An instant query yields one sample.
type Series struct {
	Labels  map[string]string `json:"labels"`
	Samples []Sample          `json:"samples"`
}

// Last is the series' newest sample.
func (s Series) Last() (Sample, bool) {
	if len(s.Samples) == 0 {
		return Sample{}, false
	}
	return s.Samples[len(s.Samples)-1], true
}

// LabelString renders labels sorted by name as `{a="1", b="2"}`.
func LabelString(labels map[string]string) string {
	names := make([]string, 0, len(labels))
	for name := range labels {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, name+"="+strconv.Quote(labels[name]))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// Result is one query's frozen outcome at ObservedAt. A non-empty Error means
// the metrics source could not answer: that is neither healthy nor firing.
type Result struct {
	Query      Query     `json:"query"`
	ObservedAt time.Time `json:"observedAt"`
	Series     []Series  `json:"series,omitempty"`
	Error      string    `json:"error,omitempty"`
}

// Alert is one alert as the Prometheus alerts API reports it.
type Alert struct {
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations,omitempty"`
	// State is "firing" or "pending".
	State    string    `json:"state"`
	ActiveAt time.Time `json:"activeAt,omitempty"`
	Value    string    `json:"value,omitempty"`
}

// Name is the alert's rule name.
func (a Alert) Name() string { return a.Labels["alertname"] }

// AlertsResult is every alert the source reported at ObservedAt, frozen for
// one evaluation cycle. A non-empty Error means the alerts could not be read.
type AlertsResult struct {
	ObservedAt time.Time `json:"observedAt"`
	Alerts     []Alert   `json:"alerts,omitempty"`
	Error      string    `json:"error,omitempty"`
}

// Source is implemented by detection contexts that carry frozen metric
// results. The controller runtime's snapshots implement it.
type Source interface {
	MetricResult(id string) (Result, bool)
}

// QuerySource evaluates a typed PromQL query through the runtime.
// Results (including failures) are frozen for the evaluation cycle.
type QuerySource interface {
	QueryMetrics(Query) Result
}

// AlertSource is implemented by detection contexts that carry frozen alerts.
type AlertSource interface {
	AlertsResult() (AlertsResult, bool)
}

// ResultFrom returns the frozen result of query id, when the context carries it.
func ResultFrom(ctx sdk.DetectionContext, id string) (Result, bool) {
	source, ok := ctx.(Source)
	if !ok {
		return Result{}, false
	}
	return source.MetricResult(id)
}

// AlertsFrom returns the frozen alerts, when the context carries them.
func AlertsFrom(ctx sdk.DetectionContext) (AlertsResult, bool) {
	source, ok := ctx.(AlertSource)
	if !ok {
		return AlertsResult{}, false
	}
	return source.AlertsResult()
}

// Consumer is implemented by detectors that evaluate metric queries. The
// controller runtime evaluates exactly the queries its detectors declare.
type Consumer interface {
	MetricQueries() []Query
}

// AlertConsumer is implemented by detectors that evaluate firing alerts.
type AlertConsumer interface {
	ConsumesAlerts() bool
}

// Validator is implemented by metrics detectors that can check their own
// configuration, so a mistake surfaces in tests rather than at runtime.
type Validator interface {
	Validate() error
}

// Queries returns the distinct queries the detectors consume, sorted by ID.
// Detectors that declare the same ID with different definitions conflict.
func Queries(detectors []sdk.Detector) ([]Query, error) {
	byID := make(map[string]Query)
	for _, detector := range detectors {
		consumer, ok := sdk.As[Consumer](detector)
		if !ok {
			continue
		}
		for _, query := range consumer.MetricQueries() {
			if existing, ok := byID[query.ID]; ok && existing != query {
				return nil, fmt.Errorf("detector %q redefines metric query %q", detector.Spec().ID, query.ID)
			}
			byID[query.ID] = query
		}
	}
	queries := make([]Query, 0, len(byID))
	for _, query := range byID {
		queries = append(queries, query)
	}
	sort.Slice(queries, func(left int, right int) bool { return queries[left].ID < queries[right].ID })
	if err := ValidateQueries(queries); err != nil {
		return nil, err
	}
	return queries, nil
}

// ConsumesAlerts reports whether any detector evaluates alerts.
func ConsumesAlerts(detectors []sdk.Detector) bool {
	for _, detector := range detectors {
		if consumer, ok := sdk.As[AlertConsumer](detector); ok && consumer.ConsumesAlerts() {
			return true
		}
	}
	return false
}

// ValidateDetector checks one detector's metrics configuration. Detectors
// that consume no metrics are valid.
func ValidateDetector(detector sdk.Detector) error {
	if validator, ok := sdk.As[Validator](detector); ok {
		if err := validator.Validate(); err != nil {
			return err
		}
	}
	if consumer, ok := sdk.As[Consumer](detector); ok {
		return ValidateQueries(consumer.MetricQueries())
	}
	return nil
}

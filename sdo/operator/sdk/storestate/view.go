package storestate

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
)

// queryPrefix starts the pod read of every store-state detector.
const queryPrefix = "store-state-"

// QueryID is the pod read a store-state detector declares for component. It
// names the engine, so a reader without the detector can parse the output.
func QueryID(engine Engine, component string) string {
	return queryPrefix + string(engine) + "-" + component
}

// ParseQueryID returns the engine and component of a store-state pod read.
// Engines carry no '-', so the first segment after the prefix is the engine.
func ParseQueryID(id string) (Engine, string, bool) {
	rest, ok := strings.CutPrefix(id, queryPrefix)
	if !ok {
		return "", "", false
	}
	engine, component, ok := strings.Cut(rest, "-")
	if !ok || component == "" {
		return "", "", false
	}
	if _, known := AdapterFor(Engine(engine)); !known {
		return "", "", false
	}
	return Engine(engine), component, true
}

// StoreChanges is one store Pod's facts against the controller's promoted
// healthy baseline. Baselined is false when no usable baseline reading exists
// for the Pod; Changes is then empty. Error is set when the current read gave
// no answer, and the other fields are then unknown.
type StoreChanges struct {
	Component  string        `json:"component"`
	Engine     Engine        `json:"engine"`
	Pod        sdk.ObjectRef `json:"pod"`
	ObservedAt time.Time     `json:"observed_at,omitzero"`
	BaselineAt time.Time     `json:"baseline_at,omitzero"`
	Baselined  bool          `json:"baselined"`
	Changes    []Change      `json:"changes,omitempty"`
	Error      string        `json:"error,omitempty"`
}

// Changes is the read-only diff of every store whose store-state pod read the
// view carries (a detection snapshot, a Diagnose read view or a capture),
// sorted by component and Pod. It reads only frozen results and never fails: a
// read without an answer is an entry with Error.
func Changes(view sdk.DetectionContext) []StoreChanges {
	set, ok := view.(podread.ResultSet)
	if !ok {
		return nil
	}
	var out []StoreChanges
	for id, result := range set.PodReadResults() {
		engine, component, ok := ParseQueryID(id)
		if !ok {
			continue
		}
		adapter, _ := AdapterFor(engine)
		out = append(out, storeChanges(adapter, component, result)...)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Component != out[j].Component {
			return out[i].Component < out[j].Component
		}
		return out[i].Pod.Name < out[j].Pod.Name
	})
	return out
}

func storeChanges(adapter Adapter, component string, result podread.Result) []StoreChanges {
	engine := adapter.Engine()
	if result.Error != "" || len(result.Readings) == 0 {
		reason := result.Error
		if reason == "" {
			reason = "no readings"
		}
		return []StoreChanges{{Component: component, Engine: engine, Error: reason}}
	}
	baselines := promotedBaselines(adapter, component, result)
	out := make([]StoreChanges, 0, len(result.Readings))
	for _, reading := range result.Readings {
		entry := StoreChanges{Component: component, Engine: engine, Pod: reading.Ref(), ObservedAt: reading.ObservedAt}
		current, err := parseReading(adapter, component, reading)
		if err != nil {
			entry.Error = err.Error()
			out = append(out, entry)
			continue
		}
		if baseline, ok := pick(baselines, reading.Pod); ok {
			entry.Baselined, entry.BaselineAt = true, baseline.At
			entry.Changes = Diff(baseline, current)
		}
		out = append(out, entry)
	}
	return out
}

// parseReading parses one complete reading into facts of component.
func parseReading(adapter Adapter, component string, reading podread.Reading) (Facts, error) {
	if reading.Error != "" {
		return Facts{}, readingError{reading.Error}
	}
	if reading.Truncated {
		return Facts{}, readingError{"output truncated"}
	}
	f, err := adapter.Parse(reading.Stdout)
	if err != nil {
		return Facts{}, err
	}
	f.Component, f.Pod, f.At = component, reading.Ref(), reading.ObservedAt
	return f, nil
}

type readingError struct{ reason string }

func (e readingError) Error() string { return e.reason }

// promotedBaselines parses the result's promoted healthy readings, by Pod
// name. A reading that does not parse into a healthy baseline is dropped.
func promotedBaselines(adapter Adapter, component string, result podread.Result) map[string]Facts {
	out := map[string]Facts{}
	for _, reading := range result.Baseline {
		f, err := parseReading(adapter, component, reading)
		if err != nil || validBaseline(Config{Component: component, Engine: adapter.Engine()}, f) != nil {
			continue
		}
		out[reading.Pod] = f
	}
	return out
}

// UsableAsBaseline reports why a store-state pod read cannot become the
// healthy reference: a reading that is unavailable, unparsable, refused (other
// than an unsupported command) or shows a write block. A store whose read was
// refused when health was promoted is never observed, so its later refusals
// stay coverage gaps instead of being compared with a refused reference. It
// returns nil for a pod read that is not a store-state read.
func UsableAsBaseline(id string, result podread.Result) error {
	engine, component, ok := ParseQueryID(id)
	if !ok {
		return nil
	}
	adapter, _ := AdapterFor(engine)
	if result.Error != "" {
		return readingError{result.Error}
	}
	if len(result.Readings) == 0 {
		return readingError{"no readings"}
	}
	for _, reading := range result.Readings {
		f, err := parseReading(adapter, component, reading)
		if err == nil {
			err = validBaseline(Config{Component: component, Engine: engine}, f)
		}
		if err != nil {
			return fmt.Errorf("store %s Pod %s: %w", component, reading.Pod, err)
		}
	}
	return nil
}

// pick returns the baseline of pod, or the only baseline when there is one, so
// a single-instance store compares across a Pod replacement.
func pick(baselines map[string]Facts, pod string) (Facts, bool) {
	if b, ok := baselines[pod]; ok {
		return b, true
	}
	if len(baselines) == 1 {
		for _, b := range baselines {
			return b, true
		}
	}
	return Facts{}, false
}

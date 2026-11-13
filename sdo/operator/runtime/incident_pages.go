package runtime

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/storestate"
	"sdo.dev/operator/sdk/traffic"
)

// Page-time files: at dispatch and on each `sdo detect` the controller writes
// incident/timeline.csv and incident/changes.csv into the responder's session.
// They are information, never a verdict: no row is scored or ranked.
const (
	// incidentSignalHorizon is how much per-operation and workload history the
	// controller keeps; traffic windows alone hold only about 30 s.
	incidentSignalHorizon = 15 * time.Minute
	incidentTimelineLead  = 3 * time.Minute
	incidentTimelineStep  = 10 * time.Second
	// incidentChangesWindow is how far before onset changes.csv reaches.
	incidentChangesWindow = 30 * time.Minute
	incidentChangesCap    = 200
	incidentPageOps       = 24
	incidentPageWorkloads = 8
	incidentPageValue     = 120
	incidentSignalSamples = 4096
	incidentSignalSeries  = 128
	// incidentSessionMount is where the agent sees its session directory.
	incidentSessionMount = "/sdo/session"
)

// incidentPageNames are the files, in the order `sdo detect` names them.
var incidentPageNames = []string{"timeline.csv", "changes.csv"}

// IncidentChange is one row of changes.csv.
type IncidentChange struct {
	At             time.Time
	Source         string // kubernetes, store or app-state
	Component      string
	ReaderOrObject string
	Key            string
	Before, After  string
	Writer         string
	Noisy          bool
}

// AppStateChangeSource is the seam app-state readers plug into: the controller
// asks it for the app-state changes between since and now, and lists them in
// changes.csv with source "app-state". Values must already exclude credentials;
// the page redacts credential-shaped text again. Nil lists none.
type AppStateChangeSource interface {
	AppStateChanges(since, now time.Time) []IncidentChange
}

type operationSample struct {
	at     time.Time
	failed bool
}

type workloadSample struct {
	at                       time.Time
	ready, desired, restarts int64
}

// incidentSignals is the controller's 15-minute ring of per-operation (user
// test) results and per-workload ready/desired Pods and restarts, fed on every
// evaluation. It is safe for concurrent use.
type incidentSignals struct {
	mu         sync.Mutex
	operations map[string][]operationSample
	workloads  map[string][]workloadSample
}

func newIncidentSignals() *incidentSignals {
	return &incidentSignals{operations: map[string][]operationSample{}, workloads: map[string][]workloadSample{}}
}

// Observe adds the snapshot's traffic samples newer than those already held
// and each workload's state when it differs from the last one held.
func (s *incidentSignals) Observe(now time.Time, view sdk.DetectionContext) {
	if s == nil || view == nil {
		return
	}
	operations := map[string][]traffic.Sample{}
	if set, ok := view.(traffic.WindowSet); ok {
		for _, window := range set.TrafficWindows() {
			for _, observed := range window.Scenarios {
				operations[observed.Scenario.ID] = append(operations[observed.Scenario.ID], observed.Samples...)
			}
		}
	}
	workloads := workloadStates(view)
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, samples := range operations {
		held, known := s.operations[id]
		if !known && len(s.operations) >= incidentSignalSeries {
			continue
		}
		var last time.Time
		if n := len(held); n > 0 {
			last = held[n-1].at
		}
		sort.SliceStable(samples, func(i, j int) bool { return samples[i].At.Before(samples[j].At) })
		for _, sample := range samples {
			if sample.Outcome == traffic.OutcomeInvalid || !sample.At.After(last) {
				continue
			}
			held = append(held, operationSample{at: sample.At, failed: sample.Outcome != traffic.OutcomeOK})
			last = sample.At
		}
		s.operations[id] = held
	}
	for name, state := range workloads {
		held, known := s.workloads[name]
		if !known && len(s.workloads) >= incidentSignalSeries {
			continue
		}
		if n := len(held); n > 0 && held[n-1].ready == state.ready && held[n-1].desired == state.desired && held[n-1].restarts == state.restarts {
			continue
		}
		state.at = now
		s.workloads[name] = append(held, state)
	}
	horizon := now.Add(-incidentSignalHorizon)
	for id, held := range s.operations {
		cut := sort.Search(len(held), func(i int) bool { return !held[i].at.Before(horizon) })
		cut = max(cut, len(held)-incidentSignalSamples)
		if cut > 0 {
			held = append([]operationSample(nil), held[cut:]...)
		}
		if len(held) == 0 {
			delete(s.operations, id)
			continue
		}
		s.operations[id] = held
	}
	for name, held := range s.workloads {
		// Keep the last state before the horizon: it holds until the next change.
		cut := sort.Search(len(held), func(i int) bool { return !held[i].at.Before(horizon) }) - 1
		cut = max(cut, len(held)-incidentSignalSamples)
		if cut > 0 {
			s.workloads[name] = append([]workloadSample(nil), held[cut:]...)
		}
	}
}

// history is a copy of the ring for rendering.
func (s *incidentSignals) history() incidentSignalHistory {
	out := incidentSignalHistory{operations: map[string][]operationSample{}, workloads: map[string][]workloadSample{}}
	if s == nil {
		return out
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, held := range s.operations {
		out.operations[id] = slices.Clone(held)
	}
	for name, held := range s.workloads {
		out.workloads[name] = slices.Clone(held)
	}
	return out
}

type incidentSignalHistory struct {
	operations map[string][]operationSample
	workloads  map[string][]workloadSample
}

// workloadStates are each Deployment's and StatefulSet's ready and desired
// Pods and the restarts of the Pods it owns, keyed Kind/Name.
func workloadStates(view sdk.DetectionContext) map[string]workloadSample {
	states := map[string]workloadSample{}
	for _, deployment := range view.Deployments() {
		desired := int64(1)
		if deployment.Spec.Replicas != nil {
			desired = int64(*deployment.Spec.Replicas)
		}
		states["Deployment/"+deployment.Name] = workloadSample{ready: int64(deployment.Status.ReadyReplicas), desired: desired}
	}
	for _, statefulSet := range view.StatefulSets() {
		desired := int64(1)
		if statefulSet.Spec.Replicas != nil {
			desired = int64(*statefulSet.Spec.Replicas)
		}
		states["StatefulSet/"+statefulSet.Name] = workloadSample{ready: int64(statefulSet.Status.ReadyReplicas), desired: desired}
	}
	for _, pod := range view.Pods() {
		owner := program.WorkloadOf(view, sdk.ObjectRef{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name})
		state, ok := states[owner]
		if !ok {
			continue
		}
		for _, status := range pod.Status.ContainerStatuses {
			state.restarts += int64(status.RestartCount)
		}
		states[owner] = state
	}
	return states
}

// incidentPageInput is what the page files are built from.
type incidentPageInput struct {
	Request  IncidentRequest
	Findings []sdk.Finding
	View     detectReads
	Signals  *incidentSignals
	AppState AppStateChangeSource
	Now      time.Time
}

type incidentPages struct {
	Timeline []byte
	Changes  []byte
}

func (p incidentPages) files() map[string][]byte {
	return map[string][]byte{"timeline.csv": p.Timeline, "changes.csv": p.Changes}
}

// buildIncidentPages renders both files. Onset, the failing tests and the
// suspects are the ones `sdo detect` shows.
func buildIncidentPages(in incidentPageInput) incidentPages {
	set := buildSuspectSet(detectInput{Request: in.Request, Findings: in.Findings, View: in.View, Now: in.Now})
	onset := set.onset
	if onset.IsZero() {
		onset = in.Now
	}
	return incidentPages{
		Timeline: renderTimeline(set, in.Signals.history(), in.Request, in.View, onset, in.Now),
		Changes:  renderChanges(incidentChangeRows(in, set, onset), onset),
	}
}

func renderTimeline(set detectSuspectSet, history incidentSignalHistory, request IncidentRequest, view detectReads, onset, now time.Time) []byte {
	// Operations: the failing tests first, then the rest by name; no ranking.
	var operations []string
	for _, test := range set.failing {
		if _, ok := history.operations[test.id]; ok && !slices.Contains(operations, test.id) {
			operations = append(operations, test.id)
		}
	}
	rest := make([]string, 0, len(history.operations))
	for id := range history.operations {
		if !slices.Contains(operations, id) {
			rest = append(rest, id)
		}
	}
	sort.Strings(rest)
	operations = append(operations, rest...)
	if len(operations) > incidentPageOps {
		operations = operations[:incidentPageOps]
	}
	namespace := request.Namespace
	if namespace == "" && view != nil {
		namespace = view.Namespace()
	}
	type workloadColumn struct{ component, key string }
	var workloads []workloadColumn
	for _, suspect := range set.suspects {
		if suspect.component.Workload == "" || len(workloads) == incidentPageWorkloads {
			continue
		}
		ref := suspect.component.WorkloadRef(namespace)
		workloads = append(workloads, workloadColumn{component: suspect.component.Name, key: ref.Kind + "/" + ref.Name})
	}
	header := []string{"t_rel_s", "at_utc"}
	for _, id := range operations {
		header = append(header, "fail:"+redactDetectText(id, 80))
	}
	for _, column := range workloads {
		name := redactDetectText(column.component, 80)
		header = append(header, "ready:"+name, "desired:"+name, "restarts:"+name)
	}
	floor := func(d time.Duration) int64 {
		q := int64(d / incidentTimelineStep)
		if d < 0 && d%incidentTimelineStep != 0 {
			q--
		}
		return q
	}
	// Buckets are aligned on onset and start no earlier than the ring holds.
	first := max(-int64(incidentTimelineLead/incidentTimelineStep), -floor(onset.Sub(now.Add(-incidentSignalHorizon))))
	last := floor(now.Sub(onset))
	rows := [][]string{header}
	for bucket := first; bucket <= last; bucket++ {
		start := onset.Add(time.Duration(bucket) * incidentTimelineStep)
		end := start.Add(incidentTimelineStep)
		row := []string{strconv.FormatInt(int64(start.Sub(onset)/time.Second), 10), start.UTC().Format(time.RFC3339)}
		for _, id := range operations {
			failed, total := 0, 0
			for _, sample := range history.operations[id] {
				if !sample.at.Before(start) && sample.at.Before(end) {
					total++
					if sample.failed {
						failed++
					}
				}
			}
			cell := ""
			if total > 0 {
				cell = strconv.FormatFloat(float64(failed)/float64(total), 'f', 2, 64)
			}
			row = append(row, cell)
		}
		for _, column := range workloads {
			var state *workloadSample
			for index, sample := range history.workloads[column.key] {
				if sample.at.Before(end) {
					state = &history.workloads[column.key][index]
				}
			}
			if state == nil {
				row = append(row, "", "", "")
				continue
			}
			row = append(row, strconv.FormatInt(state.ready, 10), strconv.FormatInt(state.desired, 10), strconv.FormatInt(state.restarts, 10))
		}
		rows = append(rows, row)
	}
	return encodeCSV(rows)
}

// incidentChangeRows are the source departures of every component (with the
// writer), the store-state changes and the app-state changes, from
// onset-30 min to now. Every value passes the summary's redaction.
func incidentChangeRows(in incidentPageInput, set detectSuspectSet, onset time.Time) []IncidentChange {
	since := onset.Add(-incidentChangesWindow)
	var rows []IncidentChange
	if in.View != nil {
		namespace := in.Request.Namespace
		if namespace == "" {
			namespace = in.View.Namespace()
		}
		seen := map[string]bool{}
		input := departureInput(in.Request, in.View, onset)
		for _, component := range set.m.Components {
			if component.Workload == "" {
				continue
			}
			for _, departure := range SourceDepartures(input, componentObjects(in.View, component, namespace)) {
				key := refText(departure.Ref)
				if seen[key] {
					continue
				}
				seen[key] = true
				rows = append(rows, IncidentChange{
					At: departure.At, Source: "kubernetes", Component: component.Name,
					ReaderOrObject: departure.Ref.Kind + "/" + departure.Ref.Name, Key: strings.Join(departure.Fields, ";"),
					Before: "accepted-source", After: string(departure.Kind), Writer: departure.Manager,
				})
			}
		}
		rows = append(rows, storeChangeRows(storestate.Changes(in.View))...)
	}
	if in.AppState != nil {
		for _, row := range in.AppState.AppStateChanges(since, in.Now) {
			row.Source = "app-state"
			rows = append(rows, row)
		}
	}
	kept := rows[:0]
	for _, row := range rows {
		// A departure nothing timed (a missing object) is listed without a time.
		if row.At.IsZero() || !row.At.Before(since) && !row.At.After(in.Now) {
			kept = append(kept, row)
		}
	}
	rows = kept
	sort.SliceStable(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if a.At.IsZero() != b.At.IsZero() {
			return a.At.IsZero()
		}
		return a.At.Before(b.At)
	})
	if len(rows) > incidentChangesCap {
		rows = rows[len(rows)-incidentChangesCap:]
	}
	return rows
}

// storeChangeRows are the store-state changes against each store's promoted
// baseline, at the time the store was read; the change itself is untimed.
func storeChangeRows(stores []storestate.StoreChanges) []IncidentChange {
	var rows []IncidentChange
	for _, store := range stores {
		if store.Error != "" || !store.Baselined {
			continue
		}
		for _, change := range store.Changes {
			row := IncidentChange{At: store.ObservedAt, Source: "store", Component: store.Component, ReaderOrObject: store.Pod.Kind + "/" + store.Pod.Name, Key: change.Key().String()}
			if change.Before != nil {
				row.Before = bounded(change.Before.Value)
			}
			if change.After != nil {
				row.After = bounded(change.After.Value)
			}
			rows = append(rows, row)
		}
	}
	return rows
}

func renderChanges(rows []IncidentChange, onset time.Time) []byte {
	out := [][]string{{"t_rel_s", "at_utc", "source", "component", "reader_or_object", "key", "before", "after", "writer", "noisy"}}
	clean := func(text string) string { return redactDetectText(text, incidentPageValue) }
	for _, row := range rows {
		relative, at := "", ""
		if !row.At.IsZero() {
			relative = strconv.FormatInt(int64(row.At.Sub(onset)/time.Second), 10)
			at = row.At.UTC().Format(time.RFC3339)
		}
		out = append(out, []string{relative, at, row.Source, clean(row.Component), clean(row.ReaderOrObject), clean(row.Key), clean(row.Before), clean(row.After), clean(row.Writer), strconv.FormatBool(row.Noisy)})
	}
	return encodeCSV(out)
}

func encodeCSV(rows [][]string) []byte {
	var buffer bytes.Buffer
	writer := csv.NewWriter(&buffer)
	_ = writer.WriteAll(rows)
	return buffer.Bytes()
}

// writeIncidentPages builds the pages and writes them into the incident's
// session under the repository mount; it returns the paths the agent sees.
func writeIncidentPages(repositoryMount string, in incidentPageInput) ([]string, error) {
	pages := buildIncidentPages(in)
	if err := memory.WriteIncidentPages(repositoryMount, in.Request.IncidentID, pages.files()); err != nil {
		return nil, fmt.Errorf("write incident pages: %w", err)
	}
	paths := make([]string, 0, len(incidentPageNames))
	for _, name := range incidentPageNames {
		paths = append(paths, path.Join(incidentSessionMount, memory.IncidentPagesDir, name))
	}
	return paths, nil
}

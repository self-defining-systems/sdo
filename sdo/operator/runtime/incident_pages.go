package runtime

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/storeops"
	"sdo.dev/operator/sdk/storestate"
	"sdo.dev/operator/sdk/traffic"
)

// On-demand pages: `sdo changes` and `sdo timeline` print CSV computed from the
// controller's current state when called, and the first message and `sdo detect`
// show the candidate view. They are information, never a
// verdict: no row is scored or ranked beyond time order.
const (
	// incidentSignalHorizon is how much per-operation and workload history the
	// controller keeps; traffic windows alone hold only about 30 s.
	incidentSignalHorizon = 15 * time.Minute
	incidentTimelineStep  = 10 * time.Second
	// incidentDefaultLead is how far before onset `sdo changes` and `sdo timeline`
	// reach unless --since says otherwise.
	incidentDefaultLead   = 10 * time.Minute
	incidentChangesCap    = 200
	incidentInlineValue   = 60
	incidentPageOps       = 24
	incidentPageWorkloads = 8
	incidentPageValue     = 120
	incidentSignalSamples = 4096
	incidentSignalSeries  = 128
)

// IncidentChange is one row of `sdo changes`.
type IncidentChange struct {
	// At is when the change happened; zero when nothing timed it.
	At time.Time
	// ReadAt is when a source with no change time was read (a store diff): it
	// is the time the row was observed, never an onset-relative change time.
	ReadAt         time.Time
	Source         string // kubernetes, store or app-state
	Component      string
	ReaderOrObject string
	Key            string
	Before, After  string
	Writer         string
	Noisy          bool
	// Command is the command that expands or restores the row's object, when there is one.
	Command string
}

// AppStateChangeSource is the seam app-state readers plug into: the controller
// asks it for the app-state changes between since and now, and lists them in
// `sdo changes` with source "app-state". Values must already exclude credentials;
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
			if sample.Outcome == traffic.OutcomeInvalid || sample.At.Before(last) || sample.At.Equal(last) && heldAt(held, sample.At, sample.Outcome != traffic.OutcomeOK) {
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
		// A workload gone from the snapshot whose last state left the horizon is dropped.
		if _, present := workloads[name]; !present && held[len(held)-1].at.Before(horizon) {
			delete(s.workloads, name)
			continue
		}
		// Keep the last state before the horizon: it holds until the next change.
		cut := sort.Search(len(held), func(i int) bool { return !held[i].at.Before(horizon) }) - 1
		cut = max(cut, len(held)-incidentSignalSamples)
		if cut > 0 {
			s.workloads[name] = append([]workloadSample(nil), held[cut:]...)
		}
	}
}

// heldAt reports whether held ends with a sample at at with the same outcome:
// windows overlap between evaluations, so the same sample is seen again.
func heldAt(held []operationSample, at time.Time, failed bool) bool {
	for index := len(held) - 1; index >= 0 && held[index].at.Equal(at); index-- {
		if held[index].failed == failed {
			return true
		}
	}
	return false
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
	// One pass over ReplicaSets and Pods: this runs on every evaluation.
	replicaSets := map[string]string{}
	for _, replicaSet := range view.ReplicaSets() {
		if owner := metav1.GetControllerOf(&replicaSet); owner != nil {
			replicaSets[replicaSet.Name] = owner.Kind + "/" + owner.Name
		}
	}
	for _, pod := range view.Pods() {
		controller := metav1.GetControllerOf(&pod)
		if controller == nil {
			continue
		}
		owner := controller.Kind + "/" + controller.Name
		if controller.Kind == "ReplicaSet" {
			owner = replicaSets[controller.Name]
		}
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

// incidentPageInput is what the pages are built from.
type incidentPageInput struct {
	Request  IncidentRequest
	Findings []sdk.Finding
	View     detectReads
	Signals  *incidentSignals
	AppState AppStateChangeSource
	Now      time.Time
	// Since is how far before onset the page reaches; zero is incidentDefaultLead.
	Since time.Duration
	// Limit keeps only the newest rows of the changes page; zero is the cap.
	Limit int
}

func (in incidentPageInput) lead() time.Duration {
	if in.Since > 0 {
		return in.Since
	}
	return incidentDefaultLead
}

// incidentSet is the suspect set `sdo detect` shows and the onset every page
// is measured from.
func incidentSet(in incidentPageInput) (detectSuspectSet, time.Time) {
	set := buildSuspectSet(detectInput{Request: in.Request, Findings: in.Findings, View: in.View, Now: in.Now})
	if set.onset.IsZero() {
		return set, in.Now
	}
	return set, set.onset
}

// buildIncidentTimeline renders `sdo timeline`.
func buildIncidentTimeline(in incidentPageInput) []byte {
	set, onset := incidentSet(in)
	return renderTimeline(set, in.Signals.history(), in.Request, in.View, onset, in.Now, in.lead())
}

// buildIncidentChanges renders `sdo changes`.
func buildIncidentChanges(in incidentPageInput) []byte {
	set, onset := incidentSet(in)
	return renderChanges(incidentChangeRows(in, set, onset), onset)
}

func renderTimeline(set detectSuspectSet, history incidentSignalHistory, request IncidentRequest, view detectReads, onset, now time.Time, lead time.Duration) []byte {
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
	for _, node := range set.nodes {
		if node.Component.Workload == "" || len(workloads) == incidentPageWorkloads {
			continue
		}
		ref := node.Component.WorkloadRef(namespace)
		workloads = append(workloads, workloadColumn{component: node.Name, key: ref.Kind + "/" + ref.Name})
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
	first := max(-int64(lead/incidentTimelineStep), -floor(onset.Sub(now.Add(-incidentSignalHorizon))))
	last := floor(now.Sub(onset))
	rows := [][]string{header}
	for bucket := first; bucket <= last; bucket++ {
		start := onset.Add(time.Duration(bucket) * incidentTimelineStep)
		end := start.Add(incidentTimelineStep)
		row := []string{strconv.FormatInt(int64(start.Sub(onset)/time.Second), 10), start.UTC().Format(time.RFC3339)}
		for _, id := range operations {
			failed, total := 0, 0
			held := history.operations[id]
			for index := sort.Search(len(held), func(i int) bool { return !held[i].at.Before(start) }); index < len(held) && held[index].at.Before(end); index++ {
				total++
				if held[index].failed {
					failed++
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
// writer), the store-state changes and the app-state changes, from onset minus
// the lead to now, oldest first. Every value passes the summary's redaction when rendered.
func incidentChangeRows(in incidentPageInput, set detectSuspectSet, onset time.Time) []IncidentChange {
	since := onset.Add(-in.lead())
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
					Before: "accepted-source", After: string(departure.Kind), Writer: departure.Manager, Command: departure.Command(),
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
			return b.At.IsZero()
		}
		return a.At.Before(b.At)
	})
	// The cap drops the oldest timed rows; untimed rows (missing objects) sort last and stay.
	limit := incidentChangesCap
	if in.Limit > 0 {
		limit = min(limit, in.Limit)
	}
	if len(rows) > limit {
		untimed := 0
		for untimed < len(rows) && rows[len(rows)-1-untimed].At.IsZero() {
			untimed++
		}
		timed := max(limit-untimed, 0)
		rows = append(rows[len(rows)-untimed-timed:len(rows)-untimed], rows[len(rows)-untimed:]...)
		rows = rows[:min(len(rows), limit)]
	}
	return rows
}

// storeChangeRows are the store-state changes against each store's promoted
// baseline. The change itself is untimed: the row carries the time the store
// was read as ReadAt and no At, so it is never shown as an onset offset.
func storeChangeRows(stores []storestate.StoreChanges) []IncidentChange {
	var rows []IncidentChange
	for _, store := range stores {
		if store.Error != "" || !store.Baselined {
			continue
		}
		for _, change := range store.Changes {
			key, before, after := change.Describe()
			rows = append(rows, IncidentChange{ReadAt: store.ObservedAt, Source: "store", Component: store.Component, ReaderOrObject: store.Pod.Kind + "/" + store.Pod.Name, Key: key, Before: bounded(before), After: bounded(after), Noisy: change.Noisy})
		}
	}
	return rows
}

func renderChanges(rows []IncidentChange, onset time.Time) []byte {
	out := [][]string{{"t_rel_s", "at_utc", "source", "component", "reader_or_object", "key", "before", "after", "writer", "noisy", "read_at_utc"}}
	clean := func(text string) string { return redactDetectText(text, incidentPageValue) }
	for _, row := range rows {
		row = maskCredentialValues(row)
		relative, at := "", ""
		if !row.At.IsZero() {
			relative = strconv.FormatInt(int64(row.At.Sub(onset)/time.Second), 10)
			at = row.At.UTC().Format(time.RFC3339)
		}
		readAt := ""
		if !row.ReadAt.IsZero() {
			readAt = row.ReadAt.UTC().Format(time.RFC3339)
		}
		out = append(out, []string{relative, at, row.Source, clean(row.Component), clean(row.ReaderOrObject), clean(row.Key), clean(row.Before), clean(row.After), clean(row.Writer), strconv.FormatBool(row.Noisy), readAt})
	}
	return encodeCSV(out)
}

// maskCredentialValues masks a row's values when its key or object names a
// credential, whatever the values look like.
func maskCredentialValues(row IncidentChange) IncidentChange {
	if pageCredentialName(row.Key) || pageCredentialName(row.ReaderOrObject) {
		if row.Before != "" {
			row.Before = "[redacted]"
		}
		if row.After != "" {
			row.After = "[redacted]"
		}
	}
	return row
}

// relativeOffset is a time relative to onset, such as -4s, +2m05s.
func relativeOffset(d time.Duration) string {
	sign := "+"
	if d < 0 {
		sign, d = "-", -d
	}
	seconds := int64(d / time.Second)
	if seconds < 60 {
		return fmt.Sprintf("%s%ds", sign, seconds)
	}
	return fmt.Sprintf("%s%dm%02ds", sign, seconds/60, seconds%60)
}

func orNone(text string) string {
	if text == "" {
		return "(none)"
	}
	return text
}

// pageCredentialName is storeops.CredentialName widened toward refusing: a
// page row's key also counts when it names a pass, pwd, auth or private key.
func pageCredentialName(name string) bool {
	lower := strings.ToLower(name)
	if storeops.CredentialName(lower) {
		return true
	}
	for _, word := range []string{"pass", "pwd", "auth", "privatekey", "private_key", "secretkey"} {
		if strings.Contains(lower, word) {
			return true
		}
	}
	return false
}

func encodeCSV(rows [][]string) []byte {
	var buffer bytes.Buffer
	writer := csv.NewWriter(&buffer)
	_ = writer.WriteAll(rows)
	return buffer.Bytes()
}

package runtime

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"

	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/storestate"
	"sdo.dev/operator/sdk/traffic"
)

// detectSummaryCap bounds the text `sdo detect` prints.
const detectSummaryCap = 2048

const (
	detectLogWindow      = 5 * time.Minute
	detectLogReadTimeout = 2 * time.Second
	detectLogTotal       = 4 * time.Second
	detectLogBytes       = 64 << 10
	detectItemCap        = 6000
	detectFullSuspects   = 2
	detectShownTests     = 3
	// detectCandidateFloor is the room `sdo detect` keeps for the candidate view.
	detectCandidateFloor = 800
)

// detectReads is what the summary reads: the frozen snapshot, the app map and
// bounded container logs. A program.ReadView satisfies it.
type detectReads interface {
	sdk.DetectionContext
	traffic.Source
	traffic.WindowSet
	AppMap() (program.AppMap, error)
	LogsWithOptions(sdk.ObjectRef, corev1.PodLogOptions) (string, error)
}

// detectInput is everything the summary is built from.
type detectInput struct {
	Request  IncidentRequest
	Findings []sdk.Finding // active now
	View     detectReads
	Now      time.Time
	// Recovered is the incident's verified marker.
	Recovered bool
	// AppState supplies the app-state changes placed on the candidates; nil lists none.
	AppState AppStateChangeSource
}

// detectSummary is the compact text and the full text of each item a `more:`
// line points at.
type detectSummary struct {
	Text  string
	Items map[string]string
}

var (
	logErrorLine   = regexp.MustCompile(`(?i)\b(error|exception|fatal|panic|denied|refused|unauthori[sz]ed|forbidden|timed? ?out|unreachable|too many)\b`)
	redactSecret   = regexp.MustCompile(`(?i)((?:password|passwd|token|secret|authorization|api[-_]?key)["']?\s*[=:]\s*)\S+`)
	redactBearer   = regexp.MustCompile(`(?i)\b(bearer|basic)\s+\S+`)
	redactLongToke = regexp.MustCompile(`[A-Za-z0-9+/_=-]{32,}`)
)

// redactDetectText collapses whitespace, masks credential-shaped text and
// bounds the result to limit runes.
func redactDetectText(text string, limit int) string {
	text = strings.Join(strings.Fields(text), " ")
	text = redactSecret.ReplaceAllString(text, "${1}[redacted]")
	text = redactBearer.ReplaceAllString(text, "$1 [redacted]")
	text = redactLongToke.ReplaceAllString(text, "[redacted]")
	if runes := []rune(text); len(runes) > limit {
		return string(runes[:limit-1]) + "…"
	}
	return text
}

type failingTest struct {
	id        string
	path      []string // component names along the scenario's path
	step      string
	label     string // what the user sees: "HTTP 429", "dial failed", "timeout"
	err       string
	onset     time.Time
	failures  int
	samples   int
	last      []traffic.Sample
	scenarios traffic.Descriptor
	latestOK  bool // the newest sample passed
}

// failureLabel is the user-facing result of one failed sample.
func failureLabel(sample traffic.Sample) string {
	switch {
	case sample.Outcome == traffic.OutcomeTimeout && sample.DialFailed:
		return "dial timeout"
	case sample.Outcome == traffic.OutcomeTimeout:
		return "timeout"
	case sample.DialFailed:
		return "dial failed"
	case sample.Exit != nil:
		return fmt.Sprintf("exit %d", *sample.Exit)
	case sample.Status > 0:
		return fmt.Sprintf("HTTP %d", sample.Status)
	}
	return "error"
}

func scenarioServices(scenario traffic.Descriptor) []string {
	var path []string
	for _, service := range append([]string{scenario.Target.Service}, scenario.DependsOn...) {
		if service != "" && !slices.Contains(path, service) {
			path = append(path, service)
		}
	}
	return path
}

// componentForService names the app-map component that serves a Service, or
// the Service itself when the map does not know it.
func componentForService(m program.AppMap, service string) program.AppComponent {
	for _, component := range m.Components {
		if component.Name == service || slices.Contains(component.Services, service) {
			return component
		}
	}
	return program.AppComponent{Name: service}
}

// testJudgement is the user tests as the traffic windows judge them.
type testJudgement struct {
	// failing are the tests judged failing: the group an incident opens on.
	failing []failingTest
	// alsoFailing are tests not judged failing that have a failed sample in
	// the window (healthy, or too few samples to judge): a side-effect page
	// leaves the real failing operation here.
	alsoFailing  []failingTest
	passing      []string // judged healthy with no failed sample
	unevaluated  []string // no failed sample and too few samples to judge
	tests        []candidateTest
	disagreement []string // tests whose live traffic reaches components their mapped path lacks
}

// judgedTests reads the user tests from the traffic windows. g is the map the
// accepted architecture gives, or nil: a test's path is the union of the
// operation paths of the probe the map names for it and the components its
// traffic descriptor reaches, or the descriptor's alone when the map names no
// probe.
func judgedTests(view detectReads, m program.AppMap, g *mapGraph) testJudgement {
	var out testJudgement
	windows := view.TrafficWindows()
	names := make([]string, 0, len(windows))
	for name := range windows {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		window := windows[name]
		samples := map[string][]traffic.Sample{}
		descriptors := map[string]traffic.Descriptor{}
		for _, observed := range window.Scenarios {
			samples[observed.Scenario.ID] = observed.Samples
			descriptors[observed.Scenario.ID] = observed.Scenario
		}
		judged := map[string]traffic.SLOVerdict{}
		for _, verdict := range traffic.Judge(window) {
			judged[verdict.ScenarioID] = verdict
		}
		ids := make([]string, 0, len(descriptors))
		for id := range descriptors {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			verdict, ok := judged[id]
			var described []string
			for _, service := range scenarioServices(descriptors[id]) {
				described = append(described, componentForService(m, service).Name)
			}
			path, source := described, "descriptor"
			if g != nil {
				if mapped := g.testPath(id); len(mapped) > 0 {
					path, source = mapped, "map"
					for _, name := range described {
						if !slices.Contains(mapped, name) {
							path = append(path, name)
							out.disagreement = append(out.disagreement, fmt.Sprintf("%s reaches %s", id, name))
						}
					}
				}
			}
			series := samples[id]
			failedSamples := 0
			for _, sample := range series {
				if sample.Outcome != traffic.OutcomeOK && sample.Outcome != traffic.OutcomeInvalid {
					failedSamples++
				}
			}
			start := len(series)
			for start > 0 && series[start-1].Outcome != traffic.OutcomeOK {
				start--
			}
			test := failingTest{id: id, scenarios: descriptors[id], failures: verdict.Errors + verdict.Timeouts, samples: verdict.Samples, path: described}
			if len(series) > 0 {
				test.latestOK = series[len(series)-1].Outcome == traffic.OutcomeOK
			}
			if start < len(series) {
				test.onset = series[start].At
			}
			for _, sample := range series[start:] {
				if sample.Outcome != traffic.OutcomeInvalid {
					test.last = append(test.last, sample)
				}
			}
			if len(test.last) > 3 {
				test.last = test.last[len(test.last)-3:]
			}
			if n := len(test.last); n > 0 {
				latest := test.last[n-1]
				test.step, test.label, test.err = latest.Step, failureLabel(latest), latest.Error
			} else if len(verdict.RecentFailures) > 0 {
				latest := verdict.RecentFailures[len(verdict.RecentFailures)-1]
				test.step, test.label, test.err = latest.Step, failureLabel(latest), latest.Error
			}
			if test.label == "" {
				test.label = "error"
			}
			entry := candidateTest{ID: id, Symptom: test.label, Path: path, Source: source, Failures: test.failures, Samples: test.samples, LatestOK: test.latestOK}
			switch {
			case ok && verdict.Evaluated && !verdict.Healthy:
				entry.State = testFailing
				out.failing = append(out.failing, test)
			case failedSamples > 0:
				entry.State = testAlsoFailing
				if entry.Failures == 0 {
					entry.Failures, entry.Samples = failedSamples, len(series)
				}
				test.failures, test.samples = entry.Failures, entry.Samples
				out.alsoFailing = append(out.alsoFailing, test)
			case !ok || !verdict.Evaluated:
				entry.State = testUnevaluated
				out.unevaluated = append(out.unevaluated, id)
			default:
				entry.State = testPassing
				out.passing = append(out.passing, id)
			}
			out.tests = append(out.tests, entry)
		}
	}
	// The group that opened the incident prints last when its tests pass again.
	sort.SliceStable(out.failing, func(i, j int) bool { return !out.failing[i].latestOK && out.failing[j].latestOK })
	return out
}

// workloadObjects are the objects whose change can explain a workload's
// behaviour: the workload, the Services that select its Pods, the policies,
// autoscalers and budgets that select or target it, and the ConfigMaps and
// Secrets its Pod template references.
func workloadObjects(view sdk.DetectionContext, workload sdk.ObjectRef) []sdk.ObjectRef {
	var template *corev1.PodTemplateSpec
	for _, deployment := range view.Deployments() {
		if workload.Kind == "Deployment" && deployment.Name == workload.Name {
			template = &deployment.Spec.Template
		}
	}
	for _, statefulSet := range view.StatefulSets() {
		if workload.Kind == "StatefulSet" && statefulSet.Name == workload.Name {
			template = &statefulSet.Spec.Template
		}
	}
	refs := []sdk.ObjectRef{workload}
	if template == nil {
		return refs
	}
	ns := workload.Namespace
	add := func(apiVersion, kind, name string) {
		refs = append(refs, sdk.ObjectRef{APIVersion: apiVersion, Kind: kind, Namespace: ns, Name: name})
	}
	selects := func(selector map[string]string) bool {
		if len(selector) == 0 {
			return false
		}
		for key, value := range selector {
			if template.Labels[key] != value {
				return false
			}
		}
		return true
	}
	for _, service := range view.Services() {
		if selects(service.Spec.Selector) {
			add("v1", "Service", service.Name)
		}
	}
	for _, policy := range view.NetworkPolicies() {
		selector, err := metav1.LabelSelectorAsMap(&policy.Spec.PodSelector)
		if err == nil && (len(selector) == 0 && len(policy.Spec.PodSelector.MatchExpressions) == 0 || selects(selector)) {
			add("networking.k8s.io/v1", "NetworkPolicy", policy.Name)
		}
	}
	for _, hpa := range view.HorizontalPodAutoscalers() {
		if hpa.Spec.ScaleTargetRef.Kind == workload.Kind && hpa.Spec.ScaleTargetRef.Name == workload.Name {
			add("autoscaling/v2", "HorizontalPodAutoscaler", hpa.Name)
		}
	}
	for _, budget := range view.PodDisruptionBudgets() {
		if budget.Spec.Selector != nil {
			if selector, err := metav1.LabelSelectorAsMap(budget.Spec.Selector); err == nil && selects(selector) {
				add("policy/v1", "PodDisruptionBudget", budget.Name)
			}
		}
	}
	for _, volume := range template.Spec.Volumes {
		if volume.ConfigMap != nil {
			add("v1", "ConfigMap", volume.ConfigMap.Name)
		}
		if volume.Secret != nil {
			add("v1", "Secret", volume.Secret.SecretName)
		}
	}
	for _, container := range append(append([]corev1.Container{}, template.Spec.InitContainers...), template.Spec.Containers...) {
		for _, from := range container.EnvFrom {
			if from.ConfigMapRef != nil {
				add("v1", "ConfigMap", from.ConfigMapRef.Name)
			}
			if from.SecretRef != nil {
				add("v1", "Secret", from.SecretRef.Name)
			}
		}
		for _, env := range container.Env {
			if env.ValueFrom == nil {
				continue
			}
			if env.ValueFrom.ConfigMapKeyRef != nil {
				add("v1", "ConfigMap", env.ValueFrom.ConfigMapKeyRef.Name)
			}
			if env.ValueFrom.SecretKeyRef != nil {
				add("v1", "Secret", env.ValueFrom.SecretKeyRef.Name)
			}
		}
	}
	return refs
}

func refText(ref sdk.ObjectRef) string {
	return ref.Kind + "/" + ref.Namespace + "/" + ref.Name
}

func hms(at time.Time) string {
	if at.IsZero() {
		return "?"
	}
	return at.UTC().Format("15:04:05Z")
}

// suspectLogs reads the bounded error counts of one candidate: before and after
// onset in the newest Pod's first two containers and the previous container of
// a restarted one. deadline bounds the whole read.
func suspectLogs(in detectInput, workload sdk.ObjectRef, onset time.Time, deadline time.Time) (line, command string) {
	if onset.IsZero() || in.View == nil {
		return "", ""
	}
	var newest *corev1.Pod
	for _, pod := range in.View.Pods() {
		pod := pod
		ref := sdk.ObjectRef{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name}
		if program.WorkloadOf(in.View, ref) != workload.Kind+"/"+workload.Name {
			continue
		}
		if newest == nil || pod.CreationTimestamp.After(newest.CreationTimestamp.Time) {
			newest = &pod
		}
	}
	if newest == nil {
		return "", ""
	}
	span := min(in.Now.Sub(onset), detectLogWindow)
	if span < 0 {
		span = 0
	}
	since := metav1.NewTime(onset.Add(-span))
	pod := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: newest.Namespace, Name: newest.Name}
	type read struct {
		container string
		previous  bool
	}
	var reads []read
	for index, container := range newest.Spec.Containers {
		if index == 2 {
			break
		}
		reads = append(reads, read{container: container.Name})
	}
	for _, status := range newest.Status.ContainerStatuses {
		if status.RestartCount > 0 {
			reads = append(reads, read{container: status.Name, previous: true})
		}
	}
	before, after, timedOut := 0, 0, false
	sample := ""
	limit := int64(detectLogBytes)
	for _, item := range reads {
		if lifecycleio.Now().After(deadline) {
			timedOut = true
			break
		}
		type result struct {
			text string
			err  error
		}
		done := make(chan result, 1)
		options := corev1.PodLogOptions{Container: item.container, SinceTime: &since, Timestamps: true, LimitBytes: &limit, Previous: item.previous}
		go func() {
			text, err := in.View.LogsWithOptions(pod, options)
			done <- result{text, err}
		}()
		select {
		case got := <-done:
			if got.err != nil {
				continue
			}
			for _, raw := range strings.Split(got.text, "\n") {
				stamp, rest, _ := strings.Cut(raw, " ")
				if !logErrorLine.MatchString(rest) {
					continue
				}
				if at, err := time.Parse(time.RFC3339Nano, stamp); err == nil && at.Before(onset) {
					before++
				} else {
					after++
					if sample == "" {
						sample = rest
					}
				}
			}
		case <-lifecycleio.After(detectLogReadTimeout):
			timedOut = true
		}
	}
	command = fmt.Sprintf("kubectl -n %s logs %s/%s --since-time=%s", workload.Namespace, strings.ToLower(workload.Kind), workload.Name, onset.UTC().Format(time.RFC3339))
	if timedOut && before+after == 0 {
		return "logs: unavailable (timeout)", command
	}
	if before+after == 0 {
		return "logs: no error lines near onset", command
	}
	line = fmt.Sprintf("logs: %d error lines since onset, %d before", after, before)
	if sample != "" {
		line += fmt.Sprintf(": %q", redactDetectText(sample, 70))
	}
	return line, command
}

// shortID is the six-hex handle of a clue in `more:` lines.
func shortID(fingerprint string) string {
	sum := sha256.Sum256([]byte(fingerprint))
	return hex.EncodeToString(sum[:3])
}

func objectOfFinding(finding sdk.Finding) []sdk.ObjectRef {
	refs := []sdk.ObjectRef{finding.PrimaryResource}
	refs = append(refs, finding.RelatedResources...)
	return refs
}

// detectSuspectSet is what the incident points at: the judged user tests, the
// candidate nodes placed on the map, and the opening findings and clues. Both
// `sdo detect`, the first message and the built-in restore playbook's
// candidates read it, so they name the same nodes.
type detectSuspectSet struct {
	m                      program.AppMap
	graph                  *mapGraph
	mapNote                string
	mapLines               []string
	judged                 testJudgement
	failing                []failingTest
	passing, unevaluated   []string
	openingFindings, clues []sdk.Finding
	conditions             []string
	onset                  time.Time
	// nodes are the candidates in position order, before any evidence ranks them.
	nodes []*candidateNode
}

// loadCandidateGraph is the one map source of the candidate view: the accepted
// `.sdo/arch.md` at the accepted commit, parsed once. A view that cannot hand
// over the document (a replay or a capture) falls back to its AppMap read,
// which is the same document's components and edges without operations or
// probes. The program-level map is derived from the graph, so the two never
// disagree.
func loadCandidateGraph(view detectReads) (*mapGraph, program.AppMap, string) {
	var note string
	if doc, ok, err := architectureOf(view); ok {
		if err != nil {
			note = err.Error()
		} else if g, err := newMapGraph(doc); err != nil {
			note = err.Error()
		} else {
			return &g, g.programMap(), ""
		}
	}
	m, err := view.AppMap()
	if err == nil && len(m.Components) > 0 {
		g := graphFromProgramMap(m)
		return &g, g.programMap(), ""
	}
	if note == "" {
		note = "no application map"
		if err != nil {
			note = err.Error()
		}
	}
	return nil, program.AppMap{}, note
}

// architectureOf finds the accepted architecture document behind a read view,
// through the wrappers that cache or record its reads.
func architectureOf(view any) (doc string, ok bool, err error) {
	for depth := 0; depth < 8 && view != nil; depth++ {
		if reader, found := view.(architectureReader); found {
			doc, err = reader.ArchitectureText()
			return doc, true, err
		}
		switch v := view.(type) {
		case *CycleReadView:
			view = v.RecordingReadView.ReadView
		case interface{ UnderlyingReadView() program.ReadView }:
			view = v.UnderlyingReadView()
		default:
			return "", false, nil
		}
	}
	return "", false, nil
}

func buildSuspectSet(in detectInput) detectSuspectSet {
	g, m, note := loadCandidateGraph(in.View)
	judged := judgedTests(in.View, m, g)
	opening := alertKeys(&in.Request)
	var openingFindings, clues []sdk.Finding
	for _, finding := range in.Findings {
		if slices.Contains(opening, findingKey(finding)) {
			openingFindings = append(openingFindings, finding)
		} else {
			clues = append(clues, finding)
		}
	}
	onset := in.Request.DetectedAt
	for _, test := range append(slices.Clone(judged.failing), judged.alsoFailing...) {
		if !test.onset.IsZero() && (onset.IsZero() || test.onset.Before(onset)) {
			onset = test.onset
		}
	}
	var conditions []string
	for _, finding := range openingFindings {
		if isProbeFinding(finding) {
			continue
		}
		if component, ok := m.ComponentOf(in.View, finding.PrimaryResource); ok && !slices.Contains(conditions, component.Name) {
			conditions = append(conditions, component.Name)
		}
	}
	set := detectSuspectSet{m: m, graph: g, mapNote: note, judged: judged, failing: judged.failing, passing: judged.passing, unevaluated: judged.unevaluated, openingFindings: openingFindings, clues: clues, conditions: conditions, onset: onset}
	if len(judged.disagreement) > 0 {
		line := fmt.Sprintf("map: live traffic: %s, not on its mapped path", judged.disagreement[0])
		if n := len(judged.disagreement) - 1; n > 0 {
			line += fmt.Sprintf(" (+%d more)", n)
		}
		set.mapLines = []string{line}
	}
	set.nodes = computeCandidates(set.candidateInput(in, nil)).Nodes
	return set
}

// candidateInput is the computation's input for a set; the callers that render
// evidence add rows and extra evidence.
func (set detectSuspectSet) candidateInput(in detectInput, rows []IncidentChange) candidateInput {
	return candidateInput{Graph: set.graph, MapNote: set.mapNote, MapLines: set.mapLines, Conditions: set.conditions, Tests: set.judged.tests, Onset: set.onset, Now: in.Now, Rows: rows, Components: set.m, SourceChecked: in.View != nil && !in.Request.DeploymentInFlight}
}

// storeObservation says, for each component that has a store-state read,
// whether its state was observed: "" when it was, else why not.
func storeObservation(view sdk.DetectionContext) map[string]string {
	out := map[string]string{}
	if view == nil {
		return out
	}
	for _, store := range storestate.Changes(view) {
		switch {
		case store.Error != "":
			if prev, seen := out[store.Component]; !seen || prev != "" {
				out[store.Component] = "store read failed: " + redactDetectText(store.Error, 70)
			}
		case !store.Baselined:
			if _, seen := out[store.Component]; !seen {
				out[store.Component] = "no baseline yet"
			}
		default:
			out[store.Component] = ""
		}
	}
	return out
}

// candidateEvidenceFor gathers what the change rows do not carry: log errors on
// the leading workloads, object differences against the last healthy state,
// clues that resolve to a node and the playbook that matched it.
func candidateEvidenceFor(in detectInput, set detectSuspectSet, rows []IncidentChange, namespace string) (extra map[string][]candidateEvidence, other []sdk.Finding, items map[string]string) {
	extra = map[string][]candidateEvidence{}
	items = map[string]string{}
	byName := map[string]*candidateNode{}
	for _, n := range set.nodes {
		byName[n.Name] = n
	}
	// Object differences against the last healthy baseline; a departure row
	// already names the object, so it is not listed twice.
	if changes := in.Request.StateChanges; changes != nil && in.View != nil {
		for _, change := range changes.Changes {
			if change.Kind == StoreStateChangeKind {
				continue
			}
			for _, n := range set.nodes {
				if n.Component.Workload == "" {
					continue
				}
				objects := componentObjects(in.View, n.Component, namespace)
				if !slices.ContainsFunc(objects, func(ref sdk.ObjectRef) bool { return ref.Kind == change.Kind && ref.Name == change.Name }) {
					continue
				}
				name := change.Kind + "/" + change.Name
				if slices.ContainsFunc(rows, func(row IncidentChange) bool { return row.Component == n.Name && row.ReaderOrObject == name }) {
					continue
				}
				extra[n.Name] = append(extra[n.Name], candidateEvidence{Kind: "state", Unexplained: true, Text: fmt.Sprintf("%s %s since the last healthy state  (read %s, change untimed)", name, redactDetectText(string(change.Change), 30), hms(changes.ObservedAt))})
			}
		}
	}
	// Drift events name the manager and time of a change even when the source
	// comparison shows no difference; one made after onset is a consequence.
	for _, event := range in.Request.DriftEvents {
		if sdk.IsSDOFieldManager(event.Manager) || strings.HasPrefix(event.Manager, "sdo") || in.View == nil {
			continue
		}
		at := event.ObservedAt
		if event.ManagerAt != nil {
			at = *event.ManagerAt
		}
		if !set.onset.IsZero() && at.After(set.onset.Add(departureOnsetSlack)) {
			continue
		}
		name := event.Ref.Kind + "/" + event.Ref.Name
		for _, n := range set.nodes {
			if n.Component.Workload == "" || slices.ContainsFunc(rows, func(row IncidentChange) bool { return row.Component == n.Name && row.ReaderOrObject == name }) {
				continue
			}
			if !slices.ContainsFunc(componentObjects(in.View, n.Component, namespace), func(ref sdk.ObjectRef) bool { return ref.Kind == event.Ref.Kind && ref.Name == event.Ref.Name }) {
				continue
			}
			var fields []string
			for _, field := range event.Fields {
				if len(fields) < 2 {
					fields = append(fields, redactDetectText(field.Path, 40))
				}
			}
			row := IncidentChange{At: at, Source: "kubernetes", Component: n.Name, ReaderOrObject: name, Key: strings.Join(fields, ";"), After: "changed", Writer: orQuestion(event.Manager), Command: "sdo source " + refText(event.Ref)}
			if e, ok := rowEvidence(row, set.onset); ok {
				extra[n.Name] = append(extra[n.Name], e)
			}
		}
	}
	for _, clue := range set.clues {
		placed := false
		for _, ref := range objectOfFinding(clue) {
			if component, ok := set.m.ComponentOf(in.View, ref); ok && byName[component.Name] != nil {
				extra[component.Name] = append(extra[component.Name], candidateEvidence{Kind: "clue", Unexplained: true, Text: "clue: " + redactDetectText(clue.Summary, 100) + "  more: sdo detect " + shortID(FindingFingerprint(clue))})
				placed = true
				break
			}
		}
		items[shortID(FindingFingerprint(clue))] = redactDetectText(clue.Summary+"\n"+clue.Evidence, detectItemCap)
		if !placed {
			other = append(other, clue)
		}
	}
	for _, n := range set.nodes {
		if match := playbookFor(in, set.m, n.Name); match != nil {
			extra[n.Name] = append(extra[n.Name], candidateEvidence{Kind: "playbook", Text: "playbooks: " + match.GetPlaybookId() + "  more: " + strings.Replace(match.GetMitigateCommand(), "sdo mitigate", "sdo diagnose", 1)})
		}
	}
	// Logs: the leading workloads by the evidence so far, under one deadline.
	if in.View == nil {
		return extra, other, items
	}
	ranked := set.candidateInput(in, rows)
	ranked.Extra = extra
	deadline := lifecycleio.Now().Add(detectLogTotal)
	read := 0
	for _, n := range computeCandidates(ranked).Nodes {
		if read == detectFullSuspects {
			break
		}
		if n.Component.Workload == "" {
			continue
		}
		read++
		line, _ := suspectLogs(in, n.Component.WorkloadRef(namespace), set.onset, deadline)
		switch {
		case line == "":
		case strings.HasPrefix(line, "logs: ") && strings.Contains(line, " error lines since onset") && !strings.HasPrefix(line, "logs: 0 "):
			extra[n.Name] = append(extra[n.Name], candidateEvidence{Kind: "logs", Unexplained: true, Text: line})
		default:
			extra[n.Name] = append(extra[n.Name], candidateEvidence{Kind: "note", Text: line})
		}
	}
	return extra, other, items
}

// candidateViewFor computes the ranked view with all of its evidence, and the
// drill-down items of the view and of the clues.
func candidateViewFor(in detectInput, set detectSuspectSet) (candidateView, map[string]string) {
	namespace := in.Request.Namespace
	if namespace == "" && in.View != nil {
		namespace = in.View.Namespace()
	}
	rows := incidentChangeRows(incidentPageInput{Request: in.Request, Findings: in.Findings, View: in.View, AppState: in.AppState, Now: in.Now}, set, set.onset)
	extra, other, items := candidateEvidenceFor(in, set, rows, namespace)
	input := set.candidateInput(in, rows)
	input.Extra = extra
	input.Observed = storeObservation(in.View)
	if source, ok := in.AppState.(appStateReaderSource); ok && in.View != nil {
		for component, reason := range appStateCoverage(in.View, set.m, namespace, source.AppStateReaders()) {
			if _, seen := input.Observed[component]; !seen || reason == "" {
				input.Observed[component] = reason
			}
		}
	}
	view := computeCandidates(input)
	for _, clue := range other {
		view.otherClues = append(view.otherClues, redactDetectText(clue.Summary, 90))
	}
	for key, text := range view.items() {
		items[key] = text
	}
	return view, items
}

// incidentCandidateBlock is what the first message carries (the IncidentRequest
// recent_changes slot): the candidate view as `sdo detect` renders it, then a
// pointer to the live commands. Both call computeCandidates.
func incidentCandidateBlock(in incidentPageInput) []string {
	detect := detectInput{Request: in.Request, Findings: in.Findings, View: in.View, Now: in.Now, AppState: in.AppState}
	set := buildSuspectSet(detect)
	if set.onset.IsZero() {
		set.onset = in.Now
	}
	view, _ := candidateViewFor(detect, set)
	const pointer = "live: sdo detect (this view); sdo changes and sdo timeline list every row"
	text := view.render(candidateRenderCap - len(pointer) - 1)
	if text == "" {
		return []string{pointer}
	}
	return append(strings.Split(text, "\n"), pointer)
}

// buildDetectSummary renders the compact incident summary: what users see
// first, then the candidate nodes with their evidence, what is elsewhere and
// what was not observed. It never exceeds detectSummaryCap bytes.
func buildDetectSummary(in detectInput) detectSummary {
	items := map[string]string{}
	set := buildSuspectSet(in)
	failing, passing, unevaluated := set.failing, set.passing, set.unevaluated
	openingFindings, onset := set.openingFindings, set.onset
	view, viewItems := candidateViewFor(in, set)
	for key, text := range viewItems {
		items[key] = text
	}

	state := "no"
	if in.Recovered {
		state = "yes"
	}
	active := len(openingFindings)
	if active == 0 {
		active = len(failing)
	}
	if in.Recovered {
		active = 0
	}
	opened := "Kubernetes condition"
	switch {
	case in.Request.ExternalMessage != "":
		opened = "page"
	case len(failing) > 0:
		opened = "user tests"
	}
	header := []string{
		fmt.Sprintf("incident %s  opened on: %s  onset %s  now %s", in.Request.IncidentID, opened, hms(onset), hms(in.Now)),
		freshnessLine(in.View, in.Now),
		fmt.Sprintf("recovered: %s (%d alerts active)", state, active),
	}
	if line := alertSetLine(&in.Request); line != "" {
		header = append(header, line)
	}
	for _, note := range in.Request.PriorStages {
		for _, mitigation := range note.GetMitigations() {
			if mitigation.GetResult() != "skipped" {
				header = append(header, fmt.Sprintf("stage: by sdo (playbook %s): %s", mitigation.GetPlaybookId(), redactDetectText(mitigation.GetResult(), 40)))
			}
		}
	}

	testBlock := func(test failingTest, role string) ([]string, string) {
		since := ""
		if !test.onset.IsZero() {
			since = " since " + hms(test.onset)
		}
		lines := []string{
			fmt.Sprintf("%s%s  %s %q  step %s", test.id, role, test.label, redactDetectText(test.err, 60), orQuestion(redactDetectText(test.step, 30))),
			fmt.Sprintf("  %s  %d/%d failing%s", orQuestion(redactDetectText(strings.Join(test.path, ">"), 100)), test.failures, test.samples, since),
			"  more: sdo detect " + test.id,
		}
		var detail strings.Builder
		fmt.Fprintf(&detail, "%s: %s\npath: %s\nsince %s, %d/%d failing\nlast failing samples:\n", test.id, test.scenarios.Description, strings.Join(test.path, ">"), hms(test.onset), test.failures, test.samples)
		for _, sample := range test.last {
			fmt.Fprintf(&detail, "  %s step %s %s: %s %s\n", hms(sample.At), orQuestion(sample.Step), sample.Request, failureLabel(sample), redactDetectText(sample.Error, 200))
		}
		return lines, detail.String()
	}
	var testBlocks, alsoBlocks [][]string
	var allTests strings.Builder
	for _, test := range failing {
		lines, detail := testBlock(test, "")
		testBlocks = append(testBlocks, lines)
		items[test.id] = detail
		allTests.WriteString(detail + "\n")
	}
	for _, test := range set.judged.alsoFailing {
		lines, detail := testBlock(test, "  (also failing)")
		alsoBlocks = append(alsoBlocks, lines)
		items[test.id] = detail
		allTests.WriteString(detail + "\n")
	}
	items["tests"] = allTests.String()
	var conditionBlocks [][]string
	var allConditions strings.Builder
	for _, finding := range openingFindings {
		if isProbeFinding(finding) {
			continue
		}
		ref := finding.PrimaryResource
		conditionBlocks = append(conditionBlocks, []string{
			fmt.Sprintf("%s/%s  %s", ref.Kind, ref.Name, redactDetectText(finding.Summary, 110)),
			fmt.Sprintf("  more: kubectl -n %s describe %s %s", ref.Namespace, strings.ToLower(ref.Kind), ref.Name),
		})
		items[shortID(FindingFingerprint(finding))] = redactDetectText(finding.Summary+"\n"+finding.Evidence, detectItemCap)
		allConditions.WriteString(shortID(FindingFingerprint(finding)) + "  " + redactDetectText(finding.Summary, 200) + "\n")
	}
	items["conditions"] = allConditions.String()
	var backgroundAll strings.Builder
	for _, clue := range set.clues {
		backgroundAll.WriteString(shortID(FindingFingerprint(clue)) + "  " + redactDetectText(clue.Summary, 200) + "\n")
	}
	items["background"] = backgroundAll.String()

	shownTests, shownConditions, shownAlso := min(len(testBlocks), detectShownTests), min(len(conditionBlocks), detectShownTests), min(len(alsoBlocks), 2)
	showPassing := true
	observation := func() string {
		out := slices.Clone(header)
		if len(testBlocks)+len(conditionBlocks) > 0 {
			out = append(out, "", "OPENED ON")
		}
		for _, block := range testBlocks[:shownTests] {
			out = append(out, block...)
		}
		if dropped := len(testBlocks) - shownTests; dropped > 0 {
			out = append(out, fmt.Sprintf("+%d more: sdo detect tests", dropped))
		}
		for _, block := range conditionBlocks[:shownConditions] {
			out = append(out, block...)
		}
		if dropped := len(conditionBlocks) - shownConditions; dropped > 0 {
			out = append(out, fmt.Sprintf("+%d more: sdo detect conditions", dropped))
		}
		for _, block := range alsoBlocks[:shownAlso] {
			out = append(out, block...)
		}
		if dropped := len(alsoBlocks) - shownAlso; dropped > 0 {
			out = append(out, fmt.Sprintf("+%d more also failing: sdo detect tests", dropped))
		}
		if showPassing && len(passing) > 0 {
			out = append(out, "passing: "+redactDetectText(strings.Join(passing, " "), 160))
		} else if !showPassing && len(passing) > 0 {
			out = append(out, fmt.Sprintf("passing: %d tests", len(passing)))
		}
		if len(unevaluated) > 0 {
			out = append(out, "not yet evaluated: "+redactDetectText(strings.Join(unevaluated, " "), 100))
		}
		return strings.Join(out, "\n")
	}
shrink:
	for len(observation()) > detectSummaryCap-detectCandidateFloor {
		switch {
		case shownAlso > 0:
			shownAlso--
		case shownConditions > 1:
			shownConditions--
		case shownTests > 1:
			shownTests--
		case showPassing:
			showPassing = false
		default:
			break shrink
		}
	}
	text := observation()
	if rest := view.render(detectSummaryCap - len(text) - 2); rest != "" {
		text += "\n\n" + rest
	}
	if len(text) > detectSummaryCap {
		cut := text[:detectSummaryCap-40]
		for len(cut) > 0 && !strings.HasSuffix(cut, "\n") && cut[len(cut)-1] >= 0x80 {
			cut = cut[:len(cut)-1]
		}
		text = cut + "\n+more: sdo detect --json"
	}
	return detectSummary{Text: text, Items: items}
}

// freshnessLine says how current the summary is: the detectors ran on reads
// taken now, and the newest user-test request the prober recorded.
func freshnessLine(view detectReads, now time.Time) string {
	var newest time.Time
	for _, window := range view.TrafficWindows() {
		for _, observed := range window.Scenarios {
			for _, sample := range observed.Samples {
				if sample.At.After(newest) {
					newest = sample.At
				}
			}
		}
	}
	line := "fresh: detectors ran on reads taken " + hms(now)
	if newest.IsZero() {
		return line + "; no user-test request recorded yet"
	}
	return fmt.Sprintf("%s; newest user-test request %s (%s ago)", line, hms(newest), now.Sub(newest).Round(time.Second))
}

// playbookFor is the first playbook whose Diagnose matched on the component.
func playbookFor(in detectInput, m program.AppMap, component string) *contracts.PlaybookMatch {
	for _, match := range in.Request.PlaybookCoverage.GetMatches() {
		if match.GetMitigateCommand() == "" {
			continue
		}
		for _, binding := range match.GetBindings() {
			// The built-in source-restore playbook binds a component by name.
			if match.GetPlaybookId() == SourceRestoreID && binding.GetName() == SourceRestoreComponentParam && binding.GetValue().GetStringValue() == component {
				return match
			}
			ref := binding.GetObject()
			if ref == nil {
				continue
			}
			found, ok := m.ComponentOf(in.View, sdk.ObjectRef{Kind: ref.GetKind(), Namespace: ref.GetNamespace(), Name: ref.GetName()})
			if ok && found.Name == component {
				return match
			}
		}
	}
	return nil
}

func orQuestion(text string) string {
	if text == "" {
		return "?"
	}
	return text
}

// item resolves `sdo detect NAME` to a summary item: a test, component, clue prefix,
// `tests` or `background`.
func (s detectSummary) item(id string) (string, bool) {
	if text, ok := s.Items[id]; ok {
		return text, true
	}
	keys := make([]string, 0, len(s.Items))
	for key := range s.Items {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(id) >= 3 {
		for _, key := range keys {
			if strings.HasPrefix(key, id) {
				return s.Items[key], true
			}
		}
	}
	return "", false
}

// detectIncidentRecovered reports whether the incident's verified marker is set.
func (l *ResponderLoop) detectIncidentRecovered() bool {
	return l.IncidentStatus != nil && l.IncidentStatus() != nil
}

// alertSetLine says what Recovered waits on when user tests joined the
// opening alerts: the whole set must clear and stay clear. "" when none joined.
func alertSetLine(request *IncidentRequest) string {
	if request == nil || len(request.JoinedKeys) == 0 {
		return ""
	}
	var names []string
	for _, finding := range request.Findings {
		if slices.Contains(request.JoinedKeys, findingKey(finding)) {
			names = append(names, strings.TrimPrefix(finding.RuleID, traffic.RuleIDPrefix))
		}
	}
	sort.Strings(names)
	return fmt.Sprintf("alert set: %d opening + %d joined user tests (%s); recovered needs all of them clear and staying clear", len(openingKeys(request)), len(names), strings.Join(names, ", "))
}

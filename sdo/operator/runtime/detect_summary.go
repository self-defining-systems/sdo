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
}

type detectSuspect struct {
	component program.AppComponent
	passing   bool // also on passing paths
	// everyPath is true for a component on every failing path, or the workload of an opening condition.
	everyPath bool
	changeAt  time.Time
	change    string
	changeCmd string
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

// judgedTests reads the user tests from the traffic windows: the failing ones
// with their last failing samples and failure onset, and the passing ones.
func judgedTests(view detectReads, m program.AppMap) (failing []failingTest, passing, unevaluated []string, passingServices map[string]bool) {
	passingServices = map[string]bool{}
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
			switch {
			case !ok || !verdict.Evaluated:
				unevaluated = append(unevaluated, id)
			case verdict.Healthy:
				passing = append(passing, id)
				for _, service := range scenarioServices(descriptors[id]) {
					passingServices[service] = true
				}
			default:
				test := failingTest{id: id, scenarios: descriptors[id], failures: verdict.Errors + verdict.Timeouts, samples: verdict.Samples}
				for _, service := range scenarioServices(descriptors[id]) {
					test.path = append(test.path, componentForService(m, service).Name)
				}
				series := samples[id]
				start := len(series)
				for start > 0 && series[start-1].Outcome != traffic.OutcomeOK {
					start--
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
				failing = append(failing, test)
			}
		}
	}
	return failing, passing, unevaluated, passingServices
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

// suspectChange is the latest change to a suspect's objects: a drift event
// (who, when, which fields), preferring one made shortly before onset, or the
// configuration diff against the last healthy baseline, which has no time.
func suspectChange(in detectInput, objects []sdk.ObjectRef, onset time.Time) (at time.Time, text, command string) {
	// Departures from accepted source by a non-SDO manager are the strongest
	// clue: the classifier the responder's other readers share names who, when
	// and what, and the expand line is the command that restores the object.
	departures := SourceDepartures(departureInput(in.Request, in.View, onset), objects)
	if len(departures) > 0 {
		first := departures[0]
		text = first.Text()
		if len(departures) > 1 {
			text += fmt.Sprintf(" (+%d more)", len(departures)-1)
		}
		command = first.Command()
		if command == "" {
			command = fmt.Sprintf("kubectl -n %s describe secret %s", first.Ref.Namespace, first.Ref.Name)
		}
		return first.At, text, command
	}
	var best *sdk.DriftEvent
	var bestAt time.Time
	for index := range in.Request.DriftEvents {
		event := &in.Request.DriftEvents[index]
		if !slices.ContainsFunc(objects, func(ref sdk.ObjectRef) bool { return ref.Kind == event.Ref.Kind && ref.Name == event.Ref.Name }) {
			continue
		}
		when := event.ObservedAt
		if event.ManagerAt != nil {
			when = *event.ManagerAt
		}
		before := onset.IsZero() || !when.After(onset.Add(30*time.Second))
		bestBefore := best != nil && (onset.IsZero() || !bestAt.After(onset.Add(30*time.Second)))
		if best == nil || before && !bestBefore || before == bestBefore && when.After(bestAt) {
			best, bestAt = event, when
		}
	}
	if best != nil {
		var fields []string
		for _, field := range best.Fields {
			if len(fields) < 2 {
				fields = append(fields, redactDetectText(field.Path, 40))
			}
		}
		by := "an unknown manager"
		if best.Manager != "" {
			by = best.Manager
		}
		if !strings.HasPrefix(best.Manager, "sdo") {
			by += " (not sdo)"
		}
		what := "changed"
		if len(fields) > 0 {
			what = strings.Join(fields, ",") + " changed"
		}
		return bestAt, fmt.Sprintf("%s/%s %s at %s by %s", best.Ref.Kind, best.Ref.Name, what, hms(bestAt), by), "sdo source " + refText(best.Ref)
	}
	if changes := in.Request.StateChanges; changes != nil {
		for _, change := range changes.Changes {
			if slices.ContainsFunc(objects, func(ref sdk.ObjectRef) bool { return ref.Kind == change.Kind && ref.Name == change.Name }) {
				return changes.ObservedAt, fmt.Sprintf("%s/%s %s since the last healthy state", change.Kind, change.Name, change.Change), "sdo source " + change.Kind + "/" + change.Namespace + "/" + change.Name
			}
		}
	}
	return time.Time{}, "", ""
}

// logLine reads the bounded error counts of one suspect: before and after
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
// components on the failing paths, and the opening findings and clues. Both
// `sdo detect` and the built-in restore playbook's candidates read it, so they
// name the same suspects.
type detectSuspectSet struct {
	m                              program.AppMap
	failing                        []failingTest
	passing, unevaluated           []string
	passingServices                map[string]bool
	openingFindings, clues         []sdk.Finding
	onset                          time.Time
	suspects                       []*detectSuspect
}

func buildSuspectSet(in detectInput) detectSuspectSet {
	m, _ := in.View.AppMap()
	failing, passing, unevaluated, passingServices := judgedTests(in.View, m)
	opening := openingKeys(&in.Request)
	var openingFindings, clues []sdk.Finding
	for _, finding := range in.Findings {
		if slices.Contains(opening, findingKey(finding)) {
			openingFindings = append(openingFindings, finding)
		} else {
			clues = append(clues, finding)
		}
	}
	onset := in.Request.DetectedAt
	for _, test := range failing {
		if !test.onset.IsZero() && (onset.IsZero() || test.onset.Before(onset)) {
			onset = test.onset
		}
	}

	// Suspects: components on the failing paths (every failing path first), or the
	// workloads of the opening conditions.
	var suspects []*detectSuspect
	seen := map[string]bool{}
	addSuspect := func(component program.AppComponent, everyPath bool) {
		if component.Name == "" || seen[component.Name] {
			return
		}
		seen[component.Name] = true
		suspect := &detectSuspect{component: component, everyPath: everyPath}
		for _, service := range component.Services {
			suspect.passing = suspect.passing || passingServices[service]
		}
		suspect.passing = suspect.passing || passingServices[component.Name]
		suspects = append(suspects, suspect)
	}
	if len(failing) > 0 {
		var common []string
		for _, name := range failing[0].path {
			onAll := true
			for _, other := range failing[1:] {
				onAll = onAll && slices.Contains(other.path, name)
			}
			if onAll {
				common = append(common, name)
			}
		}
		var rest []string
		for _, test := range failing {
			for _, name := range test.path {
				if !slices.Contains(common, name) && !slices.Contains(rest, name) {
					rest = append(rest, name)
				}
			}
		}
		for _, name := range append(common, rest...) {
			component, ok := m.Component(name)
			if !ok {
				component = program.AppComponent{Name: name}
			}
			addSuspect(component, slices.Contains(common, name))
		}
	}
	for _, finding := range openingFindings {
		if isProbeFinding(finding) {
			continue
		}
		if component, ok := m.ComponentOf(in.View, finding.PrimaryResource); ok {
			addSuspect(component, true)
		}
	}
	return detectSuspectSet{m: m, failing: failing, passing: passing, unevaluated: unevaluated, passingServices: passingServices, openingFindings: openingFindings, clues: clues, onset: onset, suspects: suspects}
}

// buildDetectSummary renders the compact incident summary: what users see
// first, then the suspects on the failing paths (each with its latest change,
// new log errors, clues and matching playbooks), then the other clues. It
// never exceeds detectSummaryCap bytes and counts every item it drops.
func buildDetectSummary(in detectInput) detectSummary {
	items := map[string]string{}
	set := buildSuspectSet(in)
	m, failing, passing, unevaluated := set.m, set.failing, set.passing, set.unevaluated
	openingFindings, clues, onset, suspects := set.openingFindings, set.clues, set.onset, set.suspects
	seen := map[string]bool{}
	for _, suspect := range suspects {
		seen[suspect.component.Name] = true
	}
	namespace := in.Request.Namespace
	if namespace == "" {
		namespace = in.View.Namespace()
	}
	for _, suspect := range suspects {
		if suspect.component.Workload == "" {
			continue
		}
		suspect.changeAt, suspect.change, suspect.changeCmd = suspectChange(in, componentObjects(in.View, suspect.component, namespace), onset)
	}
	// Latest change before onset first; the failing-path order otherwise.
	order := map[string]int{}
	for index, suspect := range suspects {
		order[suspect.component.Name] = index
	}
	sort.SliceStable(suspects, func(i, j int) bool {
		a, b := suspects[i], suspects[j]
		if (a.change != "") != (b.change != "") {
			return a.change != ""
		}
		if a.change != "" && !a.changeAt.Equal(b.changeAt) {
			return a.changeAt.After(b.changeAt)
		}
		return order[a.component.Name] < order[b.component.Name]
	})

	// Clues that resolve to a suspect attach to it; the rest are background.
	attached := map[string][]sdk.Finding{}
	var background []sdk.Finding
	for _, clue := range clues {
		placed := false
		for _, ref := range objectOfFinding(clue) {
			if component, ok := m.ComponentOf(in.View, ref); ok && seen[component.Name] {
				attached[component.Name] = append(attached[component.Name], clue)
				placed = true
				break
			}
		}
		if !placed {
			background = append(background, clue)
		}
	}

	// Header and OPENED ON.
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
	if len(failing) > 0 {
		opened = "user tests"
	}
	header := []string{
		fmt.Sprintf("incident %s  opened on: %s  onset %s  now %s", in.Request.IncidentID, opened, hms(onset), hms(in.Now)),
		fmt.Sprintf("recovered: %s (%d opening alerts active)", state, active),
	}
	for _, note := range in.Request.PriorStages {
		for _, mitigation := range note.GetMitigations() {
			if mitigation.GetResult() != "skipped" {
				header = append(header, fmt.Sprintf("stage: by sdo (playbook %s): %s", mitigation.GetPlaybookId(), redactDetectText(mitigation.GetResult(), 40)))
			}
		}
	}

	testBlocks := make([][]string, 0, len(failing))
	for _, test := range failing {
		since := ""
		if !test.onset.IsZero() {
			since = " since " + hms(test.onset)
		}
		// The user-facing response comes first: what a person sees is the evidence.
		lines := []string{
			fmt.Sprintf("%s  %s %q  step %s", test.id, test.label, redactDetectText(test.err, 60), orQuestion(redactDetectText(test.step, 30))),
			fmt.Sprintf("  %s  %d/%d failing%s", orQuestion(redactDetectText(strings.Join(test.path, ">"), 100)), test.failures, test.samples, since),
			"  more: sdo detect --finding " + test.id,
		}
		testBlocks = append(testBlocks, lines)
		var detail strings.Builder
		fmt.Fprintf(&detail, "%s: %s\npath: %s\nsince %s, %d/%d failing\nlast failing samples:\n", test.id, test.scenarios.Description, strings.Join(test.path, ">"), hms(test.onset), test.failures, test.samples)
		for _, sample := range test.last {
			fmt.Fprintf(&detail, "  %s step %s %s: %s %s\n", hms(sample.At), orQuestion(sample.Step), sample.Request, failureLabel(sample), redactDetectText(sample.Error, 200))
		}
		items[test.id] = detail.String()
	}
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
	var allTests strings.Builder
	for _, test := range failing {
		allTests.WriteString(items[test.id] + "\n")
	}
	items["tests"] = allTests.String()

	// Suspects.
	deadline := lifecycleio.Now().Add(detectLogTotal)
	type suspectText struct{ full, short []string }
	texts := make([]suspectText, len(suspects))
	for index, suspect := range suspects {
		workload := suspect.component.WorkloadRef(namespace)
		title := suspect.component.Name
		if suspect.component.Workload != "" {
			title += "  " + suspect.component.Workload
		}
		if suspect.passing {
			title += "  (also on passing paths)"
		}
		full := []string{title}
		if suspect.component.Workload == "" {
			texts[index] = suspectText{full: full, short: full}
			continue
		}
		if suspect.change != "" {
			full = append(full, "  change: "+redactDetectText(suspect.change, 110), "    more: "+suspect.changeCmd)
		} else {
			full = append(full, "  change: none since accepted", "    more: sdo source "+refText(workload))
		}
		if index < detectFullSuspects {
			if line, command := suspectLogs(in, workload, onset, deadline); line != "" {
				full = append(full, "  "+line, "    more: "+command)
			}
		}
		if mine := attached[suspect.component.Name]; len(mine) > 0 {
			clue := mine[0]
			id := shortID(FindingFingerprint(clue))
			full = append(full, "  clue: "+redactDetectText(clue.Summary, 100), "    more: sdo detect --finding "+id)
			for _, extra := range mine {
				items[shortID(FindingFingerprint(extra))] = redactDetectText(extra.Summary+"\n"+extra.Evidence, detectItemCap)
			}
		}
		if match := playbookFor(in, m, suspect.component.Name); match != nil {
			full = append(full, "  playbooks: "+match.GetPlaybookId(), "    more: "+strings.Replace(match.GetMitigateCommand(), "sdo mitigate", "sdo diagnose", 1))
		}
		texts[index] = suspectText{full: full, short: full[:1]}
		items[suspect.component.Name] = strings.Join(full, "\n")
	}

	// Background clues.
	var backgroundLines []string
	var backgroundAll strings.Builder
	for _, clue := range background {
		id := shortID(FindingFingerprint(clue))
		backgroundLines = append(backgroundLines, fmt.Sprintf("%s  more: sdo detect --finding %s", redactDetectText(clue.Summary, 90), id))
		items[id] = redactDetectText(clue.Summary+"\n"+clue.Evidence, detectItemCap)
		backgroundAll.WriteString(id + "  " + redactDetectText(clue.Summary, 200) + "\n")
	}
	items["background"] = backgroundAll.String()

	// Assemble, dropping the least important items until the text fits.
	shownTests, shownSuspects, fullSuspects, shownClues := len(testBlocks), len(texts), min(detectFullSuspects, len(texts)), min(2, len(backgroundLines))
	shownConditions := min(len(conditionBlocks), detectShownTests)
	showPassing := true
	if shownTests > detectShownTests {
		shownTests = detectShownTests
	}
	assemble := func() string {
		var out []string
		out = append(out, header...)
		out = append(out, "", "OPENED ON")
		for _, block := range testBlocks[:shownTests] {
			out = append(out, block...)
		}
		if dropped := len(testBlocks) - shownTests; dropped > 0 {
			out = append(out, fmt.Sprintf("+%d more: sdo detect --finding tests", dropped))
		}
		for _, block := range conditionBlocks[:shownConditions] {
			out = append(out, block...)
		}
		if dropped := len(conditionBlocks) - shownConditions; dropped > 0 {
			out = append(out, fmt.Sprintf("+%d more: sdo detect --finding conditions", dropped))
		}
		if showPassing && len(passing) > 0 {
			out = append(out, "passing: "+redactDetectText(strings.Join(passing, " "), 160))
		} else if !showPassing && len(passing) > 0 {
			out = append(out, fmt.Sprintf("passing: %d tests", len(passing)))
		}
		if len(unevaluated) > 0 {
			out = append(out, "not yet evaluated: "+redactDetectText(strings.Join(unevaluated, " "), 100))
		}
		if len(texts) > 0 {
			label := "SUSPECTS (on every failing path)"
			if len(failing) == 0 {
				label = "SUSPECTS (from the opening conditions)"
			}
			out = append(out, "", label)
		}
		for index := 0; index < shownSuspects; index++ {
			if index < fullSuspects {
				out = append(out, texts[index].full...)
			} else {
				out = append(out, texts[index].short...)
			}
		}
		if dropped := len(texts) - shownSuspects; dropped > 0 {
			out = append(out, fmt.Sprintf("+%d more suspects: sdo detect --finding tests", dropped))
		}
		if len(backgroundLines) > 0 {
			out = append(out, "", "OTHER CLUES (not on a failing path)")
			out = append(out, backgroundLines[:shownClues]...)
			if dropped := len(backgroundLines) - shownClues; dropped > 0 {
				out = append(out, fmt.Sprintf("+%d more: sdo detect --finding background", dropped))
			}
		}
		return strings.Join(out, "\n")
	}
	for len(assemble()) > detectSummaryCap {
		switch {
		case fullSuspects > 1:
			fullSuspects--
		case shownSuspects > fullSuspects:
			shownSuspects--
		case shownClues > 0:
			shownClues--
		case shownConditions > 1:
			shownConditions--
		case shownTests > 1:
			shownTests--
		case showPassing:
			showPassing = false
		case fullSuspects > 0:
			fullSuspects--
			shownSuspects = min(shownSuspects, 1)
		case shownSuspects > 0:
			shownSuspects--
		default:
			text := assemble()
			cut := text[:detectSummaryCap-40]
			for len(cut) > 0 && !strings.HasSuffix(cut, "\n") && cut[len(cut)-1] >= 0x80 {
				cut = cut[:len(cut)-1]
			}
			return detectSummary{Text: cut + "\n+more: sdo detect --json", Items: items}
		}
	}
	return detectSummary{Text: assemble(), Items: items}
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

// detectItem resolves an `--finding ID`: a test, component, clue prefix,
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

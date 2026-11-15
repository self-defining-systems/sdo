package runtime

import (
	"context"
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/requestmetrics"
	"sdo.dev/operator/telemetry"
)

// `sdo metrics`, `sdo logs` and `sdo traces` are read-only views of the same
// telemetry sources the detectors read: the controller's bound metrics source,
// the application's pod logs and a trace store when one is bound. Every call is
// recorded with its response like any other tool call, so a rollout replays.
const (
	telemetryDefaultLogWindow = 10 * time.Minute
	telemetryLogBytes         = 256 * 1024
	telemetryLogPods          = 12
	telemetryLogLines         = 120
	telemetryLineWidth        = 300
	telemetryMetricRows       = 50
	telemetryListNames        = 200
	telemetryRouteRollup      = 10
	telemetrySamplePoints     = 12
)

// PodLogReader opens one container's log; the controller's Kubernetes client
// implements it.
type PodLogReader func(ctx context.Context, namespace, pod string, options *corev1.PodLogOptions) (io.ReadCloser, error)

// ApplicationTelemetry is what the telemetry commands read. A zero field is a
// missing source, and the command says so.
type ApplicationTelemetry struct {
	// Metrics answers PromQL; nil when no metrics source is bound.
	Metrics MetricsAPI
	// Sources are the bound sources, for saying what exists.
	Sources []telemetry.Source
	// PodLogs reads container logs; nil when the controller has no client.
	PodLogs PodLogReader
	// Traces reads the bound trace store; nil when none is bound or readable.
	Traces telemetry.TraceReader
}

// metricsSources lists the bound sources of a MetricsAPI that knows them.
func metricsSources(api MetricsAPI) []telemetry.Source {
	if known, ok := api.(interface{ Sources() []telemetry.Source }); ok {
		return known.Sources()
	}
	return nil
}

func (l *ResponderLoop) applicationTelemetry(ctx context.Context, request ToolRequest, response *ToolResponse) {
	var source ApplicationTelemetry
	if l.Telemetry != nil {
		source = l.Telemetry()
	}
	var since time.Duration
	if request.Since != "" {
		parsed, err := time.ParseDuration(request.Since)
		if err != nil || parsed <= 0 {
			response.Error = fmt.Sprintf("--since %q is not a positive duration such as 10m", request.Since)
			return
		}
		since = parsed
	}
	switch request.Command {
	case "metrics":
		source.metrics(ctx, l.clock().UTC(), since, request, response)
	case "logs":
		l.podLogs(ctx, source, since, request, response)
	default:
		l.applicationTraces(ctx, source, since, request, response)
	}
}

func (t ApplicationTelemetry) metrics(ctx context.Context, now time.Time, since time.Duration, request ToolRequest, response *ToolResponse) {
	if t.Metrics == nil {
		response.Error = "no metrics source is bound: the environment provides no Prometheus and the accepted revision binds none. " +
			"The application can expose Prometheus metrics (a /metrics endpoint with scrape annotations) for SDO's Prometheus to collect"
		return
	}
	var step time.Duration
	if request.Step != "" {
		parsed, err := time.ParseDuration(request.Step)
		if err != nil || parsed <= 0 {
			response.Error = fmt.Sprintf("--step %q is not a positive duration such as 15s", request.Step)
			return
		}
		step = parsed
	}
	if request.List {
		t.listMetrics(ctx, now, request.Name, response)
		return
	}
	if strings.TrimSpace(request.Query) == "" {
		response.Error = "sdo metrics needs a PromQL QUERY, or --list [MATCH] to see what exists"
		return
	}
	query := metrics.Query{ID: "sdo-metrics", Expr: request.Query}
	if since > 0 {
		query.Range, query.Step = since, step
	} else if step > 0 {
		response.Error = "--step needs --since"
		return
	}
	if err := query.Validate(); err != nil {
		response.Error = err.Error()
		return
	}
	queryCtx, cancel := context.WithTimeout(ctx, DefaultMetricsQueryTimeout)
	defer cancel()
	series, err := t.Metrics.Query(queryCtx, query, now)
	if err != nil {
		response.Error = "metrics query failed: " + err.Error()
		return
	}
	response.Text = renderMetricSeries(series, since > 0)
	response.Summary = fmt.Sprintf("%d series", len(series))
}

// renderMetricSeries prints one CSV row per series, largest first: the value
// of an instant query, or first, min, max, last and a few evenly spaced values
// of a range query.
func renderMetricSeries(series []metrics.Series, ranged bool) string {
	if len(series) == 0 {
		return "no data: the query matched no series in this window\n"
	}
	type row struct {
		cells []string
		order float64
	}
	var rows []row
	for _, s := range series {
		if len(s.Samples) == 0 {
			continue
		}
		label := redactDetectText(metrics.LabelString(s.Labels), 200)
		first, last := s.Samples[0], s.Samples[len(s.Samples)-1]
		if !ranged {
			rows = append(rows, row{cells: []string{label, formatMetric(last.Value)}, order: last.Value})
			continue
		}
		low, high := first.Value, first.Value
		for _, sample := range s.Samples {
			low, high = math.Min(low, sample.Value), math.Max(high, sample.Value)
		}
		var points []string
		for i := range min(len(s.Samples), telemetrySamplePoints) {
			index := i
			if len(s.Samples) > telemetrySamplePoints {
				index = i * (len(s.Samples) - 1) / (telemetrySamplePoints - 1)
			}
			points = append(points, formatMetric(s.Samples[index].Value))
		}
		rows = append(rows, row{cells: []string{label, formatMetric(first.Value), formatMetric(low), formatMetric(high), formatMetric(last.Value), strconv.Itoa(len(s.Samples)), strings.Join(points, " "), last.At.UTC().Format(time.RFC3339)}, order: high})
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].order > rows[j].order })
	header := []string{"series", "value"}
	if ranged {
		header = []string{"series", "first", "min", "max", "last", "samples", "values", "last_at_utc"}
	}
	out := [][]string{header}
	for i, r := range rows {
		if i == telemetryMetricRows {
			break
		}
		out = append(out, r.cells)
	}
	text := string(encodeCSV(out))
	if len(rows) > telemetryMetricRows {
		text += fmt.Sprintf("# %d more series not shown; narrow the query\n", len(rows)-telemetryMetricRows)
	}
	return text
}

func formatMetric(v float64) string {
	switch {
	case math.IsNaN(v):
		return "NaN"
	case math.IsInf(v, 0):
		return "Inf"
	}
	return strconv.FormatFloat(v, 'g', 6, 64)
}

// listMetrics names the metrics that exist (with series counts); when MATCH
// names exactly one metric it also lists that metric's labels.
func (t ApplicationTelemetry) listMetrics(ctx context.Context, now time.Time, match string, response *ToolResponse) {
	pattern := ".+"
	if strings.TrimSpace(match) != "" {
		pattern = ".*" + regexp.QuoteMeta(strings.TrimSpace(match)) + ".*"
	}
	expr := fmt.Sprintf(`count by (__name__) ({__name__=~%s})`, strconv.Quote(pattern))
	queryCtx, cancel := context.WithTimeout(ctx, DefaultMetricsQueryTimeout)
	defer cancel()
	series, err := t.Metrics.Query(queryCtx, metrics.Query{ID: "sdo-metrics-list", Expr: expr}, now)
	if err != nil {
		response.Error = "metrics list failed (narrow it with MATCH): " + err.Error()
		return
	}
	names := make([]string, 0, len(series))
	counts := map[string]float64{}
	for _, s := range series {
		name := s.Labels["__name__"]
		if last, ok := s.Last(); ok && name != "" {
			names = append(names, name)
			counts[name] = last.Value
		}
	}
	sort.Strings(names)
	if len(names) == 0 {
		response.Text = "no metric names match\n"
		return
	}
	var out strings.Builder
	if len(names) == 1 {
		detail, err := t.Metrics.Query(queryCtx, metrics.Query{ID: "sdo-metrics-labels", Expr: names[0]}, now)
		if err == nil {
			fmt.Fprintf(&out, "%s: %d series\n", names[0], len(detail))
			values := map[string]map[string]bool{}
			for _, s := range detail {
				for label, value := range s.Labels {
					if label == "__name__" {
						continue
					}
					if values[label] == nil {
						values[label] = map[string]bool{}
					}
					values[label][value] = true
				}
			}
			labels := make([]string, 0, len(values))
			for label := range values {
				labels = append(labels, label)
			}
			sort.Strings(labels)
			for _, label := range labels {
				var sample []string
				for value := range values[label] {
					sample = append(sample, value)
				}
				sort.Strings(sample)
				more := ""
				if len(sample) > 5 {
					more = fmt.Sprintf(" (+%d more)", len(sample)-5)
					sample = sample[:5]
				}
				fmt.Fprintf(&out, "  label %s: %s%s\n", label, redactDetectText(strings.Join(sample, ", "), 160), more)
			}
			response.Text = out.String()
			return
		}
	}
	rows := [][]string{{"metric", "series"}}
	for i, name := range names {
		if i == telemetryListNames {
			break
		}
		rows = append(rows, []string{name, formatMetric(counts[name])})
	}
	out.Write(encodeCSV(rows))
	if len(names) > telemetryListNames {
		fmt.Fprintf(&out, "# %d more names not shown; pass a MATCH substring\n", len(names)-telemetryListNames)
	}
	response.Text = out.String()
}

// errorLine marks a log line an application reported as an error. The request
// parser covers access-log lines with a 4xx or 5xx status.
var errorLine = regexp.MustCompile(`(?i)\b(error|err|fatal|panic|exception|traceback|critical|failed|failure)\b|"level"\s*:\s*"(error|fatal|panic)"`)

type logLine struct {
	at   time.Time
	pod  string
	text string
}

// podLogs prints the matching lines of a component's pods across replicas.
func (l *ResponderLoop) podLogs(ctx context.Context, source ApplicationTelemetry, since time.Duration, request ToolRequest, response *ToolResponse) {
	if source.PodLogs == nil {
		response.Error = "no pod log reader: the controller has no Kubernetes client for logs"
		return
	}
	if strings.TrimSpace(request.Name) == "" {
		response.Error = "sdo logs needs a COMPONENT (an application-map component, workload or Service)"
		return
	}
	var grep *regexp.Regexp
	if request.Grep != "" {
		compiled, err := regexp.Compile(request.Grep)
		if err != nil {
			response.Error = fmt.Sprintf("--grep %q is not a valid RE2 expression: %v", request.Grep, err)
			return
		}
		grep = compiled
	}
	if l.Provider == nil {
		response.Error = "fresh snapshot provider is unavailable"
		return
	}
	snapshot, err := l.Provider.Snapshot(ctx)
	if err != nil {
		response.Error = err.Error()
		return
	}
	pods, err := componentPods(l, snapshot, request.Name)
	if err != nil {
		response.Error = err.Error()
		return
	}
	if since == 0 {
		since = telemetryDefaultLogWindow
	}
	now := l.clock().UTC()
	var lines []logLine
	var scanned int
	var unread []string
	for i, pod := range pods {
		if i == telemetryLogPods {
			unread = append(unread, fmt.Sprintf("%d more pods not read", len(pods)-i))
			break
		}
		for _, container := range pod.Spec.Containers {
			limit := int64(telemetryLogBytes)
			options := &corev1.PodLogOptions{Container: container.Name, Timestamps: true, LimitBytes: &limit, SinceTime: &metav1.Time{Time: now.Add(-since)}}
			readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			stream, err := source.PodLogs(readCtx, pod.Namespace, pod.Name, options)
			if err != nil {
				cancel()
				unread = append(unread, fmt.Sprintf("%s/%s: %s", pod.Name, container.Name, redactDetectText(err.Error(), 120)))
				continue
			}
			data, readErr := io.ReadAll(io.LimitReader(stream, telemetryLogBytes+1))
			_ = stream.Close()
			cancel()
			if readErr != nil {
				unread = append(unread, fmt.Sprintf("%s/%s: %s", pod.Name, container.Name, redactDetectText(readErr.Error(), 120)))
			}
			name := pod.Name
			if len(pod.Spec.Containers) > 1 {
				name += "/" + container.Name
			}
			for _, raw := range strings.Split(string(data), "\n") {
				if strings.TrimSpace(raw) == "" {
					continue
				}
				scanned++
				at, text := splitLogTimestamp(raw)
				if grep != nil && !grep.MatchString(text) {
					continue
				}
				if request.Failing && !failingLogLine(text) {
					continue
				}
				lines = append(lines, logLine{at: at, pod: name, text: text})
			}
		}
	}
	response.Text, response.Summary = renderLogs(request, lines, scanned, len(pods), unread)
}

func failingLogLine(text string) bool {
	if _, ok := requestmetrics.ParseFailingLine(text); ok {
		return true
	}
	return errorLine.MatchString(text)
}

func splitLogTimestamp(raw string) (time.Time, string) {
	stamp, text, found := strings.Cut(raw, " ")
	if found {
		if at, err := time.Parse(time.RFC3339Nano, stamp); err == nil {
			return at, text
		}
	}
	return time.Time{}, raw
}

// renderLogs prints the newest lines, each tagged with its pod, and for a
// failing read a rollup of the failing requests by method, path and status.
func renderLogs(request ToolRequest, lines []logLine, scanned, pods int, unread []string) (string, string) {
	sort.SliceStable(lines, func(i, j int) bool { return lines[i].at.Before(lines[j].at) })
	var out strings.Builder
	summary := fmt.Sprintf("%d of %d lines from %d pods", len(lines), scanned, pods)
	fmt.Fprintf(&out, "# %s\n", summary)
	for _, note := range unread {
		fmt.Fprintf(&out, "# unread: %s\n", note)
	}
	if request.Failing {
		counts := map[string]int{}
		for _, line := range lines {
			if sample, ok := requestmetrics.ParseFailingLine(line.text); ok {
				counts[sample.Method+" "+sample.Path+" -> "+sample.Status]++
			}
		}
		keys := make([]string, 0, len(counts))
		for key := range counts {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			if counts[keys[i]] != counts[keys[j]] {
				return counts[keys[i]] > counts[keys[j]]
			}
			return keys[i] < keys[j]
		})
		for i, key := range keys {
			if i == telemetryRouteRollup {
				break
			}
			fmt.Fprintf(&out, "# failing requests: %d x %s\n", counts[key], redactDetectText(key, 160))
		}
	}
	shown := lines
	if len(shown) > telemetryLogLines {
		fmt.Fprintf(&out, "# %d older matching lines not shown; narrow with --since or --grep\n", len(shown)-telemetryLogLines)
		shown = shown[len(shown)-telemetryLogLines:]
	}
	for _, line := range shown {
		at := ""
		if !line.at.IsZero() {
			at = line.at.UTC().Format("15:04:05") + " "
		}
		text := line.text
		if sample, ok := requestmetrics.ParseFailingLine(text); ok {
			// The parsed request drops the query string, which may carry tokens.
			text = sample.Method + " " + sample.Path + " -> " + sample.Status
		}
		fmt.Fprintf(&out, "%s%s %s\n", at, line.pod, redactDetectText(text, telemetryLineWidth))
	}
	if len(lines) == 0 {
		out.WriteString("no matching lines\n")
	}
	return out.String(), summary
}

// componentPods are the pods of a component named as an application-map
// component, a workload (Kind/name or name) or a Service, oldest name first.
func componentPods(l *ResponderLoop, snapshot sdk.DetectionContext, name string) ([]corev1.Pod, error) {
	name = strings.TrimSpace(name)
	namespace := l.Request.Namespace
	if namespace == "" {
		namespace = snapshot.Namespace()
	}
	workload := name
	if reads, ok := any(NewCycleReadView(program.ReadFrom(snapshot))).(detectReads); ok {
		if graph, _, _ := loadCandidateGraph(reads); graph != nil {
			if component, ok := graph.comps[name]; ok && component.Workload != "" {
				workload = component.Workload
			}
		}
	}
	kind, short, qualified := strings.Cut(workload, "/")
	if !qualified {
		kind, short = "", workload
	}
	var pods []corev1.Pod
	for _, pod := range snapshot.Pods() {
		if pod.Namespace != namespace {
			continue
		}
		owner := program.WorkloadOf(snapshot, sdk.ObjectRef{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name})
		ownerKind, ownerName, _ := strings.Cut(owner, "/")
		switch {
		case ownerName == short && (kind == "" || kind == ownerKind):
			pods = append(pods, pod)
		case owner == "Pod/"+pod.Name && strings.HasPrefix(pod.Name, short+"-") && (kind == "" || kind == "Pod" || kind == "Deployment" || kind == "StatefulSet"):
			pods = append(pods, pod)
		}
	}
	if len(pods) == 0 {
		pods = append(pods, snapshot.PodsForService(namespace, short)...)
	}
	if len(pods) == 0 {
		return nil, fmt.Errorf("no pods found for %q in namespace %s; name an application-map component (sdo map), a workload or a Service", name, namespace)
	}
	sort.Slice(pods, func(i, j int) bool { return pods[i].Name < pods[j].Name })
	return pods, nil
}

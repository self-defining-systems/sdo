package runtime

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/telemetry"
)

const (
	traceDefaultWindow = 15 * time.Minute
	traceListRows      = 20
	traceTreeSpans     = 80
	traceWidth         = 200
)

// applicationTraces answers `sdo traces`: a listing of recent traces, or with a
// trace ID the span tree. The store is the controller's bound trace source.
func (l *ResponderLoop) applicationTraces(ctx context.Context, source ApplicationTelemetry, since time.Duration, request ToolRequest, response *ToolResponse) {
	if source.Traces == nil {
		response.Error = noTraceStore(source)
		return
	}
	queryCtx, cancel := context.WithTimeout(ctx, DefaultMetricsQueryTimeout)
	defer cancel()
	if id := strings.TrimSpace(request.Name); id != "" {
		trace, err := source.Traces.Trace(queryCtx, id)
		if err != nil {
			response.Error = "trace read failed: " + err.Error()
			return
		}
		response.Text = renderTraceTree(trace)
		response.Summary = fmt.Sprintf("trace %s, %d spans", id, len(trace.Spans))
		return
	}
	if since == 0 {
		since = traceDefaultWindow
	}
	found, err := source.Traces.Traces(queryCtx, telemetry.TraceQuery{End: l.clock().UTC(), Lookback: since, Failing: request.Failing})
	if err != nil {
		response.Error = "trace list failed: " + err.Error()
		return
	}
	terms := l.traceTerms(ctx, request)
	var kept []telemetry.TraceSummary
	for _, trace := range found {
		if request.Failing && !trace.Failed {
			continue
		}
		if len(terms) > 0 && !traceMatches(trace, terms) {
			continue
		}
		kept = append(kept, trace)
	}
	sort.SliceStable(kept, func(i, j int) bool { return kept[i].Start.After(kept[j].Start) })
	response.Text = renderTraceList(kept)
	response.Summary = fmt.Sprintf("%d of %d traces", min(len(kept), traceListRows), len(found))
}

func noTraceStore(source ApplicationTelemetry) string {
	var bound []string
	for _, s := range source.Sources {
		if s.Serves(telemetry.KindTraces) {
			bound = append(bound, s.ID)
		}
	}
	if len(bound) > 0 {
		return fmt.Sprintf("trace source %s is bound, but this controller cannot read traces from it", strings.Join(bound, ", "))
	}
	return "no trace store is bound: the environment provides no trace backend and the accepted revision binds none. " +
		"The application can emit OpenTelemetry traces (OTLP) to the trace store's endpoint when the environment offers one; until then use `sdo logs COMPONENT --failing` and `sdo metrics` to compare failing and working requests"
}

// traceTerms are the strings a listed trace must contain in its root operation
// or route: --route as typed, and --operation as the map's entry path of that
// operation when the map knows it, else as typed.
func (l *ResponderLoop) traceTerms(ctx context.Context, request ToolRequest) []string {
	var terms []string
	if route := strings.TrimSpace(request.Route); route != "" {
		terms = append(terms, route)
	}
	if name := strings.TrimSpace(request.Operation); name != "" {
		term := name
		if graph, _, _, err := l.loadMap(ctx); err == nil {
			if op, found := graph.operation(name); found {
				term = op.Route
				if term == "" {
					term = op.Entry.Path
				}
			}
		}
		terms = append(terms, term)
	}
	return terms
}

func traceMatches(trace telemetry.TraceSummary, terms []string) bool {
	text := trace.RootOperation + " " + trace.Route
	for _, term := range terms {
		if !strings.Contains(text, term) {
			return false
		}
	}
	return true
}

func traceStatus(failed bool) string {
	if failed {
		return "error"
	}
	return "ok"
}

func formatDuration(d time.Duration) string {
	switch {
	case d >= time.Second:
		return d.Round(10 * time.Millisecond).String()
	case d >= time.Millisecond:
		return d.Round(time.Millisecond).String()
	}
	return d.Round(time.Microsecond).String()
}

func renderTraceList(traces []telemetry.TraceSummary) string {
	if len(traces) == 0 {
		return "no matching traces in this window\n"
	}
	rows := [][]string{{"trace_id", "start_utc", "root", "status", "duration", "services", "first_failure"}}
	for i, trace := range traces {
		if i == traceListRows {
			break
		}
		root := trace.RootOperation
		if trace.RootService != "" {
			root = trace.RootService + " " + root
		}
		rows = append(rows, []string{
			trace.ID, trace.Start.UTC().Format("15:04:05"), redactDetectText(root, traceWidth), traceStatus(trace.Failed),
			formatDuration(trace.Duration), strings.Join(trace.Services, ","), redactDetectText(trace.FirstFailure, traceWidth),
		})
	}
	text := string(encodeCSV(rows))
	if len(traces) > traceListRows {
		text += fmt.Sprintf("# %d older matching traces not shown; narrow with --since, --route or --failing\n", len(traces)-traceListRows)
	}
	return text
}

// renderTraceTree prints one trace as an indented tree, a span per line:
// service, operation (or statement), status and duration.
func renderTraceTree(trace telemetry.Trace) string {
	spans := trace.Spans
	ids := map[string]bool{}
	for _, s := range spans {
		ids[s.ID] = true
	}
	children := map[string][]telemetry.Span{}
	var roots []telemetry.Span
	for _, s := range spans {
		if s.ParentID == "" || !ids[s.ParentID] {
			roots = append(roots, s)
		} else {
			children[s.ParentID] = append(children[s.ParentID], s)
		}
	}
	byStart := func(list []telemetry.Span) {
		sort.SliceStable(list, func(i, j int) bool { return list[i].Start.Before(list[j].Start) })
	}
	byStart(roots)
	var out strings.Builder
	fmt.Fprintf(&out, "# trace %s, %d spans\n", trace.ID, len(spans))
	shown := 0
	var walk func(span telemetry.Span, depth int, seen map[string]bool)
	walk = func(span telemetry.Span, depth int, seen map[string]bool) {
		if seen[span.ID] {
			return
		}
		seen[span.ID] = true
		if shown == traceTreeSpans {
			shown++
			return
		}
		if shown > traceTreeSpans {
			return
		}
		shown++
		line := span.Service + " " + redactDetectText(span.Operation, traceWidth)
		if span.Statement != "" {
			line += " " + redactDetectText(span.Statement, traceWidth)
		}
		status := "ok"
		if span.Failed {
			status = "ERROR"
			if span.Error != "" {
				status += " " + redactDetectText(span.Error, traceWidth)
			}
		}
		fmt.Fprintf(&out, "%s%s  %s  %s\n", strings.Repeat("  ", depth), line, status, formatDuration(span.Duration))
		kids := children[span.ID]
		byStart(kids)
		for _, kid := range kids {
			walk(kid, depth+1, seen)
		}
	}
	seen := map[string]bool{}
	for _, root := range roots {
		walk(root, 0, seen)
	}
	if len(spans) > traceTreeSpans {
		fmt.Fprintf(&out, "# %d more spans not shown\n", len(spans)-traceTreeSpans)
	}
	return out.String()
}

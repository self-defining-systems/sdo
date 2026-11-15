package jaeger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"sdo.dev/operator/telemetry"
)

const (
	maxResponseBytes = 32 << 20
	maxErrorBytes    = 512
	defaultLimit     = 100
	maxServices      = 40
	maxStatement     = 120
)

// Client calls the Jaeger query HTTP API. It sets no timeout of its own:
// callers bound each call with the context.
type Client struct {
	BaseURL    string
	HTTPClient telemetry.HTTPDoer
}

var _ telemetry.TraceReader = Client{}

type jaegerTag struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
}

type jaegerSpan struct {
	TraceID       string      `json:"traceID"`
	SpanID        string      `json:"spanID"`
	OperationName string      `json:"operationName"`
	References    []jaegerRef `json:"references"`
	StartTime     int64       `json:"startTime"`
	Duration      int64       `json:"duration"`
	Tags          []jaegerTag `json:"tags"`
	Logs          []struct {
		Fields []jaegerTag `json:"fields"`
	} `json:"logs"`
	ProcessID string `json:"processID"`
}

type jaegerRef struct {
	RefType string `json:"refType"`
	SpanID  string `json:"spanID"`
}

type jaegerTrace struct {
	TraceID   string                `json:"traceID"`
	Spans     []jaegerSpan          `json:"spans"`
	Processes map[string]jaegerProc `json:"processes"`
}

type jaegerProc struct {
	ServiceName string `json:"serviceName"`
}

// Services lists the services that emitted traces, without Jaeger's own.
func (c Client) Services(ctx context.Context) ([]string, error) {
	var data struct {
		Data []string `json:"data"`
	}
	if err := c.get(ctx, "/api/services", nil, &data); err != nil {
		return nil, err
	}
	var out []string
	for _, name := range data.Data {
		if strings.HasPrefix(name, "jaeger") {
			continue
		}
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}

// Traces lists recent traces of every service, newest first.
func (c Client) Traces(ctx context.Context, q telemetry.TraceQuery) ([]telemetry.TraceSummary, error) {
	services, err := c.Services(ctx)
	if err != nil {
		return nil, err
	}
	if len(services) > maxServices {
		services = services[:maxServices]
	}
	limit := q.Limit
	if limit <= 0 {
		limit = defaultLimit
	}
	lookback := q.Lookback
	if lookback <= 0 {
		lookback = time.Hour
	}
	if q.End.IsZero() {
		return nil, errors.New("a trace listing needs the end of its window")
	}
	end := q.End
	seen := map[string]bool{}
	var out []telemetry.TraceSummary
	for _, service := range services {
		values := url.Values{
			"service": {service},
			"limit":   {strconv.Itoa(limit)},
			"start":   {strconv.FormatInt(end.Add(-lookback).UnixMicro(), 10)},
			"end":     {strconv.FormatInt(end.UnixMicro(), 10)},
		}
		var data struct {
			Data []jaegerTrace `json:"data"`
		}
		if err := c.get(ctx, "/api/traces", values, &data); err != nil {
			return nil, err
		}
		for _, raw := range data.Data {
			if seen[raw.TraceID] {
				continue
			}
			seen[raw.TraceID] = true
			summary := summarize(convert(raw))
			if q.Failing && !summary.Failed {
				continue
			}
			out = append(out, summary)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Start.After(out[j].Start) })
	return out, nil
}

// Trace reads one trace by ID.
func (c Client) Trace(ctx context.Context, id string) (telemetry.Trace, error) {
	if id == "" || strings.ContainsAny(id, "/?# ") {
		return telemetry.Trace{}, fmt.Errorf("%q is not a trace ID", id)
	}
	var data struct {
		Data []jaegerTrace `json:"data"`
	}
	if err := c.get(ctx, "/api/traces/"+id, nil, &data); err != nil {
		return telemetry.Trace{}, err
	}
	if len(data.Data) == 0 {
		return telemetry.Trace{}, fmt.Errorf("trace %s not found", id)
	}
	return convert(data.Data[0]), nil
}

func convert(raw jaegerTrace) telemetry.Trace {
	trace := telemetry.Trace{ID: raw.TraceID}
	for _, s := range raw.Spans {
		span := telemetry.Span{
			ID: s.SpanID, Operation: s.OperationName, Service: raw.Processes[s.ProcessID].ServiceName,
			Start: time.UnixMicro(s.StartTime).UTC(), Duration: time.Duration(s.Duration) * time.Microsecond,
		}
		for _, ref := range s.References {
			if ref.RefType == "CHILD_OF" || span.ParentID == "" {
				span.ParentID = ref.SpanID
			}
		}
		tags := map[string]string{}
		for _, tag := range s.Tags {
			tags[tag.Key] = tagString(tag.Value)
		}
		span.Statement = firstTag(tags, "db.statement", "db.query.text")
		if span.Statement != "" {
			span.Statement = squash(span.Statement, maxStatement)
		}
		if tags["error"] == "true" || tags["otel.status_code"] == "ERROR" {
			span.Failed = true
		}
		for _, key := range []string{"http.status_code", "http.response.status_code"} {
			if code, err := strconv.Atoi(tags[key]); err == nil && code >= 500 {
				span.Failed = true
				span.Error = "HTTP " + tags[key]
			}
		}
		if span.Failed {
			if why := firstTag(tags, "otel.status_description", "error.message", "exception.message"); why != "" {
				span.Error = squash(why, 160)
			}
			for _, entry := range s.Logs {
				for _, field := range entry.Fields {
					if span.Error == "" && (field.Key == "message" || field.Key == "exception.message") {
						span.Error = squash(tagString(field.Value), 160)
					}
				}
			}
		}
		span.Route = firstTag(tags, "http.route", "url.path", "http.target")
		trace.Spans = append(trace.Spans, span)
	}
	return trace
}

func summarize(trace telemetry.Trace) telemetry.TraceSummary {
	summary := telemetry.TraceSummary{ID: trace.ID, Spans: len(trace.Spans)}
	if len(trace.Spans) == 0 {
		return summary
	}
	ids := map[string]bool{}
	for _, s := range trace.Spans {
		ids[s.ID] = true
	}
	var first, last time.Time
	services := map[string]bool{}
	var root *telemetry.Span
	var failure *telemetry.Span
	for i := range trace.Spans {
		s := &trace.Spans[i]
		services[s.Service] = true
		if first.IsZero() || s.Start.Before(first) {
			first = s.Start
		}
		if end := s.Start.Add(s.Duration); end.After(last) {
			last = end
		}
		if (s.ParentID == "" || !ids[s.ParentID]) && (root == nil || s.Start.Before(root.Start)) {
			root = s
		}
		if s.Failed && (failure == nil || s.Start.Before(failure.Start)) {
			failure = s
		}
	}
	summary.Start, summary.Duration = first, last.Sub(first)
	summary.RootService, summary.RootOperation = root.Service, root.Operation
	summary.Route = root.Route
	for name := range services {
		summary.Services = append(summary.Services, name)
	}
	sort.Strings(summary.Services)
	if failure != nil {
		summary.Failed = true
		summary.FirstFailure = failure.Service + " " + failure.Operation
		if failure.Error != "" {
			summary.FirstFailure += ": " + failure.Error
		}
	}
	return summary
}

func tagString(v any) string {
	switch value := v.(type) {
	case string:
		return value
	case bool:
		return strconv.FormatBool(value)
	case float64:
		return strconv.FormatFloat(value, 'f', -1, 64)
	}
	return fmt.Sprint(v)
}

func firstTag(tags map[string]string, keys ...string) string {
	for _, key := range keys {
		if tags[key] != "" {
			return tags[key]
		}
	}
	return ""
}

func squash(text string, limit int) string {
	text = strings.Join(strings.Fields(text), " ")
	if len(text) > limit {
		text = text[:limit] + "…"
	}
	return text
}

func (c Client) get(ctx context.Context, path string, values url.Values, into any) error {
	if c.BaseURL == "" {
		return errors.New("jaeger address is not configured")
	}
	target := strings.TrimSuffix(c.BaseURL, "/") + path
	if len(values) > 0 {
		target += "?" + values.Encode()
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return fmt.Errorf("jaeger %s: %w", path, err)
	}
	request.Header.Set("Accept", "application/json")
	client := c.HTTPClient
	if client == nil {
		client = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("jaeger %s: %w", path, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return fmt.Errorf("jaeger %s: read response: %w", path, err)
	}
	if len(body) > maxResponseBytes {
		return fmt.Errorf("jaeger %s: response exceeds %d bytes", path, maxResponseBytes)
	}
	if response.StatusCode < 200 || response.StatusCode > 299 {
		message := strings.TrimSpace(string(body))
		if len(message) > maxErrorBytes {
			message = message[:maxErrorBytes] + "…"
		}
		return fmt.Errorf("jaeger %s: HTTP %d: %s", path, response.StatusCode, message)
	}
	if err := json.Unmarshal(body, into); err != nil {
		return fmt.Errorf("jaeger %s: decode response: %w", path, err)
	}
	return nil
}

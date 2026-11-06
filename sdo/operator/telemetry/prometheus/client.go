// Package prometheus is the application-telemetry adapter for the Prometheus
// HTTP API. It implements telemetry.Client and telemetry.AlertReader, answers
// metric queries and reads alerts, and keeps every Prometheus wire detail out
// of the transport-neutral runtime.
package prometheus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/telemetry"
)

const (
	pathQuery      = "/api/v1/query"
	pathQueryRange = "/api/v1/query_range"
	pathAlerts     = "/api/v1/alerts"

	// maxResponseBytes bounds one response body; a larger one is an error,
	// never a truncated answer.
	maxResponseBytes = 32 << 20
	maxErrorBytes    = 512
)

// Client calls a Prometheus-compatible HTTP API. It sets no timeout of its
// own: callers bound each call with the context.
type Client struct {
	// BaseURL addresses the server, optionally with a path prefix.
	BaseURL string
	// Token, when set, returns the current bearer token. It is called on every
	// request so a rotated token is picked up.
	Token func() (string, error)
	// HTTPClient defaults to a client that follows no redirects.
	HTTPClient telemetry.HTTPDoer
}

// Query evaluates q.Text at time at: an instant query, or when q has a Range a
// range query ending at at. A response over the metrics bounds is an error,
// never a truncated answer: a cut series set would read as healthy. Samples
// that are NaN or infinite carry no value a detector can compare and are
// dropped, together with a series left without samples.
func (c Client) Query(ctx context.Context, q telemetry.Query, at time.Time) (telemetry.Result, error) {
	if q.Kind != telemetry.KindMetrics {
		return telemetry.Result{}, fmt.Errorf("prometheus serves %s queries, not %q", telemetry.KindMetrics, q.Kind)
	}
	values := url.Values{"query": {q.Text}}
	path := pathQuery
	if q.Range > 0 {
		step := metrics.Query{Range: q.Range, Step: q.Step}.EffectiveStep()
		path = pathQueryRange
		values.Set("start", unixSeconds(at.Add(-q.Range)))
		values.Set("end", unixSeconds(at))
		values.Set("step", strconv.FormatFloat(step.Seconds(), 'f', -1, 64))
	} else {
		values.Set("time", unixSeconds(at))
	}
	var data struct {
		ResultType string          `json:"resultType"`
		Result     json.RawMessage `json:"result"`
	}
	if err := c.get(ctx, path, values, &data); err != nil {
		return telemetry.Result{}, err
	}
	series, err := parseResult(data.ResultType, data.Result)
	if err != nil {
		return telemetry.Result{}, err
	}
	series = finiteSeries(series)
	if len(series) > telemetry.MaxSeries {
		return telemetry.Result{}, fmt.Errorf("query returned %d series, more than the limit of %d; narrow the expression", len(series), telemetry.MaxSeries)
	}
	samples := 0
	for _, item := range series {
		samples += len(item.Samples)
	}
	if samples > telemetry.MaxSamples {
		return telemetry.Result{}, fmt.Errorf("query returned %d samples, more than the limit of %d; narrow the expression or range", samples, telemetry.MaxSamples)
	}
	return telemetry.Result{Series: series}, nil
}

func finiteSeries(series []metrics.Series) []metrics.Series {
	kept := series[:0]
	for _, item := range series {
		samples := item.Samples[:0]
		for _, sample := range item.Samples {
			if !math.IsNaN(sample.Value) && !math.IsInf(sample.Value, 0) {
				samples = append(samples, sample)
			}
		}
		if len(samples) > 0 || len(item.Samples) == 0 {
			item.Samples = samples
			kept = append(kept, item)
		}
	}
	return kept
}

// Alerts reads every alert of the server, pending and firing.
func (c Client) Alerts(ctx context.Context) ([]metrics.Alert, error) {
	var data struct {
		Alerts []struct {
			Labels      map[string]string `json:"labels"`
			Annotations map[string]string `json:"annotations"`
			State       string            `json:"state"`
			ActiveAt    time.Time         `json:"activeAt"`
			Value       string            `json:"value"`
		} `json:"alerts"`
	}
	if err := c.get(ctx, pathAlerts, nil, &data); err != nil {
		return nil, err
	}
	alerts := make([]metrics.Alert, 0, len(data.Alerts))
	for _, alert := range data.Alerts {
		alerts = append(alerts, metrics.Alert{
			Labels: alert.Labels, Annotations: alert.Annotations, State: alert.State, ActiveAt: alert.ActiveAt.UTC(), Value: alert.Value,
		})
	}
	return alerts, nil
}

func unixSeconds(t time.Time) string {
	return strconv.FormatFloat(float64(t.UnixNano())/1e9, 'f', -1, 64)
}

type envelope struct {
	Status    string          `json:"status"`
	Data      json.RawMessage `json:"data"`
	ErrorType string          `json:"errorType"`
	Error     string          `json:"error"`
}

func (c Client) get(ctx context.Context, path string, values url.Values, into any) error {
	if c.BaseURL == "" {
		return errors.New("prometheus address is not configured")
	}
	target := strings.TrimSuffix(c.BaseURL, "/") + path
	if len(values) > 0 {
		target += "?" + values.Encode()
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return fmt.Errorf("prometheus %s: %w", path, err)
	}
	request.Header.Set("Accept", "application/json")
	if c.Token != nil {
		token, err := c.Token()
		if err != nil {
			return fmt.Errorf("prometheus %s: read bearer token: %w", path, err)
		}
		request.Header.Set("Authorization", "Bearer "+strings.TrimSpace(token))
	}
	client := c.HTTPClient
	if client == nil {
		client = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("prometheus %s: %w", path, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return fmt.Errorf("prometheus %s: read response: %w", path, err)
	}
	if len(body) > maxResponseBytes {
		return fmt.Errorf("prometheus %s: response exceeds %d bytes", path, maxResponseBytes)
	}
	var decoded envelope
	decodeErr := json.Unmarshal(body, &decoded)
	if response.StatusCode < 200 || response.StatusCode > 299 {
		message := strings.TrimSpace(string(body))
		if decodeErr == nil && decoded.Error != "" {
			message = decoded.ErrorType + ": " + decoded.Error
		}
		if len(message) > maxErrorBytes {
			message = message[:maxErrorBytes] + "…"
		}
		return fmt.Errorf("prometheus %s: HTTP %d: %s", path, response.StatusCode, message)
	}
	if decodeErr != nil {
		return fmt.Errorf("prometheus %s: decode response: %w", path, decodeErr)
	}
	if decoded.Status != "success" {
		return fmt.Errorf("prometheus %s: %s: %s", path, decoded.ErrorType, decoded.Error)
	}
	if len(decoded.Data) == 0 {
		return fmt.Errorf("prometheus %s: response has no result", path)
	}
	if err := json.Unmarshal(decoded.Data, into); err != nil {
		return fmt.Errorf("prometheus %s: decode result: %w", path, err)
	}
	return nil
}

func parseResult(resultType string, raw json.RawMessage) ([]metrics.Series, error) {
	switch resultType {
	case "vector":
		var vector []struct {
			Metric map[string]string `json:"metric"`
			Value  pair              `json:"value"`
		}
		if err := json.Unmarshal(raw, &vector); err != nil {
			return nil, fmt.Errorf("prometheus: decode vector: %w", err)
		}
		series := make([]metrics.Series, 0, len(vector))
		for _, item := range vector {
			series = append(series, metrics.Series{Labels: labelsOrEmpty(item.Metric), Samples: []metrics.Sample{item.Value.sample()}})
		}
		return series, nil
	case "matrix":
		var matrix []struct {
			Metric map[string]string `json:"metric"`
			Values []pair            `json:"values"`
		}
		if err := json.Unmarshal(raw, &matrix); err != nil {
			return nil, fmt.Errorf("prometheus: decode matrix: %w", err)
		}
		series := make([]metrics.Series, 0, len(matrix))
		for _, item := range matrix {
			samples := make([]metrics.Sample, 0, len(item.Values))
			for _, value := range item.Values {
				samples = append(samples, value.sample())
			}
			series = append(series, metrics.Series{Labels: labelsOrEmpty(item.Metric), Samples: samples})
		}
		return series, nil
	case "scalar":
		var value pair
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, fmt.Errorf("prometheus: decode scalar: %w", err)
		}
		return []metrics.Series{{Labels: map[string]string{}, Samples: []metrics.Sample{value.sample()}}}, nil
	}
	return nil, fmt.Errorf("prometheus: unsupported result type %q; detectors consume vectors, matrices and scalars", resultType)
}

func labelsOrEmpty(labels map[string]string) map[string]string {
	if labels == nil {
		return map[string]string{}
	}
	return labels
}

// pair is Prometheus's [unix seconds, "value"] sample encoding.
type pair struct {
	at    time.Time
	value float64
}

func (p pair) sample() metrics.Sample { return metrics.Sample{At: p.at, Value: p.value} }

func (p *pair) UnmarshalJSON(data []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil || len(raw) != 2 {
		return fmt.Errorf("sample must be [timestamp, value], got %s", truncate(string(data)))
	}
	var seconds float64
	if err := json.Unmarshal(raw[0], &seconds); err != nil {
		return fmt.Errorf("sample timestamp %s: %w", truncate(string(raw[0])), err)
	}
	var text string
	if err := json.Unmarshal(raw[1], &text); err != nil {
		return fmt.Errorf("sample value %s must be a string: %w", truncate(string(raw[1])), err)
	}
	value, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return fmt.Errorf("sample value %q: %w", text, err)
	}
	whole := int64(seconds)
	p.at = time.Unix(whole, int64((seconds-float64(whole))*1e9)).UTC()
	p.value = value
	return nil
}

func truncate(text string) string {
	if len(text) > 64 {
		return text[:64] + "…"
	}
	return text
}

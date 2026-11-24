package requestmetrics

import (
	"math"
	"strconv"
	"strings"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/traffic"
)

// ProbeCoverageLabel distinguishes scrape coverage samples from counts in the
// recent query. A "bracketed" sample certifies that every contributing series
// brackets the window after insetting both edges by ProbeScrapeInset.
const ProbeCoverageLabel = "sdo_probe_coverage"

// ProbeScrapeInset allows one normal scrape interval plus scheduling slack.
// ProbeClockSkew is an additional allowance on each side for clock skew.
const (
	ProbeScrapeInset = 6 * time.Second
	ProbeClockSkew   = time.Second
)

type probeBucket struct {
	route, method, status string
}

type probeStep struct {
	at time.Time
	probeBucket
}

type probeReceipts map[probeBucket][]traffic.HTTPReceipt

func probePathMatches(pattern, path string) bool {
	if traffic.HTTPPathKey(pattern) == path {
		return true
	}
	patternParts, parts := strings.Split(pattern, "/"), strings.Split(path, "/")
	if len(patternParts) != len(parts) {
		return false
	}
	for i, part := range patternParts {
		if traffic.HTTPPathKey(part) == parts[i] {
			continue
		}
		if parts[i] == "" || !strings.HasPrefix(part, "{") || !strings.HasSuffix(part, "}") || len(part) < 3 {
			return false
		}
	}
	return true
}

func probeObservations(ctx sdk.DetectionContext) []traffic.HTTPReceipt {
	set, ok := ctx.(traffic.WindowSet)
	if !ok {
		return nil
	}
	window := set.TrafficWindows()[traffic.HTTPReceiptWindowID]
	if window.LoadError != "" {
		return nil
	}
	return window.HTTPRequests
}

func (p entryPlan) bucket(labels map[string]string) probeBucket {
	return probeBucket{labels[p.RouteLabel], labels[p.MethodLabel], labels[p.StatusLabel]}
}

// probeDiscounts indexes matching receipts for this counter. The counter may
// expose status codes or status classes. If both encodings (or two method
// encodings) can claim a receipt, neither gets to spend it.
func (p entryPlan) probeDiscounts(operations []Operation, receipts []traffic.HTTPReceipt, result metrics.Result, window time.Duration) map[probeStep]float64 {
	if len(receipts) == 0 || p.Background || p.StatusLabel == "" {
		return nil
	}
	buckets := map[probeBucket]int{}
	bracketed := map[probeStep]bool{}
	for _, s := range result.Series {
		key := p.bucket(s.Labels)
		if s.Labels[ProbeCoverageLabel] == "bracketed" {
			for _, sample := range s.Samples {
				if sample.Value > 0 {
					bracketed[probeStep{sample.At, key}] = true
				}
			}
		} else {
			buckets[key]++
		}
	}
	if len(bracketed) == 0 {
		return nil
	}

	// Pick one route across the entry's bindings before looking at counters.
	// Otherwise overlapping path templates could spend the same receipt twice.
	routes := map[probeBucket][]probeBucket{}
	for key := range buckets {
		routeMethod := probeBucket{route: key.route, method: strings.ToUpper(key.method)}
		routes[routeMethod] = append(routes[routeMethod], key)
	}
	matched := probeReceipts{}
	for _, receipt := range receipts {
		if receipt.Service != p.Service || receipt.Status < 100 || receipt.Status > 599 ||
			receipt.SentAt.IsZero() || receipt.Answered.Before(receipt.SentAt) {
			continue
		}
		route, ok := p.probeRoute(operations, receipt)
		if !ok {
			continue
		}
		method := receipt.Method
		if p.MethodLabel == "" {
			method = ""
		}
		status := strconv.Itoa(receipt.Status)
		var key probeBucket
		matches := 0
		for _, candidate := range routes[probeBucket{route: route, method: method}] {
			bucketStatus := strings.ToLower(candidate.status)
			if bucketStatus == status || bucketStatus == status[:1]+"xx" || bucketStatus == status[:1] {
				key = candidate
				matches++
			}
		}
		if matches == 1 && buckets[key] == 1 {
			matched[key] = append(matched[key], receipt)
		}
	}

	out := map[probeStep]float64{}
	inset := ProbeScrapeInset + ProbeClockSkew
	for step := range bracketed {
		start, end := step.at.Add(-window+inset), step.at.Add(-inset)
		for _, receipt := range matched[step.probeBucket] {
			if receipt.SentAt.After(start) && receipt.Answered.Before(end) {
				out[step]++
			}
		}
	}
	return out
}

func (p entryPlan) probeRoute(operations []Operation, receipt traffic.HTTPReceipt) (string, bool) {
	var route string
	for _, op := range operations {
		if op.Unmeasurable != "" || op.Service != p.Service || op.ProbePort <= 0 || op.ProbePort != receipt.Port ||
			!strings.EqualFold(op.Method, receipt.Method) || !probePathMatches(op.ProbePath, receipt.PathKey) {
			continue
		}
		if route != "" && route != op.Route {
			return "", false
		}
		route = op.Route
	}
	return route, route != ""
}

func (c counts) subtract(own counts) counts {
	return counts{math.Max(0, c.total-own.total), math.Max(0, c.server-own.server), math.Max(0, c.client-own.client)}
}

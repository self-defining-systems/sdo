package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/telemetry"
)

// maxConcurrentPerSource caps the simultaneous requests one source receives
// in a cycle, so a detector bundle with many queries cannot flood a backend.
const maxConcurrentPerSource = 8

// resolverMetrics is the observer's MetricsAPI over every bound telemetry
// source: a query goes to the source it names (Query.Source), or to the
// default metrics source when it names none; alerts are the merge of every
// alert-capable source. The runtime passes query text through unparsed.
type resolverMetrics struct {
	resolver *telemetry.Resolver
	slots    map[string]chan struct{}
}

func newResolverMetrics(resolver *telemetry.Resolver) *resolverMetrics {
	slots := map[string]chan struct{}{}
	for _, source := range resolver.Sources() {
		slots[source.ID] = make(chan struct{}, maxConcurrentPerSource)
	}
	return &resolverMetrics{resolver: resolver, slots: slots}
}

func (m *resolverMetrics) acquire(ctx context.Context, id string) (func(), error) {
	slot, ok := m.slots[id]
	if !ok {
		return func() {}, nil
	}
	select {
	case slot <- struct{}{}:
		return func() { <-slot }, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("telemetry source %q unavailable: %w", id, ctx.Err())
	}
}

func (m *resolverMetrics) Query(ctx context.Context, query metrics.Query, at time.Time) ([]metrics.Series, error) {
	source, err := m.resolver.Resolve(query.Source, telemetry.KindMetrics)
	if err != nil {
		return nil, err
	}
	release, err := m.acquire(ctx, source.ID)
	if err != nil {
		return nil, err
	}
	defer release()
	request := telemetry.Query{SourceID: source.ID, Kind: telemetry.KindMetrics, Text: query.Expr}
	if query.Range > 0 {
		request.Range, request.Step = query.Range, query.EffectiveStep()
	}
	result, err := m.resolver.Query(ctx, request, at)
	if err != nil {
		return nil, err
	}
	if result.Truncated {
		return nil, fmt.Errorf("telemetry source %q truncated the answer; narrow the expression", source.ID)
	}
	return result.Series, nil
}

// Alerts reads every alert-capable source concurrently and merges the
// answers. A source that fails fails the read: an alert list that silently
// omits a source would read as healthy.
func (m *resolverMetrics) Alerts(ctx context.Context) ([]metrics.Alert, error) {
	ids := m.resolver.AlertSources()
	if len(ids) == 0 {
		return nil, errors.New("no bound telemetry source serves alerts")
	}
	type answer struct {
		alerts []metrics.Alert
		err    error
	}
	answers := make([]answer, len(ids))
	var wait sync.WaitGroup
	for i, id := range ids {
		wait.Add(1)
		go func() {
			defer wait.Done()
			release, err := m.acquire(ctx, id)
			if err != nil {
				answers[i].err = err
				return
			}
			defer release()
			answers[i].alerts, answers[i].err = m.resolver.Alerts(ctx, id)
		}()
	}
	wait.Wait()
	var merged []metrics.Alert
	var failures []string
	for _, a := range answers {
		if a.err != nil {
			failures = append(failures, a.err.Error())
			continue
		}
		merged = append(merged, a.alerts...)
	}
	if len(failures) > 0 {
		return nil, errors.New(strings.Join(failures, "; "))
	}
	return merged, nil
}

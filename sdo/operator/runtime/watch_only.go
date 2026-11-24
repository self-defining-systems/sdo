package runtime

import (
	"context"
	"time"
)

// watchOnlyDispatcher completes the response without claiming a worker.
type watchOnlyDispatcher struct {
	clock func() time.Time
}

func (d watchOnlyDispatcher) Dispatch(ctx context.Context, request IncidentRequest) (IncidentResult, error) {
	return d.DispatchWithClaim(ctx, request, nil)
}

func (d watchOnlyDispatcher) DispatchWithClaim(_ context.Context, request IncidentRequest, _ func()) (IncidentResult, error) {
	at := d.clock().UTC()
	return IncidentResult{
		SchemaVersion: ProtocolSchemaVersion, IncidentID: request.IncidentID, Status: IncidentCompleted,
		Timing: TimingMetrics{StartedAt: at, CompletedAt: at},
	}, nil
}

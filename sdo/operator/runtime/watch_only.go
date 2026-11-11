package runtime

import "context"

// watchOnlyDispatcher is the response of a watch-only controller: it starts no
// worker and makes no change, and returns only when the controller ends the
// incident's response (closure on independently verified health, or the
// response deadline). It never claims a dispatch, so lifecycle telemetry
// records no responder.
type watchOnlyDispatcher struct{}

func (watchOnlyDispatcher) Dispatch(ctx context.Context, request IncidentRequest) (IncidentResult, error) {
	return watchOnlyDispatcher{}.DispatchWithClaim(ctx, request, nil)
}

func (watchOnlyDispatcher) DispatchWithClaim(ctx context.Context, _ IncidentRequest, _ func()) (IncidentResult, error) {
	<-ctx.Done()
	return IncidentResult{}, ctx.Err()
}

package runtime

import (
	"context"
)

type fastloopDispatch struct {
	requests chan IncidentRequest
	results  chan IncidentResult
}

func (d fastloopDispatch) Dispatch(ctx context.Context, request IncidentRequest) (IncidentResult, error) {
	select {
	case d.requests <- request:
	case <-ctx.Done():
		return IncidentResult{}, ctx.Err()
	}
	select {
	case result := <-d.results:
		return result, nil
	case <-ctx.Done():
		return IncidentResult{}, ctx.Err()
	}
}

package runtime

import (
	"context"
	"errors"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/latefindings"
)

// lateFindingSink projects controller-authored telemetry into the incident's
// read-only worker ConfigMap. The durable firing stream remains its source.
type lateFindingSink struct {
	Downstream FiringSink
	Store      latefindings.Store
}

func (s lateFindingSink) Record(record *contracts.FiringRecord) error {
	var downstreamErr error
	if s.Downstream != nil {
		downstreamErr = s.Downstream.Record(record)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return errors.Join(downstreamErr, s.Store.Update(ctx, record))
}

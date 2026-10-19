package runtime

import (
	"context"
	"errors"
	"time"

	"sdo.dev/operator/latefindings"
)

// lateFindingSink projects controller-authored telemetry into the incident's
// read-only worker ConfigMap. The durable firing stream remains its source.
type lateFindingSink struct {
	Downstream FiringSink
	Store      latefindings.Store
}

func (s lateFindingSink) Record(record FiringRecord) error {
	var downstreamErr error
	if s.Downstream != nil {
		downstreamErr = s.Downstream.Record(record)
	}
	bindings := make(map[string]any, len(record.ParameterBindings))
	for name, value := range record.ParameterBindings {
		bindings[name] = value
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	projectionErr := s.Store.Update(ctx, latefindings.Event{
		IncidentID: record.IncidentID, Relation: string(record.DispatchRelation), Event: string(record.Event),
		DetectorID: record.DetectorID, DetectorClass: record.DetectorClass, RuleID: record.RuleID,
		Fingerprint: record.Fingerprint, Severity: record.Severity, RecordedAt: record.RecordedAt.UTC().Format(time.RFC3339Nano),
		ParameterBindings: bindings, PlaybookIDs: append([]string(nil), record.SurfacedPlaybooks...),
	})
	return errors.Join(downstreamErr, projectionErr)
}

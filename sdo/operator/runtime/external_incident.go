package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/runtime/lifecyclecore"
)

// ErrExternalIncidentBusy tells an ingress to retain the alert and retry.
var ErrExternalIncidentBusy = errors.New("an incident is already open")

// ExternalIncident is a caller's alert, without detector or benchmark authority.
// ID must be stable across retries. A namespace has at most one open incident.
type ExternalIncident struct{ ID, Namespace, Message string }

// OpenExternalIncident accepts a namespace-scoped alert into the same durable
// workspace/dispatch/closure lifecycle as detected incidents. PersistState must
// succeed before the caller acknowledges delivery. External-only controllers
// never execute detectors and close only after the responder returns.
func (c *Controller) OpenExternalIncident(ctx context.Context, now time.Time, alert ExternalIncident) error {
	defer c.lockObservation("external_incident")()
	if err := c.openExternalIncident(now, alert); err != nil {
		return err
	}
	return c.PersistState(ctx)
}

func (c *Controller) openExternalIncident(now time.Time, alert ExternalIncident) error {
	if !c.config.ExternalIncidentsOnly {
		return fmt.Errorf("external incident entry is disabled")
	}
	if err := wire.Validate(&contracts.ExternalIncident{Id: alert.ID, Namespace: alert.Namespace, Message: alert.Message}); err != nil {
		return err
	}
	if strings.TrimSpace(alert.Message) == "" || alert.Namespace != c.config.Namespace {
		return fmt.Errorf("external incident must name this namespace and a message")
	}
	c.mu.Lock()
	if alert.ID == c.lastAcknowledgedIncidentID {
		c.mu.Unlock()
		return nil
	}
	if r := c.currentIncidentRequest; r != nil && r.IncidentID == alert.ID {
		same := r.ExternalMessage == alert.Message && r.Namespace == alert.Namespace
		c.mu.Unlock()
		if !same {
			return fmt.Errorf("external incident identity changed")
		}
		return nil
	}
	if c.pendingClosure != nil && c.pendingClosure.Request.IncidentID == alert.ID {
		same := c.pendingClosure.Request.ExternalMessage == alert.Message
		c.mu.Unlock()
		if !same {
			return fmt.Errorf("external incident identity changed")
		}
		return nil
	}
	busy := c.incidentOpen || c.closureBlocksNextLocked()
	c.mu.Unlock()
	// Retain the alert in the caller's inbox until current work is acknowledged.
	if busy {
		return ErrExternalIncidentBusy
	}
	request := IncidentRequest{SchemaVersion: ProtocolSchemaVersion, Application: c.config.Application, Namespace: c.config.Namespace,
		IncidentID: alert.ID, ExternalMessage: alert.Message, SourceCommit: c.config.SourceCommit, DeployedCommit: c.config.DeployedCommit,
		RepositoryWorktree: c.config.RepositoryWorktree, RepositoryBaseCommit: c.config.SourceCommit,
		ResponseDeadline: now.Add(c.config.ResponseTimeout).UTC(), CancellationToken: "cancel-" + alert.ID, RepairPolicy: c.config.RepairPolicy}
	c.beginIncident(now, request, nil)
	return nil
}

func (c *Controller) closeExternalIncident(now time.Time) {
	c.mu.Lock()
	if !c.incidentOpen || c.currentIncidentRequest == nil || !c.responderDone {
		c.mu.Unlock()
		return
	}
	closure := IncidentClosure{Request: *cloneIncidentRequest(c.currentIncidentRequest), Result: cloneIncidentResult(c.currentIncidentResult),
		DispatchError: c.dispatchError, DetectedAt: c.incidentDetectedAt, DispatchedAt: c.incidentDispatchedAt,
		ResponderCompletedAt: c.responderCompletedAt, VerifiedAt: now.UTC(),
		UnconfirmedReason: contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_EXTERNAL_COMPLETION,
		UnconfirmedDetail: "Responder completed; no independent detector verification was requested."}
	projected, err := incidentClosureContract(closure)
	if err == nil {
		_, err = c.stepClosureLocked(lifecyclecore.Event{Kind: lifecyclecore.Begin, At: now, Closure: projected})
	}
	if err != nil {
		c.mu.Unlock()
		c.reportBrokerError("external closure", err)
		return
	}
	c.clearIncidentLocked()
	c.mu.Unlock()
	if c.OnIncidentClosed != nil {
		c.OnIncidentClosed(closure)
	}
}

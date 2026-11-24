package runtime

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/sdk"
)

// A page is a person's or an on-call system's report through the external
// incident inbox while the controller runs its detectors (detector mode). It
// is the one opener that is not a detector alert:
//
//   - with no incident open it opens one whose alert set starts empty, and the
//     responder is dispatched once with the page text (IncidentRequest.Page);
//   - with an incident open it joins it: the request records the page and the
//     responder is never re-prompted;
//   - a page that opened an incident requires its responder to return.
//     A page joined during a running responder turn also waits for its return.
//     During automatic recovery of an alert-opened incident, a joined page is
//     evidence and the opening and joined alerts decide verification.
//
// A page is accepted once (lastPageID, persisted); the producer may leave it in
// the inbox. One that cannot open yet (a closure in flight, the start-up
// period) stays unaccepted and is read again. Watch-only controllers never
// dispatch, so they ignore pages.

// pageOpened reports an incident a page opened rather than a detector alert.
func pageOpened(request *IncidentRequest) bool {
	return request != nil && request.Page != "" && len(request.OpeningKeys) == 0
}

// awaitsResponderReturn prevents early closure of a running responder's
// external incident or an incident carrying a page.
func awaitsResponderReturn(request *IncidentRequest) bool {
	return request != nil && (request.ExternalMessage != "" || request.Page != "")
}

// pageText is the page that opened the incident, from either inbox mode.
func pageText(request IncidentRequest) string {
	if request.ExternalMessage != "" {
		return request.ExternalMessage
	}
	if pageOpened(&request) {
		return request.Page
	}
	return ""
}

// readPage reads the inbox once and admits a page it has not accepted yet.
// Inbox faults never stop detection: they are logged once per reason.
func (c *Controller) readPage(ctx context.Context, now time.Time, snapshot sdk.DetectionContext) {
	if c.ExternalIncidents == nil || c.config.WatchOnly || c.config.ExternalIncidentsOnly {
		return
	}
	alert, err := c.ExternalIncidents(ctx)
	if err == nil && alert != nil {
		err = c.admitPage(ctx, now, snapshot, *alert)
	}
	if err == nil || errors.Is(err, ErrExternalIncidentBusy) {
		c.rejectedExternalIncident = ""
		return
	}
	if reason := err.Error(); reason != c.rejectedExternalIncident {
		c.rejectedExternalIncident = reason
		slog.Warn("page_rejected", "error", reason)
	}
}

func (c *Controller) admitPage(ctx context.Context, now time.Time, snapshot sdk.DetectionContext, alert ExternalIncident) error {
	if err := wire.Validate(&contracts.ExternalIncident{Id: alert.ID, Namespace: alert.Namespace, Message: alert.Message}); err != nil {
		return errors.Join(ErrInvalidExternalIncident, err)
	}
	if strings.TrimSpace(alert.Message) == "" || alert.Namespace != c.config.Namespace {
		return errors.Join(ErrInvalidExternalIncident, errors.New("page "+alert.ID+" must name this namespace and a message"))
	}
	c.mu.Lock()
	if alert.ID == c.lastPageID {
		c.mu.Unlock()
		return nil
	}
	if c.incidentOpen && c.currentIncidentRequest != nil {
		request := c.currentIncidentRequest
		if request.Page == "" {
			// Preserve legacy detector-opened requests whose alert keys were
			// implicit before a joined page made an empty key set significant.
			if len(request.OpeningKeys) == 0 {
				request.OpeningKeys = openingKeys(request)
			}
			request.Page = alert.Message
			if c.incidentDispatchedAt.IsZero() {
				request.PreparedAgentTask = ""
			}
		}
		c.lastPageID = alert.ID
		slog.Info("page_joined", "incident_id", request.IncidentID, "page_id", alert.ID)
		c.mu.Unlock()
		return nil
	}
	busy := c.earlyReportPendingLocked() || c.closureBlocksNextLocked()
	c.mu.Unlock()
	// Like any opener, a page waits out a new install's start-up period.
	if busy || c.startupHolds(now) {
		return ErrExternalIncidentBusy
	}
	if snapshot == nil {
		var err error
		if snapshot, err = c.provider.Snapshot(ctx); err != nil {
			return err
		}
	}
	// The clues active now ride as evidence; none is an alert of the page.
	clues := c.activeClues(nil)
	request := c.incidentRequest(now, clues)
	request.Page = alert.Message
	request.StandingKeys = findingKeys(clues)
	request.PassingTests = passingTests(snapshot)
	request.ObservationGeneration = observationGeneration(snapshot)
	if c.Baseline != nil {
		request.StateChanges = cloneStateChanges(c.Baseline.Changes(now))
	}
	request.StateChanges = withStoreChanges(request.StateChanges, snapshot, now)
	c.beginIncident(now, request, clues)
	c.mu.Lock()
	c.lastPageID = alert.ID
	c.mu.Unlock()
	slog.Info("page_opened", "incident_id", request.IncidentID, "page_id", alert.ID)
	if c.config.CorpusSessionRoot != "" {
		if err := c.bindIncidentHealthyBaseline(request.IncidentID); err != nil {
			c.reportHealthyCaptureError(err)
		}
		c.captureOpenDetect(ctx, snapshot, request)
	}
	return nil
}

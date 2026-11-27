package runtime

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// A page is a person's or an on-call system's report through the external
// incident inbox while the controller runs its detectors (detector mode). It
// is the one opener that is not a detector alert:
//
//   - with no incident open it opens one whose alert set starts empty, and the
//     responder starts with the page text, after at most one automatic attempt
//     (pageScanBudget) and directly during startup;
//   - with an incident open it joins it: the request records the page and the
//     responder is never re-prompted;
//   - a page that opened an incident requires its responder to return.
//     A page joined during a running responder turn also waits for its return.
//     During automatic recovery of an alert-opened incident, a joined page is
//     evidence and the opening and joined alerts decide verification.
//
// A page is accepted once (lastPageID, persisted); the producer may leave it in
// the inbox. A page arriving during a closure stays unaccepted and is read
// again. Watch-only controllers ignore pages.

// A page's automatic attempt reads for at most pageDiagnoseBudget and reaches
// its write, if any, within pageScanBudget; the responder then starts.
const (
	pageDiagnoseBudget = time.Second
	pageScanBudget     = 2 * time.Second
)

// pageMatch reports whether a scan answered with one Match and nothing else
// that could be one: only its own diagnosis ties the playbook to the page.
func pageMatch(runs []ProgramRun) bool {
	matches := 0
	for _, run := range runs {
		switch run.Verdict.Result {
		case program.Match:
			matches++
		case program.NoMatch:
		default:
			return false
		}
	}
	return matches == 1
}

// pageOpened reports an incident a page opened rather than a detector alert.
func pageOpened(request *IncidentRequest) bool {
	return request != nil && request.Page != "" && len(request.OpeningKeys) == 0
}

// awaitsResponderReturn prevents early closure of a running responder's
// external incident or an incident carrying a page.
func awaitsResponderReturn(request *IncidentRequest) bool {
	return request != nil && (request.ExternalMessage != "" || request.Page != "")
}

// pageDirect reports a page the responder starts on without the automatic
// stage: during startup, when policy rules out the agent the page needs after
// a playbook, or when its task is already frozen. Caller holds c.mu.
func (c *Controller) pageDirect(request IncidentRequest) bool {
	return pageOpened(&request) && deterministicStageReport(request.PriorStages) == nil &&
		(c.startup.holding(c.clock()) || request.PreparedAgentTask != "" ||
			c.config.AfterPlaybook != "" && c.config.AfterPlaybook != settings.AfterPlaybookAgent)
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
	// A page waits, retained in the inbox, while the previous incident's
	// closure or its work (closing turn, cleanup, source publication) runs.
	busy := c.earlyReportPendingLocked() || c.closureBlocksNextLocked() || c.priorIncidentWorkLocked() != ""
	c.mu.Unlock()
	if busy {
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
	request.RecentChanges = c.heldTargetLines()
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

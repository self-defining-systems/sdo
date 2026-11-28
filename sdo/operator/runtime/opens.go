package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/requestmetrics"
	"sdo.dev/operator/sdk/traffic"
)

// openers is the per-commit view opens reads: the scenario IDs of the user
// tests, or nil when the accepted map is unreadable, and the accepted map.
type openers struct {
	mu          sync.Mutex
	loaded      bool
	commit      string
	userTests   map[string]bool
	acceptedMap appmap.Map
}

// opens reports whether a finding is an alert that opens an incident:
//
//   - a failing user test: a scenario finding of a Go-installed health-probe
//     traffic detector whose scenario covers an operation entering at a user
//     entry Service (appmap.Coverage). Without a readable accepted map (or a
//     git reader) every health-probe scenario counts;
//
//   - a Kubernetes failure condition that outlived Kubernetes' own
//     self-healing: any finding of the generic condition detectors, which
//     report a rule only after its give-up signal or its self-heal hold.
//
//   - failing real user traffic: a request-metrics error finding of a declared
//     operation or an undeclared route, unless shadow mode is on
//     (detection.request_metrics_shadow), where it is a clue. Failing
//     background jobs (an entry only internal operations enter) are clues. A
//     not-instrumented note is always a clue.
//
// Every other finding (drift, wiring, events, saturation, links, store state,
// data checks, metrics alerts, authored and learned detectors) is a clue: it
// never opens or holds an incident.
func (c *Controller) opens(finding sdk.Finding) bool {
	return c.openingRole(finding, nil) != noOpener
}

type openingRole uint8

const (
	noOpener openingRole = iota
	userTrafficOpener
	conditionOpener
	userTestOpener
)

// openingRole classifies an observed rule or, when possibleUserTest is set,
// a detector whose observation is missing. For a missing observation a rule
// that could have been emitted counts, even when its health is unknown.
func (c *Controller) openingRole(finding sdk.Finding, possibleUserTest func() bool) openingRole {
	switch {
	case finding.DetectorID == memory.RequestMetricsDetectorID:
		if c.config.RequestMetricsOpens && finding.Metadata[requestmetrics.MetaBackground] != true &&
			(finding.RuleID == requestmetrics.RuleErrors || possibleUserTest != nil) {
			return userTrafficOpener
		}
	case slices.Contains(memory.KubernetesConditionDetectorIDs(), finding.DetectorID):
		return conditionOpener
	case strings.HasPrefix(finding.DetectorID, memory.TrafficDetectorPrefix):
		if possibleUserTest != nil {
			if possibleUserTest() {
				return userTestOpener
			}
		} else if scenario, ok := strings.CutPrefix(finding.RuleID, traffic.RuleIDPrefix); ok {
			tests := c.userTests()
			if tests == nil || tests[scenario] {
				return userTestOpener
			}
		}
	}
	return noOpener
}

// verificationOpeners identifies the detectors whose absent observation can
// hide an opening rule. Use the accepted probe catalog even when the prober
// cannot supply a window. Fall back to a possible user test if the map or
// workload cannot be read, just as opens falls back for an unreadable map.
func (c *Controller) verificationOpeners(detectors []sdk.Detector, catalog []appmap.Workload) map[string]bool {
	workloads := map[string]appmap.Workload{}
	for _, workload := range catalog {
		workloads[workload.Name] = workload
	}
	result := map[string]bool{}
	for _, detector := range detectors {
		id := detector.Spec().ID
		result[id] = c.openingRole(sdk.Finding{DetectorID: id}, func() bool {
			name := traffic.ScenarioSLOWorkload(detector)
			if name == "" {
				return false
			}
			tests := c.userTests()
			workload, ok := workloads[name]
			if tests == nil {
				return true
			}
			if !ok {
				return len(tests) > 0
			}
			for _, scenario := range workload.Scenarios {
				if tests[scenario] && !slices.Contains(workload.KnownBroken, scenario) {
					return true
				}
			}
			return false
		}) != noOpener
	}
	return result
}

// AlertJoinWindow bounds when a new opener belongs to the same incident.
// After the window an independent fault pages at once (pageLate) and opens the
// next incident.
const AlertJoinWindow = 5 * time.Minute

// joinAlertSet admits a user test or failing user traffic into the alert set
// while something still works the incident: the agent before it returns (the
// early-report path included) or the automatic stage while it owns the
// repair, within the join window. Kubernetes conditions never join. After the
// responder returned, a new opener is a symptom nobody works on: it waits in
// the batcher and opens the next incident once this one closes. An opener
// already in the set refreshes it and withdraws recovery. Joining never
// dispatches or re-prompts a responder.
func (c *Controller) joinAlertSet(finding sdk.Finding, now time.Time) bool {
	role := c.openingRole(finding, nil)
	if role == noOpener {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	request, detected := c.currentIncidentRequest, c.incidentDetectedAt
	pending := !c.incidentOpen && c.provisionalClosureLocked()
	if pending && request == nil {
		request, detected = &c.pendingClosure.Request, c.pendingClosure.DetectedAt
	}
	if request == nil || (!c.incidentOpen && !pending) {
		return false
	}
	key := findingKey(finding)
	if slices.Contains(alertKeys(request), key) {
		c.withdrawRecoveryLocked(now, finding)
		return true
	}
	if role == conditionOpener || now.Sub(detected) > AlertJoinWindow {
		return false
	}
	// Before the responder returns it sees the joiner in sdo detect; the
	// automatic stage evaluates it while it owns the repair.
	early := c.earlyReportPendingLocked()
	running := !c.responderDone && (c.incidentOpen || early)
	if !running && c.awaitingAutomaticRecoveryLocked() == nil {
		return false
	}
	if len(request.OpeningKeys) == 0 && request.Page == "" {
		request.OpeningKeys = openingKeys(request)
	}
	request.JoinedKeys = append(request.JoinedKeys, key)
	sort.Strings(request.JoinedKeys)
	c.joinCapturesDue = append(c.joinCapturesDue, key)
	finding.Fingerprint = FindingFingerprint(finding)
	request.Findings = append(request.Findings, finding)
	sortFindings(request.Findings)
	if !c.incidentDispatchedAt.IsZero() {
		c.markLateJoinedLocked(key)
	}
	c.publishHeldLocked()
	c.closureStableSince = time.Time{}
	c.recoverySafetyLost = true
	if early {
		c.cacheVerificationLocked(ToolResponse{Command: "verify", Summary: "recovery proof invalidated by a joined alert", Findings: []sdk.Finding{finding}})
		c.withdrawRecoveryLocked(now, finding)
	}
	return true
}

// pageLate pages a user test or failing user traffic that starts failing
// while an incident is open but does not join its alert set (past the join
// window, or after the responder returned). The controller works one incident
// at a time, and the open one can stay open as long as its responder works or
// its own alerts keep failing, so the page cannot wait for the next incident:
// it is recorded on the open incident (LatePages, alert_paged) when the
// failure is detected, once per key. It never joins the alert set or
// re-prompts a responder; the finding still waits in the batcher and opens the
// next incident. Kubernetes conditions and clues never page late.
func (c *Controller) pageLate(finding sdk.Finding, now time.Time) {
	switch c.openingRole(finding, nil) {
	case userTestOpener, userTrafficOpener:
	default:
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	request := c.currentIncidentRequest
	if !c.incidentOpen || request == nil {
		return
	}
	key := findingKey(finding)
	if slices.Contains(alertKeys(request), key) || slices.ContainsFunc(request.LatePages, func(page LatePage) bool { return page.Key == key }) {
		return
	}
	request.LatePages = append(request.LatePages, LatePage{Key: key, PagedAt: now.UTC()})
	slog.Info("late_alert_paged", "incident_id", request.IncidentID, "finding_key", key)
	c.emitLifecycleLocked(&contracts.LifecycleEvent{
		Event: PhaseAlertPaged, EventId: lifecycleEventID(request.IncidentID, PhaseAlertPaged, key),
		RecordedAt: contractTimestamp(now), IncidentId: request.IncidentID, FindingKeys: []string{key},
	})
}

// captureJoinedAlerts stores the open state alerts joined the incident in, or
// new findings first appeared in, from the observation they activated in. A
// fault that starts while the incident is open is absent from the opening
// captures; without this one the closing turn's own-incident check rejects a
// playbook written for it. A join capture can follow SDO's or the agent's
// writes, so it also records whether a known write preceded it. Evidence
// only, never a gate: a failure is reported and the incident goes on.
func (c *Controller) captureJoinedAlerts(snapshot sdk.DetectionContext) {
	c.mu.Lock()
	due, request, result, loop := c.joinCapturesDue, c.currentIncidentRequest, c.currentIncidentResult, c.responderLoop
	c.joinCapturesDue = nil
	var id, namespace string
	var refs []sdk.ObjectRef
	var stageWrote bool
	var found []sdk.Finding
	if request != nil {
		id, namespace = request.IncidentID, request.Namespace
		found = c.takeFindingCapturesLocked(id)
		refs = findingCaptureRefs(append(slices.Clone(request.Findings), found...))
		stageWrote = playbookApplied(request.PriorStages)
	}
	var resultPlans, resultActions bool
	if result != nil {
		resultPlans, resultActions = len(result.PlanApplications) > 0, slices.ContainsFunc(result.RepairActions, func(r RepairActionReceipt) bool { return r.Success })
	}
	c.mu.Unlock()
	if len(due)+len(found) == 0 || id == "" || c.config.CorpusSessionRoot == "" || snapshot == nil {
		return
	}
	joined, err := memory.NextJoinCapture(c.config.CorpusSessionRoot, id)
	if err != nil {
		if c.OnError != nil {
			c.OnError(fmt.Errorf("number the join capture: %w", err))
		}
		return
	}
	// One capture per pass holds every alert that joined and every finding
	// that appeared in it.
	if err := memory.CaptureIncidentSnapshot(c.config.CorpusSessionRoot, id, memory.JoinedCapturePhase(joined), snapshot, namespace, refs...); err != nil && c.OnError != nil {
		c.OnError(fmt.Errorf("capture joined alert open state: %w", err))
		return
	}
	joinContext := memory.JoinCaptureContext{JoinedKeys: due}
	for _, finding := range found {
		joinContext.NewFindings = append(joinContext.NewFindings, FindingStateKey(finding.DetectorID, finding.Fingerprint))
	}
	if stageWrote {
		joinContext.WriteSources = append(joinContext.WriteSources, "automatic_stage")
	}
	responderPlans, recorded := resultPlans, false
	if loop != nil {
		loop.locked(func() {
			responderPlans = responderPlans || len(loop.applicationsLog) > 0
			if loop.ExternalReceipts != nil {
				joinContext.Recording = true
				receipts, _, _, err := loop.ExternalReceipts()
				recorded = recorded || err == nil && len(receipts) > 0
			}
		})
	}
	if responderPlans {
		joinContext.WriteSources = append(joinContext.WriteSources, "responder_plan")
	}
	if recorded {
		joinContext.WriteSources = append(joinContext.WriteSources, "agent_recording")
	}
	if resultActions {
		joinContext.WriteSources = append(joinContext.WriteSources, "repair_action")
	}
	joinContext.KnownWritesBefore = len(joinContext.WriteSources) > 0
	if err := memory.CaptureJoinContext(c.config.CorpusSessionRoot, id, joined, joinContext); err != nil && c.OnError != nil {
		c.OnError(fmt.Errorf("record joined alert capture context: %w", err))
	}
}

// nextIncidentWaitingLocked reports a confirmed opener in the batcher: a
// symptom no one works on that opens the next incident once this one closes.
// Caller holds c.mu.
func (c *Controller) nextIncidentWaitingLocked(now time.Time) bool {
	return c.batcher.Ready(now)
}

// handOverStillFailingLocked queues every alert of a closing escalated
// incident that still fails, so the next incident opens on all of them with
// the waiting opener. Caller holds c.mu.
func (c *Controller) handOverStillFailingLocked(request IncidentRequest, now time.Time) {
	states := c.tracker.Snapshot()
	for _, key := range alertKeys(&request) {
		if state, ok := states[key]; ok && state.Active {
			c.batcher.AddAt(state.Finding, now)
		}
	}
}

// userTests reads the accepted map once per source commit.
func (c *Controller) userTests() map[string]bool {
	tests, _ := c.openerView()
	return tests
}

// openerView is the user tests and the accepted map, read once per source
// commit.
func (c *Controller) openerView() (map[string]bool, appmap.Map) {
	c.openers.mu.Lock()
	defer c.openers.mu.Unlock()
	c.mu.Lock()
	commit, repository := c.config.SourceCommit, c.config.RepositoryWorktree
	c.mu.Unlock()
	if c.openers.loaded && c.openers.commit == commit {
		return c.openers.userTests, c.openers.acceptedMap
	}
	m, document := acceptedVerificationMap(context.Background(), c.gitRead, IncidentRequest{SourceCommit: commit, RepositoryWorktree: repository})
	var tests map[string]bool
	if len(m.Components) > 0 {
		tests = map[string]bool{}
		for _, component := range m.Coverage(appmap.ParseInventory(document), nil).Components {
			for _, scenario := range component.Probes {
				tests[scenario] = true
			}
		}
	}
	c.openers.loaded, c.openers.commit, c.openers.userTests, c.openers.acceptedMap = true, commit, tests, m
	return tests, m
}

// openingKeys are the request's opening alerts. A request persisted before
// opening keys existed opened on all of its findings.
func openingKeys(request *IncidentRequest) []string {
	if request == nil {
		return nil
	}
	if len(request.OpeningKeys) > 0 {
		return request.OpeningKeys
	}
	if request.Page != "" {
		// A page opened it: its findings are clues, not opening alerts.
		return nil
	}
	return findingKeys(request.Findings)
}

// alertKeys are the incident's opening and joined alerts. Recovered waits on
// all of them. Credit and symptom matching use openingKeys.
func alertKeys(request *IncidentRequest) []string {
	if request == nil {
		return nil
	}
	keys := openingKeys(request)
	if len(request.JoinedKeys) == 0 {
		return keys
	}
	return append(slices.Clone(keys), request.JoinedKeys...)
}

// activeClues returns the confirmed active findings the tracker holds that do
// not open an incident, so an incident's request carries the clues present
// when it opened.
func (c *Controller) activeClues(exclude []sdk.Finding) []sdk.Finding {
	skip := map[string]bool{}
	for _, finding := range exclude {
		skip[FindingStateKey(finding.DetectorID, FindingFingerprint(finding))] = true
	}
	var clues []sdk.Finding
	for key, state := range c.tracker.Snapshot() {
		if !state.Active || skip[key] || c.opens(state.Finding) {
			continue
		}
		clues = append(clues, state.Finding)
	}
	sortFindings(clues)
	return clues
}

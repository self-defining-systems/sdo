package runtime

import (
	"context"
	"path"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/requestmetrics"
	"sdo.dev/operator/sdk/traffic"
)

// openers is the per-commit view opens reads: the scenario IDs of the user
// tests, or nil when the accepted map is unreadable.
type openers struct {
	mu        sync.Mutex
	loaded    bool
	commit    string
	userTests map[string]bool
	workloads map[string]appmap.Workload
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

// closureOpeners uses the accepted workload files even when a probe is down.
func (c *Controller) closureOpeners(ctx context.Context, detectors []sdk.Detector) map[string]bool {
	tests := c.userTests()
	c.openers.mu.Lock()
	var catalog []appmap.Workload
	if c.gitRead != nil && tests != nil {
		c.mu.Lock()
		repository := c.config.RepositoryWorktree
		c.mu.Unlock()
		for _, detector := range detectors {
			name := traffic.ScenarioSLOWorkload(detector)
			if name == "" {
				continue
			}
			workload, loaded := c.openers.workloads[name]
			if !loaded {
				file := name + ".yaml"
				content, err := c.gitRead(ctx, repository, "show", c.openers.commit+":"+path.Join(layout.TrafficWorkloadsDir, file))
				if err == nil {
					workload, _ = memory.MapWorkload(file, content)
				}
				if ctx.Err() == nil {
					c.openers.workloads[name] = workload
				}
			}
			if workload.Name != "" {
				catalog = append(catalog, workload)
			}
		}
	}
	c.openers.mu.Unlock()
	return c.verificationOpeners(detectors, catalog)
}

// AlertJoinWindow bounds when a new opener belongs to the same incident.
// After the window an independent fault can open the next incident.
const AlertJoinWindow = 5 * time.Minute

// joinAlertSet admits an opener within the join window, including while the
// outcome is provisional. Joining never dispatches or re-prompts a responder.
func (c *Controller) joinAlertSet(finding sdk.Finding, now time.Time) bool {
	if !c.opens(finding) {
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
		return true
	}
	if now.Sub(detected) > AlertJoinWindow {
		return false
	}
	if pending && !c.earlyReportPendingLocked() {
		if err := c.reobserveClosureLocked(now, request); err != nil {
			return false
		}
		request = c.currentIncidentRequest
	}
	if len(request.OpeningKeys) == 0 && request.Page == "" {
		request.OpeningKeys = openingKeys(request)
	}
	request.JoinedKeys = append(request.JoinedKeys, key)
	sort.Strings(request.JoinedKeys)
	finding.Fingerprint = FindingFingerprint(finding)
	request.Findings = append(request.Findings, finding)
	sortFindings(request.Findings)
	if !c.incidentDispatchedAt.IsZero() {
		c.markLateJoinedLocked(key)
	}
	c.publishHeldLocked()
	c.closureStableSince = time.Time{}
	c.recoverySafetyLost = true
	if c.earlyReportPendingLocked() {
		c.cacheVerificationLocked(ToolResponse{Command: "verify", Summary: "recovery proof invalidated by a joined alert", Findings: []sdk.Finding{finding}})
	} else if c.responderDone && (pending || c.awaitingAutomaticRecoveryLocked() == nil) {
		// The provisional outcome already ended response. During an automatic
		// wait the first responder handoff can still be due.
		c.escalateLocked(now)
	}
	// A new alert reopens a settled automatic stage while repair is ongoing.
	c.midStage.settled = false
	return true
}

// userTests reads the accepted map once per source commit.
func (c *Controller) userTests() map[string]bool {
	c.openers.mu.Lock()
	defer c.openers.mu.Unlock()
	c.mu.Lock()
	commit, repository := c.config.SourceCommit, c.config.RepositoryWorktree
	c.mu.Unlock()
	if c.openers.loaded && c.openers.commit == commit {
		return c.openers.userTests
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
	c.openers.loaded, c.openers.commit, c.openers.userTests = true, commit, tests
	c.openers.workloads = make(map[string]appmap.Workload)
	return tests
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

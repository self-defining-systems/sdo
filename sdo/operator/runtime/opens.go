package runtime

import (
	"context"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/appmap"
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

// AlertJoinWindow bounds how long after detection a user test that starts
// failing joins the open incident's alert set. The live side-effect cases had
// a second test failing within seconds to about a minute of onset; a test that
// begins failing later is more likely an independent fault and opens the next
// incident. The set only grows inside the window, so it cannot hold an
// incident forever.
const AlertJoinWindow = 5 * time.Minute

// joinAlertSet adds a user test or failing user traffic that activated while the open incident is
// inside its join window to the incident's alert set instead of the batcher.
// It reports whether the finding is now (or already was) in the set. It never
// dispatches or re-prompts the agent.
func (c *Controller) joinAlertSet(finding sdk.Finding, now time.Time) bool {
	if role := c.openingRole(finding, nil); role != userTestOpener && role != userTrafficOpener {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.incidentOpen || c.currentIncidentRequest == nil {
		return false
	}
	request := c.currentIncidentRequest
	key := FindingStateKey(finding.DetectorID, FindingFingerprint(finding))
	if slices.Contains(alertKeys(request), key) {
		return true
	}
	if now.Sub(c.incidentDetectedAt) > AlertJoinWindow {
		return false
	}
	request.JoinedKeys = append(request.JoinedKeys, key)
	sort.Strings(request.JoinedKeys)
	// The finding rides in the request so Recovered can judge the test's
	// samples (openingUserTest resolves a key through request.Findings).
	finding.Fingerprint = FindingFingerprint(finding)
	request.Findings = append(request.Findings, finding)
	sortFindings(request.Findings)
	c.publishHeldLocked()
	c.closureStableSince = time.Time{}
	c.recoverySafetyLost = true
	// A new alert reopens a settled automatic stage (see attachLearnedActivation).
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

// alertKeys are the incident's alert set: its opening alerts plus the user
// tests that joined while it was open. Recovered waits on all of them; credit
// and symptom matching keep openingKeys.
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

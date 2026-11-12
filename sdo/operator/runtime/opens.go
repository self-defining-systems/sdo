package runtime

import (
	"context"
	"slices"
	"strings"
	"sync"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/sdk"
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
//   - a Kubernetes failure condition that outlived Kubernetes' own
//     self-healing: any finding of the generic condition detectors, which
//     report a rule only after its give-up signal or its self-heal hold.
//
// Every other finding (drift, wiring, events, saturation, links, store state,
// data checks, metrics alerts, authored and learned detectors) is a clue: it
// never opens or holds an incident.
func (c *Controller) opens(finding sdk.Finding) bool {
	if slices.Contains(memory.KubernetesConditionDetectorIDs(), finding.DetectorID) {
		return true
	}
	scenario, ok := strings.CutPrefix(finding.RuleID, traffic.RuleIDPrefix)
	if !ok || !strings.HasPrefix(finding.DetectorID, memory.TrafficDetectorPrefix) {
		return false
	}
	tests := c.userTests()
	return tests == nil || tests[scenario]
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
	return findingKeys(request.Findings)
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

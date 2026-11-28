package runtime

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/sdk"
)

// detectShownNew bounds the findings the "new since the handoff" block shows
// inline; `sdo detect new` lists them all.
const detectShownNew = 3

// newSinceHandoff are the active findings outside the set the agent was
// handed at dispatch (handoff) and outside the alert set (alerts; a joined
// user test has its own lines), sorted by label. Nil handoff (no dispatch
// known, as in the automatic stage) has none.
func newSinceHandoff(findings []sdk.Finding, handoff map[string]bool, alerts []string) []sdk.Finding {
	if handoff == nil {
		return nil
	}
	var fresh []sdk.Finding
	for _, finding := range findings {
		if key := findingKey(finding); finding.Status == sdk.FindingActive && !handoff[key] && !slices.Contains(alerts, key) {
			fresh = append(fresh, finding)
		}
	}
	sort.SliceStable(fresh, func(i, j int) bool { return detectFindingLabel(fresh[i]) < detectFindingLabel(fresh[j]) })
	return fresh
}

// detectNewDriftFields bounds the changed fields shown per new finding.
const detectNewDriftFields = 3

// newBlockTitle heads the block; a response holding it showed the findings.
const newBlockTitle = "NEW SINCE THE HANDOFF"

// newFindingLines renders one new finding: its label and summary and, with
// detail, the newest drift on its object: up to detectNewDriftFields changed
// fields, the writer and when it wrote.
func newFindingLines(finding sdk.Finding, drift []sdk.DriftEvent, detail bool) []string {
	lines := []string{fmt.Sprintf("%s  %s", plainName(detectFindingLabel(finding), 80), redactDetectText(finding.Summary, 140))}
	ref := finding.PrimaryResource
	var newest *sdk.DriftEvent
	for i, event := range drift {
		if ref.Name == "" || event.Ref.Kind != ref.Kind || event.Ref.Name != ref.Name || event.Ref.Namespace != ref.Namespace {
			continue
		}
		if newest == nil || driftAt(event).After(driftAt(*newest)) {
			newest = &drift[i]
		}
	}
	if !detail || newest == nil {
		return lines
	}
	var fields []string
	for _, field := range newest.Fields[:min(len(newest.Fields), detectNewDriftFields)] {
		change := plainName(field.Path, 60)
		if field.Old != "" || field.New != "" {
			change += " " + redactDetectText(field.Old, 30) + " -> " + redactDetectText(field.New, 30)
		}
		fields = append(fields, change)
	}
	if more := len(newest.Fields) - detectNewDriftFields; more > 0 {
		fields = append(fields, fmt.Sprintf("+%d more fields", more))
	}
	writer := newest.Manager
	if writer == "" {
		writer = "unknown"
	}
	return append(lines, fmt.Sprintf("  changed %s by %s at %s", orQuestion(strings.Join(fields, "; ")), plainName(writer, 40), hms(driftAt(*newest))))
}

func driftAt(event sdk.DriftEvent) time.Time {
	if event.ManagerAt != nil {
		return *event.ManagerAt
	}
	return event.ObservedAt
}

// newSinceHandoffBlock is the block `sdo detect` leads with: the first
// `shown` findings inline (with their drift when detail is set); item is the
// full list for `sdo detect new`, bounded like every item.
func newSinceHandoffBlock(fresh []sdk.Finding, drift []sdk.DriftEvent, shown int, detail bool) (block []string, item string) {
	if len(fresh) == 0 {
		return nil, ""
	}
	var all []string
	block = []string{"", newBlockTitle + " (not in the alerts and clues you were given)"}
	for i, finding := range fresh {
		all = append(all, newFindingLines(finding, drift, true)...)
		if i < shown {
			block = append(block, newFindingLines(finding, drift, detail)...)
		}
	}
	if dropped := len(fresh) - shown; dropped > 0 || !detail {
		block = append(block, fmt.Sprintf("+%d more new, with their changes: sdo detect new", dropped))
	}
	item = strings.Join(all, "\n") + "\n"
	if runes := []rune(item); len(runes) > detectItemCap {
		item = string(runes[:detectItemCap]) + "\n+more: sdo detect --json\n"
	}
	return block, item
}

// showsNewFindings reports whether a detect response showed the new block
// (the summary, a wait that named them, or `sdo detect new`), so a named test
// or item read does not mark them seen.
func showsNewFindings(request ToolRequest, text string) bool {
	return strings.Contains(text, newBlockTitle) || strings.Contains(text, "new since the handoff: ") || request.Name == "new" || request.Finding == "new"
}

// newKeys are the finding keys of fresh, sorted.
func newKeys(fresh []sdk.Finding) []string {
	keys := make([]string, 0, len(fresh))
	for _, finding := range fresh {
		keys = append(keys, findingKey(finding))
	}
	sort.Strings(keys)
	return slices.Compact(keys)
}

// withoutFindings is findings less those in drop, so a finding the new block
// lists is not listed again as a clue.
func withoutFindings(findings, drop []sdk.Finding) []sdk.Finding {
	if len(drop) == 0 {
		return findings
	}
	keys := map[string]bool{}
	for _, finding := range drop {
		keys[findingKey(finding)] = true
	}
	kept := make([]sdk.Finding, 0, len(findings))
	for _, finding := range findings {
		if !keys[findingKey(finding)] {
			kept = append(kept, finding)
		}
	}
	return kept
}

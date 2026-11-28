package runtime

import (
	"fmt"
	"slices"
	"sort"
	"strings"

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

// newFindingLines renders one new finding: its label and summary, and for
// drift on its object the changed fields, the writer and when it wrote.
func newFindingLines(finding sdk.Finding, drift []sdk.DriftEvent) []string {
	lines := []string{fmt.Sprintf("%s  %s", plainName(detectFindingLabel(finding), 80), redactDetectText(finding.Summary, 140))}
	ref := finding.PrimaryResource
	for _, event := range drift {
		if ref.Name == "" || event.Ref.Kind != ref.Kind || event.Ref.Name != ref.Name || event.Ref.Namespace != ref.Namespace {
			continue
		}
		var fields []string
		for _, field := range event.Fields {
			change := plainName(field.Path, 60)
			if field.Old != "" || field.New != "" {
				change += " " + redactDetectText(field.Old, 40) + " -> " + redactDetectText(field.New, 40)
			}
			fields = append(fields, change)
		}
		at := event.ObservedAt
		if event.ManagerAt != nil {
			at = *event.ManagerAt
		}
		writer := event.Manager
		if writer == "" {
			writer = "unknown"
		}
		lines = append(lines, fmt.Sprintf("  changed %s by %s at %s", orQuestion(strings.Join(fields, "; ")), plainName(writer, 40), hms(at)))
	}
	return lines
}

// newSinceHandoffBlock is the block `sdo detect` leads with, the first
// `shown` findings inline, and the full list for `sdo detect new`.
func newSinceHandoffBlock(fresh []sdk.Finding, drift []sdk.DriftEvent, shown int) (block []string, item string) {
	if len(fresh) == 0 {
		return nil, ""
	}
	var all []string
	block = []string{"", "NEW SINCE THE HANDOFF (not in the alerts and clues you were given)"}
	for i, finding := range fresh {
		lines := newFindingLines(finding, drift)
		all = append(all, lines...)
		if i < shown {
			block = append(block, lines...)
		}
	}
	if dropped := len(fresh) - shown; dropped > 0 {
		block = append(block, fmt.Sprintf("+%d more new: sdo detect new", dropped))
	}
	return block, strings.Join(all, "\n") + "\n"
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

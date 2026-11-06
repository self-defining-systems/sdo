package offline

import (
	"fmt"
	"sort"
	"strings"

	"sdo.dev/operator/operability"
)

// Finding is one memory-validation diagnostic as the report sees it: the
// validator's rule ID, path, line and message. Callers convert
// validate.Diagnostic into it, so this package imports no validator.
type Finding struct {
	Rule    string
	Path    string
	Line    int
	Message string
}

// Memory is the result of one run of the shared memory validator.
type Memory struct {
	// Blocking are the diagnostics the validator returned: they reject.
	Blocking []Finding
	// Notes are findings the validator reports without rejecting (coverage gaps).
	Notes []Finding
	// OwnershipChecked says the ownership layer ran (a base, actor and phase
	// were known). When false O6.ownership is skipped, never passed.
	OwnershipChecked bool
}

// Rule families, by the validator's stable rule-ID prefixes. Every rule not
// named here is operational-memory consistency (O5).
const (
	familyCoverage  = "map"
	familyOwnership = "ownership"
	familyLifecycle = "lifecycle"
)

// ItemOf names the report item a validator rule belongs to: application-map
// coverage (map.*) is O3, ownership and lifecycle-phase rules are O6, and
// everything else is O5.
func ItemOf(rule string) operability.ItemID {
	switch family(rule) {
	case familyCoverage:
		return operability.O3
	case familyOwnership, familyLifecycle:
		return operability.O6
	}
	return operability.O5
}

func family(rule string) string {
	f, _, _ := strings.Cut(rule, ".")
	return f
}

// CheckMemory classifies one validator run into checks of O3 (coverage), O5
// (memory consistency) and O6 (ownership of the diff). Blocking is exactly what
// the validator rejected: blocking versus note is never decided here.
func CheckMemory(m Memory) []operability.Check {
	var coverage, ownership []Finding
	byFamily := map[string][]Finding{}
	for _, f := range m.Blocking {
		switch ItemOf(f.Rule) {
		case operability.O3:
			coverage = append(coverage, f)
		case operability.O6:
			ownership = append(ownership, f)
		default:
			byFamily[family(f.Rule)] = append(byFamily[family(f.Rule)], f)
		}
	}
	var out []operability.Check

	if len(coverage) == 0 {
		out = append(out, pass("O3.coverage", "the application map covers the declared operations"))
	} else {
		out = append(out, findingsCheck("O3.coverage", operability.CauseCoverage, coverage))
	}
	var coverageNotes, memoryNotes []Finding
	for _, f := range m.Notes {
		if ItemOf(f.Rule) == operability.O3 {
			coverageNotes = append(coverageNotes, f)
		} else {
			memoryNotes = append(memoryNotes, f)
		}
	}
	if len(coverageNotes) > 0 {
		out = append(out, noteCheck("O3.coverage-gaps", coverageNotes))
	}

	families := make([]string, 0, len(byFamily))
	for f := range byFamily {
		families = append(families, f)
	}
	sort.Strings(families)
	for _, f := range families {
		out = append(out, findingsCheck("O5.memory."+f, operability.CauseMemoryInvalid, byFamily[f]))
	}
	if len(families) == 0 {
		out = append(out, pass("O5.memory", "operational memory passes the shared check"))
	}
	if len(memoryNotes) > 0 {
		out = append(out, noteCheck("O5.gaps", memoryNotes))
	}

	switch {
	case !m.OwnershipChecked:
		out = append(out, operability.Check{ID: "O6.ownership", Phase: operability.PhaseOffline, Status: operability.StatusSkipped, Detail: "no base, actor and phase were given, so the diff was not checked against ownership"})
	case len(ownership) == 0:
		out = append(out, pass("O6.ownership", "the diff touches only paths its actor may change"))
	default:
		out = append(out, findingsCheck("O6.ownership", operability.CauseScope, ownership))
	}
	return out
}

const (
	maxFindingsListed = 3
	maxFindingPaths   = 10
)

func findingsCheck(id string, cause operability.Cause, fs []Finding) operability.Check {
	return operability.Check{
		ID: id, Phase: operability.PhaseOffline, Status: operability.StatusFail, Cause: cause,
		Detail: operability.Clip(summary(fs)), Paths: paths(fs),
	}
}

func noteCheck(id string, fs []Finding) operability.Check {
	return operability.Check{ID: id, Phase: operability.PhaseOffline, Status: operability.StatusNote, Detail: operability.Clip(summary(fs)), Paths: paths(fs)}
}

func summary(fs []Finding) string {
	var parts []string
	for i, f := range fs {
		if i == maxFindingsListed {
			parts = append(parts, fmt.Sprintf("and %d more", len(fs)-maxFindingsListed))
			break
		}
		where := f.Path
		if where != "" && f.Line > 0 {
			where = fmt.Sprintf("%s:%d", where, f.Line)
		}
		part := "[" + f.Rule + "] " + f.Message
		if where != "" {
			part = where + " " + part
		}
		parts = append(parts, part)
	}
	return strings.Join(parts, "; ")
}

func paths(fs []Finding) []string {
	seen := map[string]bool{}
	var out []string
	for _, f := range fs {
		if f.Path != "" && !seen[f.Path] {
			seen[f.Path] = true
			out = append(out, f.Path)
		}
	}
	sort.Strings(out)
	if len(out) > maxFindingPaths {
		out = out[:maxFindingPaths]
	}
	return out
}

// Items returns the items an offline report can satisfy at the stage: those of
// StageItems except O4, which needs a cluster.
func Items(stage operability.Stage) []operability.ItemID {
	var out []operability.ItemID
	for _, id := range operability.StageItems(stage) {
		if id != operability.O4 {
			out = append(out, id)
		}
	}
	return out
}

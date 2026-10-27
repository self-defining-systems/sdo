package replay

import (
	"context"
	"fmt"
	"strings"
	"time"

	"sdo.dev/operator/core"
	operator "sdo.dev/operator/runtime"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// Rules named by CheckRepeat. Only RuleRepeatUnreachable is information: the
// detectors never selected the playbook on its recorded incident, which is a
// gap in coverage. The others are defects of the program or its detectors.
const (
	RuleRepeatUnreachable    = "program.repeat-unreachable"
	RuleRepeatUnavailable    = "program.repeat-unavailable"
	RuleDiagnoseNoMatch      = "program.diagnose-no-match"
	RuleMitigatorMissing     = "program.mitigator-missing"
	RuleDetectorInvalid      = "detector.invalid-finding"
	RuleDetectorFiresHealthy = "detector.fires-on-healthy"
)

var repeatDefectRules = []string{RuleRepeatUnavailable, RuleDiagnoseNoMatch, RuleMitigatorMissing, RuleDetectorInvalid, RuleDetectorFiresHealthy}

// RepeatDefectRule names the blocking rule of the first CheckRepeat error that
// is a program or detector defect, or "" when there is none.
func RepeatDefectRule(errors []string) string {
	for _, message := range errors {
		for _, rule := range repeatDefectRules {
			if strings.HasPrefix(message, rule+":") {
				return rule
			}
		}
	}
	return ""
}

// CheckRepeat proves the recorded repeat path through real incident detectors,
// production candidate selection, diagnosis and the production repair replay.
// Case-file bindings never select the object roles used by this check. When no
// finding selects the playbook the mitigation is still replayed with the case
// bindings and must repair; the unreachable link is then a Note, not an error.
func CheckRepeat(ctx context.Context, registry program.ProgramRegistry, detectors []sdk.Detector, id string, c Case) Result {
	result := Result{ProgramID: id, CaseID: c.ID, Status: "failed"}
	fail := func(rule, link string) Result {
		result.Errors = append(result.Errors, fmt.Sprintf("%s: playbook %q on its recorded incident %q: %s", rule, id, c.ID, link))
		return result
	}
	p, ok := registry.Diagnoser(id)
	if !ok {
		return fail(RuleRepeatUnavailable, "missing diagnoser")
	}
	unreachable := func(link string) Result {
		note := fmt.Sprintf("%s: playbook %q not reachable on its recorded incident %q: %s", RuleRepeatUnreachable, id, c.ID, link)
		m, ok := registry.Mitigator(id)
		if !ok {
			return fail(RuleMitigatorMissing, "diagnosis has no mitigation program")
		}
		repaired := CheckRepair(ctx, p, m, c)
		if repaired.Status == "replayed" {
			repaired.Notes = append(repaired.Notes, note)
		} else if len(repaired.Errors) == 0 {
			repaired.Errors = []string{"mitigation did not clear diagnosis"}
		}
		return repaired
	}
	source, err := newSource(c)
	if err != nil {
		return fail(RuleRepeatUnavailable, "recorded source: "+err.Error())
	}
	var findings []sdk.Finding
	for _, d := range detectors {
		if d.Spec().Class != sdk.DetectorClassIncident {
			continue
		}
		healthy := newReadView(ctx, c.Corpus.Healthy, c.Corpus.PhaseRecordings("healthy"), source)
		observed, err := sdk.DetectObserved(ctx, d, healthy)
		if err != nil {
			return fail(RuleRepeatUnavailable, fmt.Sprintf("healthy detector %q unavailable: %v", d.Spec().ID, err))
		}
		for _, f := range observed {
			if f.DetectorID == "" {
				f.DetectorID = d.Spec().ID
			}
			if len(f.Playbooks) == 0 {
				f.Playbooks = append([]string(nil), d.Spec().Playbooks...)
			}
			f.Fingerprint = operator.FindingFingerprint(f)
			if err := core.ValidateFinding(d.Spec(), f); err != nil {
				return fail(RuleDetectorInvalid, fmt.Sprintf("detector %q emitted an invalid finding: %v", d.Spec().ID, err))
			}
			if f.Status == sdk.FindingActive {
				return fail(RuleDetectorFiresHealthy, fmt.Sprintf("incident detector %q fires on the recorded healthy state", d.Spec().ID))
			}
		}
		faulty := newReadView(ctx, c.Corpus.Open, c.Corpus.PhaseRecordings("open"), source)
		observed, err = sdk.DetectObserved(ctx, d, faulty)
		if err != nil {
			return fail(RuleRepeatUnavailable, fmt.Sprintf("faulty detector %q unavailable: %v", d.Spec().ID, err))
		}
		for _, f := range observed {
			if f.DetectorID == "" {
				f.DetectorID = d.Spec().ID
			}
			if len(f.Playbooks) == 0 {
				f.Playbooks = append([]string(nil), d.Spec().Playbooks...)
			}
			f.Fingerprint = operator.FindingFingerprint(f)
			if err := core.ValidateFinding(d.Spec(), f); err != nil {
				return fail(RuleDetectorInvalid, fmt.Sprintf("detector %q emitted an invalid finding: %v", d.Spec().ID, err))
			}
			if f.Status == sdk.FindingActive {
				findings = append(findings, f)
			}
		}
	}
	if len(findings) == 0 {
		return unreachable("no active incident detector finding")
	}
	view := newReadView(ctx, c.Corpus.Open, c.Corpus.PhaseRecordings("open"), source)
	// This is the same entry point used by incident coverage and responder tools,
	// including explicit playbook hints and diagnosis failure containment.
	runs := operator.SelectProgramCandidates(ctx, registry, view, findings, 5*time.Second)
	var noMatch []string
	for _, run := range runs {
		if run.ProgramID != id {
			continue
		}
		if run.Verdict.Result != program.Match {
			noMatch = append(noMatch, fmt.Sprintf("Diagnose returned %s: %v", run.Verdict.Result, run.Verdict.Evidence))
			continue
		}
		m, ok := registry.Mitigator(id)
		if !ok {
			return fail(RuleMitigatorMissing, "matching diagnosis has no mitigation program")
		}
		bound := c
		bound.Corpus.Bindings = run.Bindings.Clone()
		bound.Corpus.PeerBindings = run.Bindings.Clone()
		repaired := CheckRepair(ctx, p, m, bound)
		if repaired.Status == "replayed" {
			return repaired
		}
		// Once reached, repair failures retain the existing mitigation diagnostic.
		result = repaired
		if len(result.Errors) == 0 {
			result.Errors = []string{"mitigation did not clear diagnosis"}
		}
		return result
	}
	var reasons []string
	bound := false
	for _, f := range findings {
		if _, err := program.BindFinding(p.Spec(), f); err != nil {
			reasons = append(reasons, err.Error())
		} else {
			bound = true
		}
	}
	if bound && len(noMatch) > 0 {
		// A finding selected the playbook and its own Diagnose refused the fixture.
		return fail(RuleDiagnoseNoMatch, strings.Join(noMatch, "; "))
	}
	reasons = append(reasons, noMatch...)
	if len(reasons) == 0 {
		reasons = append(reasons, "no detector finding selects this playbook")
	}
	return unreachable(strings.Join(reasons, "; "))
}

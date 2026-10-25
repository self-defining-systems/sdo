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

// CheckRepeat proves the recorded repeat path through real incident detectors,
// production candidate selection, diagnosis and the production repair replay.
// Case-file bindings never select the object roles used by this check.
func CheckRepeat(ctx context.Context, registry program.ProgramRegistry, detectors []sdk.Detector, id string, c Case) Result {
	result := Result{ProgramID: id, CaseID: c.ID, Status: "failed"}
	fail := func(link string) Result {
		result.Errors = append(result.Errors, fmt.Sprintf("program.repeat-unreachable: playbook %q not reachable on its recorded incident %q: %s", id, c.ID, link))
		return result
	}
	p, ok := registry.Diagnoser(id)
	if !ok {
		return fail("missing diagnoser")
	}
	source, err := newSource(c)
	if err != nil {
		return fail("recorded source: " + err.Error())
	}
	var findings []sdk.Finding
	for _, d := range detectors {
		if d.Spec().Class != sdk.DetectorClassIncident {
			continue
		}
		healthy := newReadView(ctx, c.Corpus.Healthy, c.Corpus.PhaseRecordings("healthy"), source)
		observed, err := sdk.DetectObserved(ctx, d, healthy)
		if err != nil {
			return fail(fmt.Sprintf("healthy detector %q unavailable: %v", d.Spec().ID, err))
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
				return fail(fmt.Sprintf("detector %q emitted an invalid finding: %v", d.Spec().ID, err))
			}
			if f.Status == sdk.FindingActive {
				return fail(fmt.Sprintf("healthy baseline fires incident detector %q", d.Spec().ID))
			}
		}
		faulty := newReadView(ctx, c.Corpus.Open, c.Corpus.PhaseRecordings("open"), source)
		observed, err = sdk.DetectObserved(ctx, d, faulty)
		if err != nil {
			return fail(fmt.Sprintf("faulty detector %q unavailable: %v", d.Spec().ID, err))
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
				return fail(fmt.Sprintf("detector %q emitted an invalid finding: %v", d.Spec().ID, err))
			}
			if f.Status == sdk.FindingActive {
				findings = append(findings, f)
			}
		}
	}
	if len(findings) == 0 {
		return fail("no active incident detector finding; add detector coverage for this fault")
	}
	view := newReadView(ctx, c.Corpus.Open, c.Corpus.PhaseRecordings("open"), source)
	// This is the same entry point used by incident coverage and responder tools,
	// including explicit playbook hints and diagnosis failure containment.
	runs := operator.SelectProgramCandidates(ctx, registry, view, findings, 5*time.Second)
	var reasons []string
	for _, run := range runs {
		if run.ProgramID != id {
			continue
		}
		if run.Verdict.Result != program.Match {
			reasons = append(reasons, fmt.Sprintf("Diagnose returned %s: %v", run.Verdict.Result, run.Verdict.Evidence))
			continue
		}
		m, ok := registry.Mitigator(id)
		if !ok {
			return fail("matching diagnosis has no mitigation program")
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
	for _, f := range findings {
		if _, err := program.BindFinding(p.Spec(), f); err != nil {
			reasons = append(reasons, err.Error())
		}
	}
	if len(reasons) == 0 {
		reasons = append(reasons, "no detector finding selects this playbook")
	}
	return fail(strings.Join(reasons, "; "))
}

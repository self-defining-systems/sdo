package validate

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk/sdktest"
)

// validateNewProgramCorpus requires a captured matching case when a new
// diagnoser enters operational memory. Existing programs keep their cases.
func validateNewProgramCorpus(candidate, baseline string) error {
	programRoot := filepath.Join(candidate, layout.PlaybooksDir)
	programs, err := os.ReadDir(programRoot)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	caseRoot := filepath.Join(candidate, layout.CorpusDir)
	cases, err := os.ReadDir(caseRoot)
	if os.IsNotExist(err) {
		cases = nil
	} else if err != nil {
		return err
	}
	for _, item := range programs {
		if !item.IsDir() {
			continue
		}
		newSource := filepath.Join(programRoot, item.Name(), "diagnose.go")
		if _, err := os.Stat(newSource); os.IsNotExist(err) {
			continue
		} else if err != nil {
			return err
		}
		if _, err := os.Stat(filepath.Join(baseline, layout.PlaybookDir(item.Name()), "diagnose.go")); err == nil {
			continue
		} else if !os.IsNotExist(err) {
			return err
		}
		captured := false
		for _, entry := range cases {
			if !entry.IsDir() {
				continue
			}
			dir := filepath.Join(caseRoot, entry.Name())
			caseData, err := sdktest.LoadCorpusCase(dir)
			if err != nil {
				return err
			}
			for _, id := range caseData.RootCauseProgramIDs {
				if id != item.Name() {
					continue
				}
				if err := sdktest.ValidateCorpusProvenance(dir); err != nil {
					return fmt.Errorf("new program %s corpus %s: %w", item.Name(), entry.Name(), err)
				}
				if caseData.Provenance.IncidentID != "" {
					captured = true
				}
			}
		}
		if !captured {
			return fmt.Errorf("new program %s requires a captured incident corpus case", item.Name())
		}
	}
	return nil
}

// corpusRules protects accepted evidence before examining new cases. A changed
// accepted case fails closed, even if its new labels claim to be synthetic or
// identify the current incident. New evidence must use a new case ID.
func (s *run) corpusRules(baseline string) error {
	cases := map[string]bool{}
	if s.req.Base == "" {
		entries, err := os.ReadDir(filepath.Join(s.req.Root, layout.CorpusDir))
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		for _, entry := range entries {
			if entry.IsDir() {
				cases[entry.Name()] = true
			}
		}
	} else {
		for _, changed := range s.changed {
			if rel, ok := strings.CutPrefix(changed, layout.CorpusDir+"/"); ok {
				id, _, _ := strings.Cut(rel, "/")
				cases[id] = true
			}
		}
	}
	ids := make([]string, 0, len(cases))
	for id := range cases {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var capturedPaths []string
	for _, id := range ids {
		rel := layout.CorpusDir + "/" + id
		if baseline != "" {
			if _, err := os.Stat(filepath.Join(baseline, rel)); err == nil {
				s.add(Semantic, Diagnostic{Rule: RuleCorpusImmutable, Path: rel, Message: "accepted corpus case cannot be deleted or rewritten", Hint: "preserve its snapshots, recordings, provenance and labels; add new evidence under a new case ID"})
				continue
			} else if !os.IsNotExist(err) {
				return err
			}
		}
		dir := filepath.Join(s.req.Root, rel)
		if err := sdktest.ValidateCorpusProvenance(dir); err != nil {
			s.add(Semantic, Diagnostic{Rule: RuleCorpusProvenance, Path: rel, Message: err.Error()})
		}
		caseData, err := sdktest.LoadCorpusCase(dir)
		if err != nil {
			// The provenance diagnostic already explains the malformed case.
			continue
		}
		if caseData.Provenance.IncidentID == "" && caseData.SyntheticNoMatchReason != "" && len(caseData.RootCauseProgramIDs) == 0 && len(caseData.NoMatchProgramIDs) > 0 {
			continue
		}
		capturedPaths = append(capturedPaths, rel+"/labels.json")
	}
	if len(capturedPaths) == 0 {
		return nil
	}
	capture := s.req.CaptureDirectory
	if capture == "" && s.req.CaptureRoot != "" && strings.TrimSpace(s.req.IncidentID) != "" {
		var err error
		capture, err = memory.IncidentCorpusDir(s.req.CaptureRoot, s.req.IncidentID)
		if err != nil {
			return err
		}
	}
	if capture == "" || strings.TrimSpace(s.req.IncidentID) == "" {
		s.add(Semantic, Diagnostic{Rule: RuleCaptureUnavailable, Path: layout.CorpusDir, Message: "proposed corpus requires a provisioned capture directory and incident ID"})
		return nil
	}
	if info, err := os.Stat(capture); err != nil || !info.IsDir() {
		s.add(Semantic, Diagnostic{Rule: RuleCaptureUnavailable, Path: layout.CorpusDir, Message: "provisioned corpus capture directory is unavailable"})
		return nil
	}
	if err := memory.ValidateCapturedCorpusDirectory(capture, s.req.Root, s.req.IncidentID, capturedPaths); err != nil {
		s.add(Semantic, Diagnostic{Rule: RuleCaptureProvenance, Path: layout.CorpusDir, Message: err.Error()})
	}
	return nil
}

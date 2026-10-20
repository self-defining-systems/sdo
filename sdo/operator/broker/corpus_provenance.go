package broker

import (
	"fmt"
	"os"
	"path/filepath"

	"sdo.dev/operator/sdk/sdktest"
)

// ValidateNewProgramCorpus requires a captured matching case when a new
// diagnoser enters operational memory. Existing programs keep their cases.
func ValidateNewProgramCorpus(candidate, baseline string) error {
	programRoot := filepath.Join(candidate, ".sdo", "playbooks")
	programs, err := os.ReadDir(programRoot)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	caseRoot := filepath.Join(candidate, ".sdo", "corpus")
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
		if _, err := os.Stat(filepath.Join(baseline, ".sdo", "playbooks", item.Name(), "diagnose.go")); err == nil {
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

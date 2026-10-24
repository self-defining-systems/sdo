package validate

import (
	"os"
	"path/filepath"
	"sort"
	"strings"

	"sdo.dev/operator/memory/layout"
)

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
	}
	return nil
}

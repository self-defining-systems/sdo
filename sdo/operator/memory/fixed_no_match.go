package memory

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"

	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// RecordFixedNoMatchCorpus seeds exact observed negative evidence. It is not a
// positive case and earns no trust. Re-materializing after model edits preserves
// this regression until the ordinary replay gate accepts a corrected program.
func RecordFixedNoMatchCorpus(root, captures, incident string, instances map[string][]program.Bindings) ([]string, error) {
	fixed, err := os.ReadFile(filepath.Join(captures, "fixed.json"))
	if err != nil {
		return nil, err
	}
	recordings, err := os.ReadFile(filepath.Join(captures, "open@detect-recordings.json"))
	if err != nil {
		return nil, err
	}
	paths := []string{}
	for _, id := range slices.Sorted(maps.Keys(instances)) {
		for _, bindings := range instances[id] {
			raw, _ := json.Marshal(bindings)
			digest := sha256.Sum256(append([]byte(incident+"\x00"+id+"\x00"), raw...))
			relative := filepath.Join(layout.CorpusDir, fmt.Sprintf("fixed-no-match-%x", digest[:12]))
			label := struct {
				IDs        []string                 `json:"no_match_program_ids"`
				Bindings   program.Bindings         `json:"bindings"`
				Provenance sdktest.CorpusProvenance `json:"provenance"`
			}{[]string{id}, bindings, sdktest.CorpusProvenance{IncidentID: incident, OpenAt: sdktest.CapturedFixedNoMatchPhase, OpenSHA256: fmt.Sprintf("%x", sha256.Sum256(fixed)), FixedSHA256: fmt.Sprintf("%x", sha256.Sum256(fixed))}}
			raw, err = json.MarshalIndent(label, "", "  ")
			if err != nil {
				return nil, err
			}
			directory := filepath.Join(root, relative)
			if err := os.MkdirAll(directory, 0755); err != nil {
				return nil, err
			}
			files := map[string][]byte{"open.json": fixed, "fixed.json": fixed, "healthy.json": fixed, "recordings.json": recordings, "labels.json": raw}
			for _, name := range slices.Sorted(maps.Keys(files)) {
				if err := os.WriteFile(filepath.Join(directory, name), files[name], 0644); err != nil {
					return nil, err
				}
				paths = append(paths, filepath.ToSlash(filepath.Join(relative, name)))
			}
		}
	}
	return paths, nil
}

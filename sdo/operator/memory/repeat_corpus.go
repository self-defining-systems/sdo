package memory

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"

	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// RecordRepeatCorpus materializes evidence only. The caller must use the normal
// reflection validator and broker before accepting it; this grants no credit.
// Healthy peer associations come from accepted cases, never a new model claim.
func RecordRepeatCorpus(root, captures, incidentID string, instances map[string][]program.Bindings, origins ...map[string]string) (paths []string, err error) {
	fixed, err := os.ReadFile(filepath.Join(captures, "fixed.json"))
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(filepath.Join(root, layout.CorpusDir))
	if err != nil {
		return nil, err
	}
	type labels struct {
		Programs   []string                 `json:"root_cause_program_ids"`
		Bindings   program.Bindings         `json:"bindings"`
		Peers      program.Bindings         `json:"peer_bindings"`
		Provenance sdktest.CorpusProvenance `json:"provenance"`
	}
	prior := []labels{}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		data, readErr := os.ReadFile(filepath.Join(root, layout.CorpusDir, entry.Name(), "labels.json"))
		if readErr != nil {
			continue
		}
		var label labels
		if json.Unmarshal(data, &label) == nil {
			prior = append(prior, label)
		}
	}
	cases := map[string]map[string][]byte{}
	for _, id := range slices.Sorted(maps.Keys(instances)) {
		for _, binding := range instances[id] {
			phase := capturedInstancePhase(id, binding, origins)
			opened, err := os.ReadFile(filepath.Join(captures, "open@"+phase+".json"))
			if err != nil {
				return nil, err
			}
			recordings, err := os.ReadFile(filepath.Join(captures, "open@"+phase+"-recordings.json"))
			if err != nil {
				return nil, err
			}
			var peers program.Bindings
			for _, previous := range prior {
				if !slices.Contains(previous.Programs, id) {
					continue
				}
				for _, candidate := range []program.Bindings{previous.Peers, previous.Bindings} {
					if len(candidate.Objects) != len(binding.Objects) || candidate.Equal(binding) {
						continue
					}
					valid := true
					for name, ref := range binding.Objects {
						peer, ok := candidate.Objects[name]
						valid = valid && ok && peer.Kind == ref.Kind
					}
					if valid {
						peers = candidate
						break
					}
				}
				if peers.Objects != nil {
					break
				}
			}
			if peers.Objects == nil {
				return nil, fmt.Errorf("no captured accepted healthy peer for program %s", id)
			}
			encoded, _ := json.Marshal(binding)
			digest := sha256.Sum256(append([]byte(incidentID+"\x00"+id+"\x00"), encoded...))
			name := fmt.Sprintf("repeat-%x", digest[:12])
			label := labels{Programs: []string{id}, Bindings: binding, Peers: peers, Provenance: sdktest.CorpusProvenance{
				IncidentID: incidentID, OpenAt: phase, OpenSHA256: fmt.Sprintf("%x", sha256.Sum256(opened)), FixedSHA256: fmt.Sprintf("%x", sha256.Sum256(fixed)),
			}}
			encoded, err = json.MarshalIndent(label, "", "  ")
			if err != nil {
				return nil, err
			}
			cases[name] = map[string][]byte{"open.json": opened, "fixed.json": fixed, "healthy.json": fixed, "recordings.json": recordings, "labels.json": encoded}
		}
	}
	created := []string{}
	defer func() {
		if err != nil {
			for _, directory := range created {
				_ = os.RemoveAll(directory)
			}
		}
	}()
	for _, name := range slices.Sorted(maps.Keys(cases)) {
		relative := filepath.Join(layout.CorpusDir, name)
		directory := filepath.Join(root, relative)
		if err = os.Mkdir(directory, 0755); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, err
		}
		exists := errors.Is(err, os.ErrExist)
		if !exists {
			created = append(created, directory)
		}
		for _, file := range slices.Sorted(maps.Keys(cases[name])) {
			if err = os.WriteFile(filepath.Join(directory, file), cases[name][file], 0644); err != nil {
				return nil, err
			}
			paths = append(paths, filepath.ToSlash(filepath.Join(relative, file)))
		}
	}
	return paths, nil
}

// CapturedInstanceKey ties recording origin to one program and exact bindings.
func CapturedInstanceKey(id string, bindings program.Bindings) string {
	data, _ := json.Marshal(bindings)
	return id + "\x00" + string(data)
}
func capturedInstancePhase(id string, bindings program.Bindings, origins []map[string]string) string {
	if len(origins) > 0 {
		if phase := origins[0][CapturedInstanceKey(id, bindings)]; phase == "pre-apply" {
			return phase
		}
	}
	return "detect"
}

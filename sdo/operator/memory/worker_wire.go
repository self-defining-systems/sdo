package memory

import (
	"encoding/json"
	"fmt"
)

// NormalizeWorkerWire projects current storage references into logical IDs.
// This translation belongs to the memory edge, not the worker transport.
func NormalizeWorkerWire(payload []byte) ([]byte, error) {
	var document any
	if err := json.Unmarshal(payload, &document); err != nil {
		return nil, err
	}
	if err := mapPlaybookIDs(document); err != nil {
		return nil, err
	}
	return json.Marshal(document)
}

func mapPlaybookIDs(value any) error {
	switch current := value.(type) {
	case []any:
		for _, item := range current {
			if err := mapPlaybookIDs(item); err != nil {
				return err
			}
		}
	case map[string]any:
		if raw, found := current["proposed_memory_changes"]; found {
			items, ok := raw.([]any)
			if raw == nil {
				items, ok = []any{}, true
			}
			if !ok {
				return fmt.Errorf("proposed memory changes must be strings")
			}
			ids := make([]any, 0, len(items))
			for _, item := range items {
				reference, ok := item.(string)
				if !ok {
					return fmt.Errorf("proposed memory change is not a string")
				}
				if reference == ".sdo/outcomes.jsonl" {
					continue
				} // Outcome storage is broker-owned.
				id, err := ArtifactID(reference)
				if err != nil {
					return err
				}
				ids = append(ids, id)
			}
			delete(current, "proposed_memory_changes")
			current["proposed_artifact_ids"] = ids
		}
		fields := map[string]string{"playbooks": "playbook_ids"}
		if _, prior := current["match_reason"]; prior {
			fields["applied_playbooks"] = "applied_playbook_ids"
		}
		if _, timeline := current["activations"]; timeline {
			fields["surfaced_playbooks"] = "surfaced_playbook_ids"
		}
		for source, target := range fields {
			items, found := current[source].([]any)
			if !found {
				continue
			}
			ids := make([]any, 0, len(items))
			for _, item := range items {
				reference, ok := item.(string)
				if !ok {
					return fmt.Errorf("%s contains a non-string playbook reference", source)
				}
				id, err := PlaybookID(reference)
				if err != nil {
					return err
				}
				ids = append(ids, id)
			}
			delete(current, source)
			current[target] = ids
		}
		if reference, found := current["path"].(string); found {
			id, err := PlaybookID(reference)
			if err != nil {
				return err
			}
			delete(current, "path")
			current["playbook_id"] = id
		}
		if reference, found := current["playbook_id"].(string); found {
			id, err := PlaybookID(reference)
			if err != nil {
				return err
			}
			current["playbook_id"] = id
		}
		if raw, found := current["scripts"]; found {
			if playbookID, ok := current["playbook_id"].(string); ok {
				items, ok := raw.([]any)
				if raw == nil {
					items, ok = []any{}, true
				}
				if !ok {
					return fmt.Errorf("playbook scripts must be strings")
				}
				names := make([]any, 0, len(items))
				for _, item := range items {
					reference, ok := item.(string)
					if !ok {
						return fmt.Errorf("playbook script is not a string")
					}
					name, err := PlaybookScriptName(playbookID, reference)
					if err != nil {
						return err
					}
					names = append(names, name)
				}
				delete(current, "scripts")
				current["script_names"] = names
			}
		}
		for _, child := range current {
			if err := mapPlaybookIDs(child); err != nil {
				return err
			}
		}
	}
	return nil
}

package memory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"sdo.dev/operator/apihistory"
)

// typedSections are the capture's typed lists by kind, kept in step with the objects a snapshot stores in "objects".
var typedSections = map[string]string{
	"ConfigMap": "configMaps", "Service": "services", "Pod": "pods", "Deployment": "deployments", "StatefulSet": "statefulSets",
	"ReplicaSet": "replicaSets", "HorizontalPodAutoscaler": "horizontalPodAutoscalers", "PodDisruptionBudget": "podDisruptionBudgets",
	"Endpoints": "endpoints", "EndpointSlice": "endpointSlices", "NetworkPolicy": "networkPolicies",
}

// CapturePreApplyFromWrites writes open@pre-apply for an incident whose first change was a raw write rather than a plan
// application. The recording proxy keeps each written object's state just before the write (phase "before"); this
// takes the detection snapshot and puts every such object back as it stood before the agent's first write to it. It
// does nothing when open@pre-apply exists (a plan application captured it) or no write has been recorded, and never
// replaces an existing capture.
func CapturePreApplyFromWrites(repositoryMount, incidentID string, through time.Time) error {
	dir, err := IncidentCorpusDir(repositoryMount, incidentID)
	if err != nil {
		return err
	}
	target := filepath.Join(dir, "open@pre-apply.json")
	if _, err := os.Stat(target); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	relative, err := PrepareIncidentSession(repositoryMount, "", incidentID)
	if err != nil {
		return err
	}
	history, err := apihistory.ReadThrough(context.Background(), filepath.Join(repositoryMount, relative, "api-history"), through)
	if err != nil {
		return err
	}
	type key struct{ apiVersion, kind, namespace, name string }
	first := map[key]map[string]any{}
	var order []key
	for _, event := range history.Events {
		if event.Phase != "before" {
			continue
		}
		var object map[string]any
		if json.Unmarshal(history.Body(event), &object) != nil {
			continue
		}
		metadata, _ := object["metadata"].(map[string]any)
		k := key{asString(object["apiVersion"]), asString(object["kind"]), asString(metadata["namespace"]), asString(metadata["name"])}
		if k.kind == "" || k.name == "" {
			continue
		}
		if _, seen := first[k]; !seen {
			first[k] = object
			order = append(order, k)
		}
	}
	if len(order) == 0 {
		return nil
	}
	raw, err := os.ReadFile(filepath.Join(dir, "open@detect.json"))
	if err != nil {
		return fmt.Errorf("pre-apply from writes needs the detection capture: %w", err)
	}
	var snapshot map[string]any
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return err
	}
	for _, k := range order {
		object := first[k]
		same := func(item any) bool {
			m, _ := item.(map[string]any)
			metadata, _ := m["metadata"].(map[string]any)
			return asString(m["kind"]) == k.kind && asString(m["apiVersion"]) == k.apiVersion && asString(metadata["name"]) == k.name && asString(metadata["namespace"]) == k.namespace
		}
		replace := func(field string) {
			list, _ := snapshot[field].([]any)
			for i, item := range list {
				if same(item) {
					list[i] = object
					return
				}
			}
			snapshot[field] = append(list, object)
		}
		replace("objects")
		if section, ok := typedSections[k.kind]; ok {
			if _, present := snapshot[section]; present || snapshot["object_snapshot_version"] == nil {
				replace(section)
			}
		}
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	if err := writeCapturedFile(filepath.Join(dir, "open@pre-apply-recordings.json"), []byte("[]")); err != nil {
		return err
	}
	return writeCapturedFile(target, data)
}

func asString(value any) string {
	text, _ := value.(string)
	return text
}

package sdktest

import (
	"context"
	"encoding/json"
	"fmt"
	jsonpatch "github.com/evanphx/json-patch"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// CheckMitigationRepairs proves effects on the faulted snapshot itself. It
// neither copies the recorded repaired state nor assumes external effects.
func CheckMitigationRepairs(p program.Diagnoser, m program.Mitigator, c CorpusCase) error {
	plan, err := m.Mitigate(context.Background(), &ReplayReadView{Snapshot: c.Open, Calls: c.PhaseRecordings("open")}, c.Bindings)
	if err != nil {
		return fmt.Errorf("program.mitigation-unreplayable: Mitigate: %w", err)
	}
	if err := plan.Validate(); err != nil {
		return fmt.Errorf("program.mitigation-unreplayable: invalid plan: %w", err)
	}
	raw, err := json.Marshal(c.Open)
	if err != nil {
		return err
	}
	var fixture map[string]any
	if err := json.Unmarshal(raw, &fixture); err != nil {
		return err
	}
	for _, action := range plan.Actions {
		var field program.SetField
		switch a := action.(type) {
		case program.SetField:
			field = a
		case *program.SetField:
			if a == nil {
				return fmt.Errorf("program.mitigation-unreplayable: nil SetField")
			}
			field = *a
		default:
			return fmt.Errorf("program.mitigation-unreplayable: effect %T requires unavailable source, revision or rollout evidence", action)
		}
		object, err := fixtureObject(fixture, field.Ref)
		if err != nil {
			return fmt.Errorf("program.mitigation-unreplayable: %w", err)
		}
		before, err := json.Marshal(object)
		if err != nil {
			return err
		}
		if err := program.ApplySetField(object, field.Path, field.Value); err != nil {
			return fmt.Errorf("program.mitigation-unreplayable: %w", err)
		}
		after, err := json.Marshal(object)
		if err != nil {
			return err
		}
		patch, err := jsonpatch.CreateMergePatch(before, after)
		if err != nil {
			return err
		}
		patched, err := jsonpatch.MergePatch(before, patch)
		if err != nil {
			return err
		}
		for key := range object {
			delete(object, key)
		}
		if err := json.Unmarshal(patched, &object); err != nil {
			return err
		}
	}
	raw, err = json.Marshal(fixture)
	if err != nil {
		return err
	}
	var repaired Snapshot
	if err := json.Unmarshal(raw, &repaired); err != nil {
		return fmt.Errorf("program.mitigation-unreplayable: patched fixture: %w", err)
	}
	// A patched fixture has no new logs, probes or source-diff observations.
	// Neither the faulted nor recorded fixed answers prove effects here.
	verdict := program.RunDiagnose(context.Background(), p, &ReplayReadView{Snapshot: repaired}, c.Bindings, 10*time.Second)
	if verdict.Result == program.Unknown {
		return fmt.Errorf("program.mitigation-unreplayable: post-plan diagnosis requires unavailable observations: %+v", verdict)
	}
	if verdict.Result != program.NoMatch {
		return fmt.Errorf("program.mitigation-not-repaired: program %s after applying plan got %+v, want NoMatch", p.Spec().ID, verdict)
	}
	return nil
}

func fixtureObject(snapshot map[string]any, ref sdk.ObjectRef) (map[string]any, error) {
	kinds := map[string]string{"Service": "services", "Pod": "pods", "Deployment": "deployments", "ReplicaSet": "replicaSets", "ConfigMap": "configMaps", "Endpoints": "endpoints", "EndpointSlice": "endpointSlices", "NetworkPolicy": "networkPolicies"}
	versions := map[string]string{"Service": "v1", "Pod": "v1", "Deployment": "apps/v1", "ReplicaSet": "apps/v1", "ConfigMap": "v1", "Endpoints": "v1", "EndpointSlice": "discovery.k8s.io/v1", "NetworkPolicy": "networking.k8s.io/v1"}
	if ref.Namespace != snapshot["namespace"] || versions[ref.Kind] != ref.APIVersion || ref.Name == "" {
		return nil, fmt.Errorf("invalid fixture reference %+v", ref)
	}
	list, _ := snapshot[kinds[ref.Kind]].([]any)
	for _, item := range list {
		object, _ := item.(map[string]any)
		meta, _ := object["metadata"].(map[string]any)
		if meta["name"] == ref.Name && meta["namespace"] == ref.Namespace {
			return object, nil
		}
	}
	return nil, fmt.Errorf("fixture lacks %s/%s", ref.Kind, ref.Name)
}

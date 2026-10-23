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

type MitigationReplayStatus string

const (
	MitigationReplayPassed                    MitigationReplayStatus = "replayed"
	MitigationReplaySkippedUnreplayableEffect MitigationReplayStatus = "skipped_unreplayable_effect"
)

// MitigationReplayResult is informational. A skipped effect proves no repair
// and must never substitute for independently verified live trust credits.
type MitigationReplayResult struct {
	ProgramID string                 `json:"program_id"`
	CaseID    string                 `json:"case_id,omitempty"`
	Status    MitigationReplayStatus `json:"mitigation_replay"`
	Effects   []string               `json:"effects,omitempty"`
}

// CheckMitigation validates every plan. SetField-only plans must repair the
// faulted fixture; plans with external effects return an explicit skipped note.
// Diagnosis corpus replay remains a separate mandatory gate.
func CheckMitigation(p program.Diagnoser, m program.Mitigator, c CorpusCase) (MitigationReplayResult, error) {
	result := MitigationReplayResult{ProgramID: p.Spec().ID}
	plan, err := m.Mitigate(context.Background(), &ReplayReadView{Snapshot: c.Open, Calls: c.PhaseRecordings("open")}, c.Bindings)
	if err != nil {
		return result, fmt.Errorf("program.mitigation-unreplayable: Mitigate: %w", err)
	}
	if err := plan.Validate(); err != nil {
		return result, fmt.Errorf("program.mitigation-unreplayable: invalid plan: %w", err)
	}
	for _, action := range plan.Actions {
		switch a := action.(type) {
		case program.SetField:
			if a.ValueFromSource {
				result.Effects = append(result.Effects, "accepted-source SetField")
			}
		case *program.SetField:
			if a != nil && a.ValueFromSource {
				result.Effects = append(result.Effects, "accepted-source SetField")
			}
		default:
			result.Effects = append(result.Effects, fmt.Sprintf("%T", action))
		}
	}
	if len(result.Effects) > 0 {
		result.Status = MitigationReplaySkippedUnreplayableEffect
		return result, nil
	}
	if err := checkMitigationPlanRepairs(p, c, plan); err != nil {
		return result, err
	}
	result.Status = MitigationReplayPassed
	return result, nil
}

// CheckMitigationRepairs preserves the error-only policy entry point. Call
// CheckMitigation when the typed replay status is needed on a validation report.
func CheckMitigationRepairs(p program.Diagnoser, m program.Mitigator, c CorpusCase) error {
	_, err := CheckMitigation(p, m, c)
	return err
}

func checkMitigationPlanRepairs(p program.Diagnoser, c CorpusCase, plan program.Plan) error {
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
		if err := program.ApplySetFieldAction(object, field, nil); err != nil {
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
	// No new external observations are synthesized. Only unchanged direct-Pod
	// TCP inputs survive a Service targetPort edit; fixed answers never prove effects.
	verdict := program.RunDiagnose(context.Background(), p, &ReplayReadView{Snapshot: repaired, Calls: unchangedPodTCPRecordings(c, plan, fixture)}, c.Bindings, 10*time.Second)
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

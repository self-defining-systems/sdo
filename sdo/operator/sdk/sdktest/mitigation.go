package sdktest

import (
	"context"
	"encoding/json"
	"fmt"
	jsonpatch "github.com/evanphx/json-patch"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sort"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type MitigationReplayStatus string

const (
	MitigationReplayPassed                    MitigationReplayStatus = "replayed"
	MitigationReplayObservedTransition        MitigationReplayStatus = "observed-transition"
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
// Diagnosis replay is an analysis helper; its outcome never gates publication.
func CheckMitigation(p program.Diagnoser, m program.Mitigator, c CorpusCase) (MitigationReplayResult, error) {
	result := MitigationReplayResult{ProgramID: p.Spec().ID}
	plan, err := m.Mitigate(context.Background(), &ReplayReadView{Snapshot: c.Open, Calls: c.PhaseRecordings("open")}, c.Bindings)
	if err != nil {
		return result, fmt.Errorf("program.mitigation-unreplayable: Mitigate: %w", err)
	}
	if err := plan.Validate(); err != nil {
		return result, fmt.Errorf("program.mitigation-unreplayable: invalid plan: %w", err)
	}
	if err := program.CheckReplicaSetScales(plan, c.Open, c.Bindings); err != nil {
		return result, fmt.Errorf("program.mitigation-unreplayable: %w", err)
	}
	scripted := false
	for _, action := range plan.Actions {
		var script program.RunScript
		ok := false
		switch a := action.(type) {
		case program.RunScript:
			script, ok = a, true
		case *program.RunScript:
			if a != nil {
				script, ok = *a, true
			}
		}
		if ok {
			scripted = true
			if _, err := ScriptOrigin(script, c); err != nil {
				return result, fmt.Errorf("program.mitigation-not-repaired: %w", err)
			}
		}
	}
	if scripted {
		before := program.RunDiagnose(context.Background(), p, &ReplayReadView{Snapshot: c.Open, Calls: c.PhaseRecordings("open")}, c.Bindings, 5*time.Second)
		after := program.RunDiagnose(context.Background(), p, &ReplayReadView{Snapshot: c.Fixed, Calls: c.PhaseRecordings("fixed")}, c.Bindings, 5*time.Second)
		if before.Result != program.Match || after.Result != program.NoMatch {
			return result, fmt.Errorf("program.mitigation-not-repaired: observed script transition must diagnose Match then NoMatch")
		}
		result.Status = MitigationReplayObservedTransition
		return result, nil
	}
	for _, action := range plan.Actions {
		switch a := action.(type) {
		case program.SetField:
			if a.Value.Origin() == program.SourceValue {
				result.Effects = append(result.Effects, "accepted-source SetField")
			}
		case *program.SetField:
			if a != nil && a.Value.Origin() == program.SourceValue {
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
	var observedFixture map[string]any
	if err := json.Unmarshal(raw, &observedFixture); err != nil {
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
		value, _, resolveErr := program.ResolveSetFieldValue(field, program.ValueInputs{Spec: p.Spec(), Bindings: c.Bindings, Observed: func(ref sdk.ObjectRef) (map[string]any, error) { return fixtureObject(observedFixture, ref) }})
		if resolveErr != nil {
			return fmt.Errorf("program.mitigation-unreplayable: %w", resolveErr)
		}
		if err := program.ApplySetFieldAction(object, field, value); err != nil {
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
	// Generic objects are authoritative. Refresh typed compatibility arrays after
	// patching so typed diagnoses see the same repaired object as generic readers.
	if objects, ok := fixture["objects"].([]any); ok && len(objects) > 0 {
		body, _ := json.Marshal(objects)
		var canonical []unstructured.Unstructured
		if err := json.Unmarshal(body, &canonical); err != nil {
			return err
		}
		compatibility := Snapshot{NamespaceName: c.Open.NamespaceName}
		var local []unstructured.Unstructured
		for _, object := range canonical {
			if object.GetNamespace() == compatibility.NamespaceName {
				local = append(local, object)
			}
		}
		if err := sdk.DecodeTypedObjects(&compatibility, local); err != nil {
			return err
		}
		encoded, _ := json.Marshal(compatibility)
		var legacy map[string]any
		_ = json.Unmarshal(encoded, &legacy)
		for key, value := range legacy {
			if _, isList := value.([]any); isList && key != "objects" {
				fixture[key] = value
			}
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
	if ref.Name == "" || ref.Kind == "" || ref.APIVersion == "" {
		return nil, fmt.Errorf("invalid fixture reference %+v", ref)
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		return nil, err
	}
	var typed Snapshot
	if err = json.Unmarshal(raw, &typed); err != nil {
		return nil, err
	}
	known := sdk.TypedObjects(typed)
	keys := []string{"objects"}
	rest := make([]string, 0, len(snapshot))
	for key := range snapshot {
		if key != "objects" {
			rest = append(rest, key)
		}
	}
	sort.Strings(rest)
	keys = append(keys, rest...)
	for _, key := range keys {
		value := snapshot[key]
		list, _ := value.([]any)
		for _, item := range list {
			object, _ := item.(map[string]any)
			if object == nil {
				continue
			}
			meta, _ := object["metadata"].(map[string]any)
			namespace, _ := meta["namespace"].(string)
			if meta["name"] != ref.Name || namespace != ref.Namespace {
				continue
			}
			api, _ := object["apiVersion"].(string)
			kind, _ := object["kind"].(string)
			if api == ref.APIVersion && kind == ref.Kind {
				return object, nil
			}
			// Old typed fixture arrays predate explicit GVKs. Match by their JSON shape.
			if api == "" && kind == "" {
				for _, candidate := range known {
					if sdk.ObjectIdentity(candidate) == ref {
						a, _ := json.Marshal(candidate.Object)
						var fields map[string]any
						_ = json.Unmarshal(a, &fields)
						delete(fields, "apiVersion")
						delete(fields, "kind")
						b, _ := json.Marshal(fields)
						c, _ := json.Marshal(object)
						if string(b) == string(c) {
							return object, nil
						}
					}
				}
			}
		}
	}
	return nil, fmt.Errorf("fixture lacks %s/%s", ref.Kind, ref.Name)
}

package sdktest

import (
	"context"
	"encoding/json"
	"fmt"
	jsonpatch "github.com/evanphx/json-patch"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"os"
	"path/filepath"
	"sort"
	"strings"
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

// ApplyTimeEffectPrefix starts the replay effect line "recorded-apply-ms:<action>:<ms>": how long
// the action took when it was recorded, measured while users failed.
const ApplyTimeEffectPrefix = "recorded-apply-ms:"

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
	plan, err := replayMitigation(m, &ReplayReadView{Snapshot: c.Open, Calls: c.PhaseRecordings("open")}, c.Bindings)
	if err != nil {
		return result, fmt.Errorf("program.mitigation-unreplayable: Mitigate: %w", err)
	}
	if err := plan.Validate(); err != nil {
		return result, fmt.Errorf("program.mitigation-unreplayable: invalid plan: %w", err)
	}
	if err := program.CheckReplicaSetScales(plan, c.Open); err != nil {
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
	if HasObservedSecretRestore(plan, c) {
		before := program.RunDiagnose(context.Background(), p, &ReplayReadView{Snapshot: c.Open, Calls: c.PhaseRecordings("open")}, c.Bindings, 5*time.Second)
		after := program.RunDiagnose(context.Background(), p, &ReplayReadView{Snapshot: c.Fixed, Calls: c.PhaseRecordings("fixed")}, c.Bindings, 5*time.Second)
		if before.Result != program.Match || after.Result != program.NoMatch {
			return result, fmt.Errorf("program.mitigation-not-repaired: observed Secret restore must diagnose Match then NoMatch")
		}
		result.Status = MitigationReplayObservedTransition
		return result, nil
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
			if list, isList := value.([]any); isList && key != "objects" && len(list) > 0 {
				fixture[key] = value
				// Typed lists are only valid beside the objects they project.
				fixture["object_snapshot_version"] = 1
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

// CheckPlanValid runs Mitigate on the case's opening state and validates the plan it yields exactly as the host
// does before it applies one (Plan.Validate: every action, including a data repair's statement and bounds). A
// learned program then never fails on apply for a reason the check could have caught. A Mitigate that cannot run on
// the recorded reads is not a failure; planOnState says it was unverified.
func CheckPlanValid(m program.Mitigator, c CorpusCase) error {
	_, err := planOnState(m, c)
	return err
}

// planOnState is CheckPlanValid with the reason the plan could not be checked ("" when it was): Mitigate failed on
// the recorded reads (a binding or a read the capture lacks), or it yielded no action there.
func planOnState(m program.Mitigator, c CorpusCase) (unverified string, err error) {
	plan, err := replayMitigation(m, &ReplayReadView{Snapshot: c.Open, Calls: c.PhaseRecordings("open")}, c.Bindings)
	if err != nil {
		return "Mitigate could not run on it: " + err.Error(), nil
	}
	if len(plan.Actions) == 0 {
		return "Mitigate yielded no action on it", nil
	}
	if err := plan.Validate(); err != nil {
		return "", fmt.Errorf("program.plan-invalid: Mitigate of the recorded incident yields a plan the host refuses to apply: %w", err)
	}
	return escalationNote(m, plan, c), nil
}

// CheckPlansOnIncident runs CheckPlanValid on the incident's own captured open state: the controller's
// open@* snapshots in dir, with their recorded reads. The closing turn's check has them before Go writes any
// corpus case, so a brand-new program's plan is built and validated on real state. A program that needs bindings
// the capture does not carry cannot run there, which CheckPlanValid treats as unverified.
func CheckPlansOnIncident(m program.Mitigator, dir string) (unverified []string, err error) {
	paths, err := filepath.Glob(filepath.Join(dir, "open@*.json"))
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	for _, path := range paths {
		if strings.HasSuffix(path, "-recordings.json") {
			continue
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return unverified, err
		}
		snapshot, err := decodeSnapshotStrict(raw)
		if err != nil {
			return unverified, fmt.Errorf("incident capture %s: %w", filepath.Base(path), err)
		}
		c := CorpusCase{Open: snapshot}
		if recorded, readErr := os.ReadFile(strings.TrimSuffix(path, ".json") + "-recordings.json"); readErr == nil {
			if err := json.Unmarshal(recorded, &c.Recordings); err != nil {
				return unverified, fmt.Errorf("incident capture %s recordings: %w", filepath.Base(path), err)
			}
		}
		c.Open.SourceName = filepath.Base(path)
		why, err := planOnState(m, c)
		if err != nil {
			return unverified, fmt.Errorf("%s: %w", filepath.Base(path), err)
		}
		if why != "" {
			unverified = append(unverified, filepath.Base(path)+": "+why)
		}
	}
	return unverified, nil
}

// escalationNote says when a Mitigate is only an Escalate while Diagnose does not call the cause out of reach. That
// partial playbook is legitimate, but the automatic stage cannot repair with it: it records the attempt FAILED and the
// responder runs. The note is a coverage note, never a failure.
func escalationNote(m program.Mitigator, plan program.Plan, c CorpusCase) string {
	if _, sole := program.EscalationOf(plan); !sole || len(plan.Actions) != 1 {
		return ""
	}
	if diagnoser, ok := m.(program.Diagnoser); ok {
		view := &ReplayReadView{Snapshot: c.Open, Calls: c.PhaseRecordings("open")}
		verdict := program.RunDiagnose(context.Background(), diagnoser, view, c.Bindings, 5*time.Second)
		if verdict.Result == program.Match && verdict.Reach == program.ReachOutOfReach {
			return ""
		}
	}
	return "Mitigate only escalates, so a repeat still needs the responder; commands such as SQL can be automated with program.EnsureExec"
}

func replayMitigation(m program.Mitigator, view *ReplayReadView, bindings program.Bindings) (plan program.Plan, err error) {
	defer func() {
		if value := recover(); value != nil {
			if value != sdk.ErrReadReplyLimit {
				panic(value)
			}
			plan, err = program.Plan{}, sdk.ErrReadReplyLimit
		}
	}()
	source := view.ReplayProgramReadView(view)
	plan, err = m.Mitigate(context.Background(), source, bindings)
	if aborted := program.ReadAbortError(source); aborted != nil {
		return program.Plan{}, aborted
	}
	return plan, err
}

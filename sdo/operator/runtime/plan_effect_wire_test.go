package runtime

import (
	"encoding/json"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// The executor's own write observation and rollback compare the plan's
// after-image with a typed read, which omits a written zero. A SetField to an
// omitted zero is observed, and its rollback restores the pre-image.
func TestPlanExecutorObservesAndRollsBackAPIOmittedZero(t *testing.T) {
	template := restartTemplate()
	template.Spec.Containers[0].LivenessProbe = &corev1.Probe{InitialDelaySeconds: 600, ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Port: intstr.FromInt32(8080)}}}
	deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "web", UID: "uid-web", ResourceVersion: "1"}, Spec: appsv1.DeploymentSpec{Template: template}}
	deployment.Kind = "Deployment"
	client := fake.NewSimpleClientset(deployment)
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}
	executor := PlanExecutor{Client: TypedPlanClient{Client: client}, Rollouts: recordedRollouts{new([]sdk.ObjectRef)}, Verifier: passingVerifier{}, Now: func() time.Time { return time.Unix(0, 0) }}
	incident := IncidentRequest{IncidentID: "synthetic", Namespace: "app", RepairPolicy: "recorded-actions", Findings: []sdk.Finding{{RuleID: "synthetic", PrimaryResource: ref}}}
	spec := program.ProgramSpec{ID: "synthetic", Params: []program.ParamSpec{{Name: "delay", ValueKind: program.IntValue, Default: 0}}}
	path := "spec.template.spec.containers[name=main].livenessProbe.initialDelaySeconds"
	plan := program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: path, Value: program.FromParam("delay"), Reason: "no start delay"}}}
	plan = program.BindPlanValues(plan, program.ValueInputs{Spec: spec, Bindings: program.Bindings{Values: map[string]any{"delay": 0}}})
	result, err := executor.Apply(t.Context(), incident, plan, false)
	if err != nil || !result.Applied {
		t.Fatalf("Apply = %+v, %v", result, err)
	}
	delay := func() int32 {
		live, err := client.AppsV1().Deployments("app").Get(t.Context(), "web", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return live.Spec.Template.Spec.Containers[0].LivenessProbe.InitialDelaySeconds
	}
	if got := delay(); got != 0 {
		t.Fatalf("initialDelaySeconds = %d after the write, want 0", got)
	}
	if err := executor.Rollback(t.Context(), result); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if got := delay(); got != 600 {
		t.Fatalf("initialDelaySeconds = %d after rollback, want 600", got)
	}
}

func effectWireDeployment(container map[string]any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "demo", "namespace": "app", "uid": "same-object"},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
			"containers": []any{container},
			"volumes":    []any{map[string]any{"name": "data", "emptyDir": map[string]any{}}},
		}}},
	}}
}

func effectWireContainer(delay any, readOnly any) map[string]any {
	probe := map[string]any{"httpGet": map[string]any{"port": int64(8080), "path": "/", "scheme": "HTTP"}, "timeoutSeconds": int64(1), "periodSeconds": int64(10), "successThreshold": int64(1), "failureThreshold": int64(3)}
	if delay != nil {
		probe["initialDelaySeconds"] = delay
	}
	mount := map[string]any{"name": "data", "mountPath": "/data"}
	if readOnly != nil {
		mount["readOnly"] = readOnly
	}
	return map[string]any{"name": "web", "image": "example.invalid/web:1", "livenessProbe": probe, "volumeMounts": []any{mount}}
}

// effectWireResult is a source reconcile whose merge patch writes container
// (the rendered source) over a broken pre-image.
func effectWireResult(t *testing.T, container map[string]any) PlanApplicationResult {
	t.Helper()
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "demo"}
	broken := effectWireDeployment(effectWireContainer(int64(600), true))
	patch, err := json.Marshal(map[string]any{"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{container}}}}})
	if err != nil {
		t.Fatal(err)
	}
	action := program.ReconcileFromSource{Ref: ref}
	return PlanApplicationResult{
		Plan: program.Plan{Actions: []program.Action{action}}, ActionsApplied: []program.Action{action},
		PreImages: []PlanPreImage{{Ref: ref, Object: broken}},
		Patches:   []PlanPatch{{Ref: ref, Before: broken, Patch: patch}},
	}
}

// The API server omits a zero or false scalar of an omitempty field on read,
// so a repair that writes the source's explicit zero reads back without it.
// The effect check compares in that wire form, so the absent field matches.
func TestPlanEffectMatchesAPIOmittedZeroValues(t *testing.T) {
	for name, written := range map[string]map[string]any{
		"zero probe delay": effectWireContainer(int64(0), nil),
		"false readOnly":   effectWireContainer(nil, false),
		"both":             effectWireContainer(float64(0), false),
	} {
		t.Run(name, func(t *testing.T) {
			live := effectWireDeployment(effectWireContainer(nil, nil))
			get := func(sdk.ObjectRef) *unstructured.Unstructured { return live.DeepCopy() }
			explanation := explainPlanDesiredFields(effectWireResult(t, written), get, true)
			if !explanation.Matched {
				t.Fatalf("correct repair not confirmed: reason %q, mismatches %v", explanation.Reason, explanation.Mismatches)
			}
		})
	}
}

// Wire normalization must not hide a repair that did not take effect: a
// non-zero expected value absent or different live, or a zero expected value
// where live is non-zero, still differs.
func TestPlanEffectStillRejectsRealMismatches(t *testing.T) {
	const delay = "spec.template.spec.containers[0].livenessProbe.initialDelaySeconds"
	for name, tc := range map[string]struct {
		written, live map[string]any
		path          string
	}{
		"non-zero delay absent live":    {effectWireContainer(int64(5), nil), effectWireContainer(nil, nil), delay},
		"non-zero delay different live": {effectWireContainer(int64(5), nil), effectWireContainer(int64(7), nil), delay},
		"zero delay non-zero live":      {effectWireContainer(int64(0), nil), effectWireContainer(int64(600), nil), delay},
		"false readOnly true live":      {effectWireContainer(nil, false), effectWireContainer(nil, true), "spec.template.spec.containers[0].volumeMounts[0].readOnly"},
	} {
		t.Run(name, func(t *testing.T) {
			live := effectWireDeployment(tc.live)
			get := func(sdk.ObjectRef) *unstructured.Unstructured { return live.DeepCopy() }
			explanation := explainPlanDesiredFields(effectWireResult(t, tc.written), get, true)
			if explanation.Matched {
				t.Fatal("a repair that did not take effect was confirmed")
			}
			found := false
			for _, mismatch := range explanation.Mismatches {
				found = found || mismatch.GetPath() == tc.path
			}
			if !found {
				t.Errorf("mismatch on %s not reported: reason %q, mismatches %v", tc.path, explanation.Reason, explanation.Mismatches)
			}
		})
	}
}

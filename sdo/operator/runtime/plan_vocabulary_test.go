package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func TestPlanExecutorStructuredSourceAndDocument(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "flags"}
	before := planObject(ref, nil)
	delete(before.Object, "spec")
	before.SetUID("fixture-uid")
	before.SetResourceVersion("1")
	before.Object["data"] = map[string]any{"flags.json": `{"gc":true,"unrelated":7}`, "other": "preserved"}
	accepted := before.DeepCopy()
	accepted.Object["data"] = map[string]any{"flags.json": `{"gc":false,"unrelated":0}`, "other": "source"}
	after := before.DeepCopy()
	after.Object["data"].(map[string]any)["flags.json"] = `{"gc":false,"unrelated":7}`
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before}}
	executor := PlanExecutor{Client: client, Source: planFakeSource{source: accepted}, AcceptedRevision: func() string { return "accepted" }, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}, Safety: fixturePlanSafety(t, ref, before, accepted, after)}
	action := program.SetField{Ref: ref, Path: "data[flags.json]", DocumentFormat: "json", DocumentPath: []string{"gc"}, ValueFromSource: true}
	result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions", SourceCommit: "accepted"}, program.Plan{Actions: []program.Action{action}}, false)
	if err != nil || !result.VerificationPassed {
		t.Fatalf("apply: %v", err)
	}
	if got := client.objects[ref].Object["data"].(map[string]any); got["flags.json"] != `{"gc":false,"unrelated":7}` || got["other"] != "preserved" {
		t.Fatal("unrelated values changed")
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	var restored PlanApplicationResult
	if err = json.Unmarshal(encoded, &restored); err != nil {
		t.Fatal(err)
	}
	proto, err := result.Proto()
	if err != nil {
		t.Fatal(err)
	}
	decoded := planApplicationState(proto).Result.Plan.Actions[0].(program.SetField)
	if !decoded.ValueFromSource || len(decoded.DocumentPath) != 1 {
		t.Fatal("source selector lost across contract")
	}
	if err := executor.Rollback(context.Background(), result); err != nil {
		t.Fatal(err)
	}
	if !planValuesEqual(client.objects[ref].Object["data"], before.Object["data"]) {
		t.Fatal("rollback did not restore data")
	}
}

func TestPlanExecutorStructuredRefusesBeforeOpaqueRead(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "flags"}
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{}}
	executor := PlanExecutor{Client: client, AcceptedRevision: func() string { return "accepted" }, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
	_, err := executor.Apply(context.Background(), IncidentRequest{RepairPolicy: "recorded-actions", SourceCommit: "accepted"}, program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "data[key]", Value: "safe"}}}, false)
	if err == nil || !strings.Contains(err.Error(), "safety") {
		t.Fatalf("must refuse for missing safety evidence before Get: %v", err)
	}
}

func fixturePlanSafety(t *testing.T, ref sdk.ObjectRef, objects ...*unstructured.Unstructured) *PlanSafetyPolicy {
	t.Helper()
	digests := map[string]bool{}
	for _, o := range objects {
		digests[planSafetyDigest(o)] = true
	}
	return &PlanSafetyPolicy{SourceCommit: "accepted", ExpiresAt: time.Now().Add(time.Hour), Objects: map[sdk.ObjectRef]PlanObjectSafety{ref: {UID: "fixture-uid", Digests: digests}}}
}

func TestPlanExecutorStructuredObservationRejectsMissingEffect(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "flags"}
	before := planObject(ref, nil)
	delete(before.Object, "spec")
	before.SetUID("fixture-uid")
	before.Object["data"] = map[string]any{"key": "before"}
	after := before.DeepCopy()
	after.Object["data"].(map[string]any)["key"] = "after"
	executor := PlanExecutor{Client: ignoreStructuredPatch{&planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before}}}, AcceptedRevision: func() string { return "accepted" }, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}, Safety: fixturePlanSafety(t, ref, before, after)}
	result, err := executor.Apply(context.Background(), IncidentRequest{SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "data[key]", Value: "after"}}}, false)
	if err == nil || result.VerificationPassed {
		t.Fatal("unobserved write reported success")
	}
}

type ignoreStructuredPatch struct{ *planFakeClient }

func (ignoreStructuredPatch) Patch(context.Context, sdk.ObjectRef, types.PatchType, []byte) error {
	return nil
}

func TestPlanSafetyNegativeFixtures(t *testing.T) {
	cases := map[string]map[string]any{
		"secret-envfrom":      {"envFrom": []any{map[string]any{"secretRef": map[string]any{"name": "placeholder"}}}},
		"secret-keyref":       {"env": []any{map[string]any{"valueFrom": map[string]any{"secretKeyRef": map[string]any{"name": "placeholder"}}}}},
		"projected-token":     {"volumes": []any{map[string]any{"projected": map[string]any{"sources": []any{map[string]any{"serviceAccountToken": map[string]any{}}}}}}},
		"csi-hostpath-pvc":    {"volumes": []any{map[string]any{"csi": map[string]any{}}}},
		"implicit-sa":         {"containers": []any{map[string]any{"name": "app"}}},
		"privileged-init":     {"initContainers": []any{map[string]any{"securityContext": map[string]any{"privileged": true}}}},
		"admission-injection": {"volumes": []any{map[string]any{"secret": map[string]any{"secretName": "placeholder"}}}},
	}
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "app"}
	for name, template := range cases {
		t.Run(name, func(t *testing.T) {
			object := planObject(ref, map[string]any{"template": map[string]any{"spec": template}})
			// Even an independently approved digest cannot override the hard predicate.
			policy := fixturePlanSafety(t, ref, object)
			if policy.checkObject(object, ref) == nil {
				t.Fatal("unsafe reference accepted")
			}
		})
	}
	t.Run("route-exfil", func(t *testing.T) {
		ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "app"}
		object := planObject(ref, map[string]any{"type": "ExternalName", "externalName": "example.invalid"})
		if fixturePlanSafety(t, ref, object).checkObject(object, ref) == nil {
			t.Fatal("external route accepted")
		}
	})
	for _, name := range []string{"executable-configmap", "copied-secret-configmap-log"} {
		t.Run(name, func(t *testing.T) {
			ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "app"}
			object := planObject(ref, nil)
			object.Object["data"] = map[string]any{"opaque": "placeholder"}
			policy := fixturePlanSafety(t, ref, planObject(ref, nil))
			if policy.checkObject(object, ref) == nil {
				t.Fatal("unproven opaque content accepted")
			}
		})
	}
}

func TestPlanCacheStructuredUnknown(t *testing.T) {
	cache := &KubernetesCache{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := cache.AwaitWrites(ctx, []ObservedWrite{{Ref: sdk.ObjectRef{APIVersion: "policy/v1", Kind: "PodDisruptionBudget", Namespace: "app", Name: "app"}, ResourceVersion: "2", Strict: true}}); err == nil {
		t.Fatal("unwatched kind reported success")
	}
}

func TestPlanExecutorStructuredFieldFamilies(t *testing.T) {
	workload := func() map[string]any {
		return map[string]any{"template": map[string]any{"spec": map[string]any{"automountServiceAccountToken": false, "containers": []any{map[string]any{"name": "app", "command": []any{"/before"}, "args": []any{"before"}, "env": []any{map[string]any{"name": "MODE", "value": "before"}}, "volumeMounts": []any{map[string]any{"name": "tmp", "mountPath": "/before"}}}}, "nodeSelector": map[string]any{"pool": "before"}, "tolerations": []any{}, "affinity": map[string]any{}}}}
	}
	cases := []struct {
		kind, api, path string
		spec            map[string]any
		value           any
	}{
		{"Deployment", "apps/v1", "spec.template.spec.containers[name=app].command", workload(), []any{"/after"}},
		{"Deployment", "apps/v1", "spec.template.spec.containers[name=app].args", workload(), []any{"after"}},
		{"Deployment", "apps/v1", "spec.template.spec.containers[name=app].env[name=MODE].value", workload(), "after"},
		{"Deployment", "apps/v1", "spec.template.spec.containers[name=app].volumeMounts", workload(), []any{map[string]any{"name": "tmp", "mountPath": "/after"}}},
		{"Deployment", "apps/v1", "spec.template.spec.nodeSelector", workload(), map[string]any{"pool": "after"}},
		{"Deployment", "apps/v1", "spec.template.spec.tolerations", workload(), []any{map[string]any{"key": "dedicated", "operator": "Exists"}}},
		{"Deployment", "apps/v1", "spec.template.spec.affinity", workload(), map[string]any{"podAffinity": map[string]any{}}},
		{"Service", "v1", "spec.selector", map[string]any{"selector": map[string]any{"app": "before"}}, map[string]any{"app": "after"}},
		{"Service", "v1", "spec.ports[name=http].targetPort", map[string]any{"selector": map[string]any{"app": "app"}, "ports": []any{map[string]any{"name": "http", "port": float64(80), "targetPort": float64(9999)}}}, float64(8080)},
		{"NetworkPolicy", "networking.k8s.io/v1", "spec", map[string]any{"podSelector": map[string]any{}}, map[string]any{"podSelector": map[string]any{}, "policyTypes": []any{"Ingress"}}},
		{"HorizontalPodAutoscaler", "autoscaling/v2", "spec.maxReplicas", map[string]any{"maxReplicas": float64(1)}, float64(5)},
		{"PodDisruptionBudget", "policy/v1", "spec.minAvailable", map[string]any{"minAvailable": float64(0)}, float64(1)},
	}
	for _, c := range cases {
		t.Run(c.kind+"/"+c.path, func(t *testing.T) {
			ref := sdk.ObjectRef{APIVersion: c.api, Kind: c.kind, Namespace: "app", Name: "app"}
			before := planObject(ref, c.spec)
			before.SetUID("fixture-uid")
			after := before.DeepCopy()
			if err := program.ApplySetField(after.Object, c.path, c.value); err != nil {
				t.Fatal(err)
			}
			client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before.DeepCopy()}}
			e := PlanExecutor{Client: client, AcceptedRevision: func() string { return "accepted" }, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}, Safety: fixturePlanSafety(t, ref, before, after)}
			// Selector is existing vocabulary: ValueFromSource also exercises source resolution.
			a := program.SetField{Ref: ref, Path: c.path, Value: c.value}
			if !program.ExpandedSetField(a) {
				a.Value = nil
				a.ValueFromSource = true
				e.Source = planFakeSource{source: after}
			}
			result, err := e.Apply(context.Background(), IncidentRequest{RepairPolicy: "recorded-actions", SourceCommit: "accepted"}, program.Plan{Actions: []program.Action{a}}, false)
			if err != nil || !result.VerificationPassed {
				t.Fatalf("apply: %v", err)
			}
			if err := e.Rollback(context.Background(), result); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPlanExecutorServiceAccountMustAlreadyExistAndBeTokenless(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "app"}
	account := sdk.ObjectRef{APIVersion: "v1", Kind: "ServiceAccount", Namespace: "app", Name: "existing"}
	before := planObject(ref, map[string]any{"template": map[string]any{"spec": map[string]any{"automountServiceAccountToken": false, "serviceAccountName": "old", "containers": []any{map[string]any{"name": "app"}}}}})
	before.SetUID("fixture-uid")
	after := before.DeepCopy()
	if err := program.ApplySetField(after.Object, "spec.template.spec.serviceAccountName", "existing"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		account *corev1.ServiceAccount
		want    bool
	}{
		{"absent", nil, false},
		{"implicit-token", &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "existing", Namespace: "app", UID: "account-uid"}}, false},
		{"token-reference", &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "existing", Namespace: "app", UID: "account-uid"}, AutomountServiceAccountToken: ptr.To(false), Secrets: []corev1.ObjectReference{{Name: "placeholder"}}}, false},
		{"existing-tokenless", &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "existing", Namespace: "app", UID: "account-uid"}, AutomountServiceAccountToken: ptr.To(false)}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := fake.NewSimpleClientset()
			if tc.account != nil {
				if _, err := client.CoreV1().ServiceAccounts("app").Create(context.Background(), tc.account, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			objects := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before.DeepCopy()}}
			safety := fixturePlanSafety(t, ref, before, after)
			safety.ServiceAccounts = map[sdk.ObjectRef]string{account: "account-uid"}
			e := PlanExecutor{Client: planIdentityTestClient{objects, TypedPlanClient{Client: client}}, AcceptedRevision: func() string { return "accepted" }, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}, Safety: safety}
			result, err := e.Apply(context.Background(), IncidentRequest{SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "spec.template.spec.serviceAccountName", Value: "existing"}}}, false)
			if (err == nil) != tc.want || (!tc.want && (result.Applied || len(objects.patches) > 0)) {
				t.Fatalf("applied=%v err=%v", result.Applied, err)
			}
		})
	}
}

type planIdentityTestClient struct {
	*planFakeClient
	identity TypedPlanClient
}

func (c planIdentityTestClient) TokenlessServiceAccount(ctx context.Context, ref sdk.ObjectRef) (string, error) {
	return c.identity.TokenlessServiceAccount(ctx, ref)
}

func FuzzStructuredFieldPathAndSecretPredicate(f *testing.F) {
	for _, path := range []string{"spec.replicas", "spec.template.spec.containers[name=app].args", "data[flags.json]", "spec.selector.app", "spec.template.spec.containers[0].env[0].valueFrom.secretKeyRef.name", "spec.ports[-1].port", "spec.ports[name=http].targetPort"} {
		f.Add(path)
	}
	f.Fuzz(func(t *testing.T, path string) {
		if len(path) > 2048 {
			t.Skip()
		}
		ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Secret", Namespace: "app", Name: "placeholder"}
		if (program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: path, Value: "placeholder"}}}).Validate() == nil {
			t.Fatal("Secret field accepted")
		}
		object := planObject(sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "fixture"}, map[string]any{"template": map[string]any{"spec": map[string]any{"automountServiceAccountToken": false, "envFrom": []any{map[string]any{"secretRef": map[string]any{"name": path}}}}}})
		if planSecretPredicate(object) == nil {
			t.Fatal("credential reference accepted")
		}
		// Malformed selectors and arbitrary names must fail without panicking.
		_ = program.ApplySetField(map[string]any{"spec": map[string]any{"replicas": float64(1)}}, path, "placeholder")
	})
}

func TestPlanExecutorStructuredYAMLAndMissingKey(t *testing.T) {
	for _, format := range []string{"json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "flags"}
			before := planObject(ref, nil)
			delete(before.Object, "spec")
			before.SetUID("fixture-uid")
			text, want := `{"gc":true,"unrelated":7}`, `{"gc":false,"unrelated":7}`
			if format == "yaml" {
				text = "gc: true\nunrelated: 7\n"
				want = "gc: false\nunrelated: 7\n"
			}
			before.Object["data"] = map[string]any{"flags": text}
			after := before.DeepCopy()
			after.Object["data"].(map[string]any)["flags"] = want
			client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before}}
			e := PlanExecutor{Client: client, AcceptedRevision: func() string { return "accepted" }, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}, Safety: fixturePlanSafety(t, ref, before, after)}
			a := program.SetField{Ref: ref, Path: "data[flags]", DocumentFormat: format, DocumentPath: []string{"gc"}, Value: false}
			result, err := e.Apply(context.Background(), IncidentRequest{SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{a}}, false)
			if err != nil || !result.VerificationPassed {
				t.Fatal(err)
			}
			if err := e.Rollback(context.Background(), result); err != nil {
				t.Fatal(err)
			}
		})
	}
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "missing-key"}
	before := planObject(ref, nil)
	delete(before.Object, "spec")
	before.SetUID("fixture-uid")
	after := before.DeepCopy()
	after.Object["data"] = map[string]any{"missing": "safe"}
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before}}
	e := PlanExecutor{Client: client, Source: planFakeSource{source: after}, AcceptedRevision: func() string { return "accepted" }, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}, Safety: fixturePlanSafety(t, ref, before, after)}
	if result, err := e.Apply(context.Background(), IncidentRequest{SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "data[missing]", ValueFromSource: true}}}, false); err != nil || !result.VerificationPassed {
		t.Fatal(err)
	}
}

type structuredFixtureProgram struct {
	action program.Action
	client *planFakeClient
	ref    sdk.ObjectRef
	want   any
	path   string
}

func (p structuredFixtureProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "synthetic-structured", Params: []program.ParamSpec{{Name: "target", Kind: program.Kind(p.ref.Kind), Required: true}}}
}
func (p structuredFixtureProgram) Diagnose(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {
	value, err := program.FieldValue(p.client.objects[p.ref].Object, p.path)
	if err == nil && planValuesEqual(value, p.want) {
		return program.Verdict{Result: program.NoMatch}, nil
	}
	return program.Verdict{Result: program.Match}, nil
}
func (p structuredFixtureProgram) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{p.action}}, nil
}

func TestResponderToolsAcceptStructuredSetField(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "flags"}
	before := planObject(ref, nil)
	delete(before.Object, "spec")
	before.SetUID("fixture-uid")
	before.Object["data"] = map[string]any{"key": "before"}
	after := before.DeepCopy()
	after.Object["data"].(map[string]any)["key"] = "after"
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before}}
	p := structuredFixtureProgram{action: program.SetField{Ref: ref, Path: "data[key]", Value: "after"}, client: client, ref: ref, want: "after", path: "data[key]"}
	registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
	if err != nil {
		t.Fatal(err)
	}
	e := PlanExecutor{Client: client, AcceptedRevision: func() string { return "accepted" }, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}, Safety: fixturePlanSafety(t, ref, before, after)}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "synthetic", Namespace: "app", SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, registry, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, nil, e, 5)
	loop.ReadView = func(sdk.DetectionContext) program.ReadView { return &sdktest.ReplayReadView{} }
	planned := loop.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: p.Spec().ID, Bindings: program.Bindings{"target": ref}})
	if planned.Error != "" || planned.PlanID == "" {
		t.Fatalf("plan: %s", planned.Error)
	}
	applied := loop.Handle(context.Background(), ToolRequest{Command: "apply", PlanIDs: []string{planned.PlanID}})
	if applied.Error != "" || applied.ApplicationID == "" || len(client.patches) != 1 {
		t.Fatalf("apply: %s", applied.Error)
	}
}

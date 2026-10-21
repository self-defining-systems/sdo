package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	jsonpatch "github.com/evanphx/json-patch"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/core"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type planFakeClient struct {
	objects map[sdk.ObjectRef]*unstructured.Unstructured
	patches []json.RawMessage
}

func (c *planFakeClient) Get(_ context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	object := c.objects[ref]
	if object == nil {
		return nil, errors.New("missing object")
	}
	return object.DeepCopy(), nil
}

func (c *planFakeClient) Patch(_ context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	if kind != types.MergePatchType {
		return errors.New("wrong patch type")
	}
	before, _ := json.Marshal(c.objects[ref].Object)
	merged, err := jsonpatch.MergePatch(before, patch)
	if err != nil {
		return err
	}
	object := new(unstructured.Unstructured)
	if err := json.Unmarshal(merged, &object.Object); err != nil {
		return err
	}
	c.objects[ref] = object
	c.patches = append(c.patches, append([]byte(nil), patch...))
	return nil
}

type planFakeSource struct {
	source   *unstructured.Unstructured
	revision *unstructured.Unstructured
}

func (s planFakeSource) AcceptedManifest(context.Context, string, sdk.ObjectRef) (*unstructured.Unstructured, error) {
	return s.source.DeepCopy(), nil
}
func (s planFakeSource) Revision(context.Context, sdk.ObjectRef, string) (*unstructured.Unstructured, error) {
	return s.revision.DeepCopy(), nil
}

type planFakeVerifier struct{ passed bool }

func (v planFakeVerifier) Verify(context.Context) (bool, error) { return v.passed, nil }

type planTestDetector struct{ finding bool }

func (d planTestDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "health", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Interval: time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1},
		Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical}, OriginatingCommit: "accepted"}
}
func (d planTestDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	if !d.finding {
		return nil, nil
	}
	return []sdk.Finding{{RuleID: "unhealthy", Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
		Summary: "unhealthy", Evidence: "observed", PrimaryResource: sdk.ObjectRef{Kind: "Service", Name: "api"}}}, nil
}

type planFakeRollout struct{ calls int }

func (r *planFakeRollout) Wait(context.Context, []sdk.ObjectRef) error { r.calls++; return nil }

func planObject(ref sdk.ObjectRef, spec map[string]any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": ref.APIVersion, "kind": ref.Kind,
		"metadata": map[string]any{"namespace": ref.Namespace, "name": ref.Name}, "spec": spec,
	}}
}

func splitTestPath(path string) []string {
	if strings.Contains(path, "kubectl.kubernetes.io/restartedAt") {
		return []string{"spec", "template", "metadata", "annotations", "kubectl.kubernetes.io/restartedAt"}
	}
	return strings.Split(path, ".")
}

func testField(object *unstructured.Unstructured, path string) (any, bool) {
	var value any = object.Object
	for _, part := range splitTestPath(path) {
		switch current := value.(type) {
		case map[string]any:
			value = current[part]
		case []any:
			index, err := strconv.Atoi(part)
			if err != nil || index >= len(current) {
				return nil, false
			}
			value = current[index]
		default:
			return nil, false
		}
	}
	return value, value != nil
}

func TestPlanExecutorActionsAndRollback(t *testing.T) {
	service := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	deployment := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
	tests := []struct {
		name   string
		ref    sdk.ObjectRef
		before *unstructured.Unstructured
		action program.Action
		source planFakeSource
		path   string
		want   any
	}{
		{"reconcile", service, planObject(service, map[string]any{"ports": []any{map[string]any{"port": float64(80), "targetPort": float64(9999)}}}), program.ReconcileFromSource{Ref: service}, planFakeSource{source: planObject(service, map[string]any{"ports": []any{map[string]any{"port": float64(80), "targetPort": float64(8080)}}})}, "spec.ports.0.targetPort", float64(8080)},
		{"revision", deployment, planObject(deployment, map[string]any{"replicas": float64(2)}), program.Rollback{Ref: deployment, ToRevision: "previous"}, planFakeSource{revision: planObject(deployment, map[string]any{"replicas": float64(1)})}, "spec.replicas", float64(1)},
		{"set field", service, planObject(service, map[string]any{"ports": []any{map[string]any{"port": float64(80), "targetPort": float64(9999)}}}), program.SetField{Ref: service, Path: "spec.ports[0].targetPort", Value: 8080}, planFakeSource{}, "spec.ports.0.targetPort", float64(8080)},
		{"restart", deployment, planObject(deployment, map[string]any{"template": map[string]any{"metadata": map[string]any{"annotations": map[string]any{}}}}), program.RestartRollout{Ref: deployment}, planFakeSource{}, "spec.template.metadata.annotations.kubectl.kubernetes.io/restartedAt", "2026-10-20T00:00:00Z"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{test.ref: test.before.DeepCopy()}}
			rollout := &planFakeRollout{}
			executor := PlanExecutor{Client: client, Source: test.source, Rollouts: rollout, Verifier: planFakeVerifier{passed: true}, Now: func() time.Time { return time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC) }}
			result, err := executor.Apply(context.Background(), IncidentRequest{RepairPolicy: "recorded-actions", SourceCommit: "accepted"}, program.Plan{Actions: []program.Action{test.action}}, false)
			if err != nil || !result.Applied || !result.VerificationPassed || result.Duration < 0 || len(result.ActionsApplied) != 1 || rollout.calls != 1 {
				t.Fatalf("apply: %+v, %v", result, err)
			}
			value, found := testField(client.objects[test.ref], test.path)
			if !found || value != test.want {
				t.Fatalf("field = %v, found = %v", value, found)
			}
			if err := executor.Rollback(context.Background(), result); err != nil {
				t.Fatal(err)
			}
			before, _ := json.Marshal(test.before.Object)
			after, _ := json.Marshal(client.objects[test.ref].Object)
			if string(before) != string(after) {
				t.Fatalf("rollback got %s, want %s", after, before)
			}
		})
	}
}

func TestPlanExecutorDryRunReturnsPatchesWithoutMutation(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
	before := planObject(ref, map[string]any{"replicas": float64(1)})
	for _, policy := range []string{"recorded-actions", "commit"} {
		client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before.DeepCopy()}}
		executor := PlanExecutor{Client: client}
		result, err := executor.Apply(context.Background(), IncidentRequest{RepairPolicy: policy}, program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "spec.replicas", Value: 2}}}, true)
		if err != nil || result.Applied || !result.DryRun || len(result.Patches) != 1 || len(client.patches) != 0 {
			t.Fatalf("%s dry run: %+v, %v", policy, result, err)
		}
	}
}

func TestPlanExecutorRejectsPolicyAndInvalidPathBeforeMutation(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
	for _, test := range []struct{ policy, path string }{{"commit", "spec.replicas"}, {"recorded-actions", "metadata.labels.owner"}} {
		client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: planObject(ref, map[string]any{"replicas": float64(1)})}}
		executor := PlanExecutor{Client: client}
		_, err := executor.Apply(context.Background(), IncidentRequest{RepairPolicy: test.policy}, program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: test.path, Value: 2}}}, false)
		if err == nil || len(client.patches) != 0 {
			t.Fatalf("policy %q path %q: err = %v, patches = %d", test.policy, test.path, err, len(client.patches))
		}
	}
}

func TestPlanExecutorReadsAllPreImagesBeforeMutation(t *testing.T) {
	first := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "first"}
	missing := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "missing"}
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{first: planObject(first, map[string]any{"replicas": float64(1)})}}
	executor := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
	_, err := executor.Apply(context.Background(), IncidentRequest{RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{
		program.SetField{Ref: first, Path: "spec.replicas", Value: 2},
		program.SetField{Ref: missing, Path: "spec.replicas", Value: 2},
	}}, false)
	if err == nil || len(client.patches) != 0 {
		t.Fatalf("pre-image failure: err = %v, patches = %d", err, len(client.patches))
	}
}

func TestPlanExecutorReportsFailedVerification(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: planObject(ref, map[string]any{"replicas": float64(1)})}}
	executor := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: false}}
	result, err := executor.Apply(context.Background(), IncidentRequest{RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{
		program.SetField{Ref: ref, Path: "spec.replicas", Value: 2},
	}}, false)
	if err != nil || !result.Applied || !result.VerificationCompleted || result.VerificationPassed {
		t.Fatalf("failed verification: %+v, %v", result, err)
	}
}

func TestPlanExecutorRejectsActionOutsideIncidentNamespace(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "other", Name: "api"}
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: planObject(ref, map[string]any{"replicas": float64(1)})}}
	executor := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
	_, err := executor.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{
		program.SetField{Ref: ref, Path: "spec.replicas", Value: 2},
	}}, false)
	if err == nil || len(client.patches) != 0 {
		t.Fatalf("cross-namespace action: err = %v, patches = %d", err, len(client.patches))
	}
}

func TestPlanHealthVerifierRerunsHealthDetectors(t *testing.T) {
	for _, test := range []struct{ finding, want bool }{{false, true}, {true, false}} {
		verifier := PlanHealthVerifier{Detectors: []sdk.Detector{planTestDetector{finding: test.finding}},
			Provider: core.StaticSnapshotProvider{SnapshotValue: sdktest.Snapshot{NamespaceName: "app"}}}
		passed, err := verifier.Verify(context.Background())
		if err != nil || passed != test.want {
			t.Fatalf("finding = %v: passed = %v, err = %v", test.finding, passed, err)
		}
	}
}

func TestPlanApplicationResultMapsToProto(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
	action := program.SetField{Ref: ref, Path: "spec.replicas", Value: 2, Reason: "restore capacity"}
	result := PlanApplicationResult{Plan: program.Plan{Actions: []program.Action{action}}, Applied: true, ActionsApplied: []program.Action{action}, VerificationPassed: true, VerificationCompleted: true}
	encoded, err := result.Proto()
	if err != nil || !encoded.GetApplied() || encoded.GetDryRun() || encoded.GetVerification().GetResult() != contracts.Verdict_RESULT_NO_MATCH || len(encoded.GetPlan().GetActions()) != 1 || encoded.GetPlan().GetActions()[0].GetSetField().GetValue().GetNumberValue() != 2 {
		t.Fatalf("proto application = %+v, err = %v", encoded, err)
	}
}

func TestPlanApplicationJSONRoundTripsAllActionsAndSetFieldValues(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
	actions := []program.Action{
		program.ReconcileFromSource{Ref: ref},
		program.Rollback{Ref: ref, ToRevision: "previous"},
		program.SetField{Ref: ref, Path: "spec.template.spec.containers[0].resources", Value: map[string]any{"limits": map[string]any{"cpu": "500m"}, "requests": []any{1, true, nil}}, Reason: "restore limits"},
		program.RestartRollout{Ref: ref, Reason: "refresh"},
	}
	application := PlanApplicationResult{Plan: program.Plan{Actions: actions, Rationale: "repair"}, ActionsApplied: actions}
	encoded, err := json.Marshal(application)
	if err != nil {
		t.Fatal(err)
	}
	var decoded PlanApplicationResult
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	encodedAgain, err := json.Marshal(decoded)
	if err != nil || string(encodedAgain) != string(encoded) {
		t.Fatalf("action data changed after reload: %s, want %s, err = %v", encodedAgain, encoded, err)
	}
	for index, action := range decoded.ActionsApplied {
		var ok bool
		switch index {
		case 0:
			_, ok = action.(program.ReconcileFromSource)
		case 1:
			_, ok = action.(program.Rollback)
		case 2:
			_, ok = action.(program.SetField)
		case 3:
			_, ok = action.(program.RestartRollout)
		}
		if !ok {
			t.Fatalf("action %d lost its concrete type: %T", index, action)
		}
	}
	if err := json.Unmarshal([]byte(`{"Plan":{"Actions":[{"kind":"unknown","action":{}}]}}`), &decoded); err == nil {
		t.Fatal("unknown persisted action type accepted")
	}
}

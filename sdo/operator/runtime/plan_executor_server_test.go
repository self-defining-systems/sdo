package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// serverPlanClient mimics the API server: Deployment defaulting after every
// write, a controller status/annotation update that bumps resourceVersion, and
// optimistic concurrency on conditional patches.
type serverPlanClient struct {
	*planFakeClient
	version    int
	afterPatch func(*unstructured.Unstructured)
	// afterDryRun runs after each server dry-run, as a writer racing the executor would.
	afterDryRun func()
	// raceBeforePatch controller updates land between the caller's read and its next patch.
	raceBeforePatch int
	// raceEdit also changes the object on each raceBeforePatch update.
	raceEdit func(*unstructured.Unstructured)
}

func newServerPlanClient(object *unstructured.Unstructured) (*serverPlanClient, sdk.ObjectRef) {
	ref := sdk.ObjectRef{APIVersion: object.GetAPIVersion(), Kind: object.GetKind(), Namespace: object.GetNamespace(), Name: object.GetName()}
	object.SetUID("uid-1")
	object.SetResourceVersion("1")
	client := &serverPlanClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: object}}, version: 1}
	client.settle(object)
	return client, ref
}

// canonicalizeSpec round-trips a Deployment spec through the typed API object,
// as the API server's codec does: resource quantities become canonical ("1000m"
// is stored as "1", "0.5Gi" as "512Mi") and empty optional lists are dropped.
func canonicalizeSpec(object *unstructured.Unstructured) {
	spec, ok := object.Object["spec"].(map[string]any)
	if !ok || object.GetKind() != "Deployment" {
		return
	}
	var typed appsv1.DeploymentSpec
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(spec, &typed); err != nil {
		panic(err)
	}
	canonical, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&typed)
	if err != nil {
		panic(err)
	}
	object.Object["spec"] = canonical
}

// settle applies API-server normalization and defaulting and a controller-managed status bump.
func (c *serverPlanClient) settle(object *unstructured.Unstructured) {
	canonicalizeSpec(object)
	spec, _ := object.Object["spec"].(map[string]any)
	if spec != nil {
		if _, ok := spec["progressDeadlineSeconds"]; !ok {
			spec["progressDeadlineSeconds"] = float64(600)
		}
		if _, ok := spec["revisionHistoryLimit"]; !ok {
			spec["revisionHistoryLimit"] = float64(10)
		}
		containers, _, _ := unstructured.NestedSlice(object.Object, "spec", "template", "spec", "containers")
		for _, item := range containers {
			if container, ok := item.(map[string]any); ok {
				if _, ok := container["terminationMessagePath"]; !ok {
					container["terminationMessagePath"] = "/dev/termination-log"
				}
				if probe, ok := container["readinessProbe"].(map[string]any); ok {
					for key, value := range map[string]any{"periodSeconds": float64(10), "timeoutSeconds": float64(1), "successThreshold": float64(1), "failureThreshold": float64(3)} {
						if _, ok := probe[key]; !ok {
							probe[key] = value
						}
					}
					if get, ok := probe["httpGet"].(map[string]any); ok {
						if _, ok := get["scheme"]; !ok {
							get["scheme"] = "HTTP"
						}
					}
				}
			}
		}
		if len(containers) > 0 {
			_ = unstructured.SetNestedSlice(object.Object, containers, "spec", "template", "spec", "containers")
		}
	}
	c.bump(object)
}

func (c *serverPlanClient) bump(object *unstructured.Unstructured) {
	c.version++
	object.SetResourceVersion(strconv.Itoa(c.version))
	object.SetAnnotations(map[string]string{"deployment.kubernetes.io/revision": strconv.Itoa(c.version)})
	_ = unstructured.SetNestedField(object.Object, int64(c.version), "status", "observedGeneration")
}

func (c *serverPlanClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	if c.raceBeforePatch > 0 {
		c.raceBeforePatch--
		c.bump(c.objects[ref])
		if c.raceEdit != nil {
			c.raceEdit(c.objects[ref])
		}
	}
	var document map[string]any
	if err := json.Unmarshal(patch, &document); err != nil {
		return err
	}
	if metadata, ok := document["metadata"].(map[string]any); ok {
		current := c.objects[ref]
		// The API server reports a failed uid precondition as a conflict too.
		if metadata["uid"] != string(current.GetUID()) || metadata["resourceVersion"] != current.GetResourceVersion() {
			return apierrors.NewConflict(schema.GroupResource{Group: "apps", Resource: "deployments"}, ref.Name, errors.New("the object has been modified"))
		}
	}
	if err := c.planFakeClient.Patch(ctx, ref, kind, patch); err != nil {
		return err
	}
	object := c.objects[ref]
	object.SetUID("uid-1")
	c.settle(object)
	if c.afterPatch != nil {
		c.afterPatch(object)
	}
	return nil
}

// DryRunPatch returns what Patch would store, without persisting it.
func (c *serverPlanClient) DryRunPatch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) (*unstructured.Unstructured, error) {
	scratch := &serverPlanClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: c.objects[ref].DeepCopy()}}, version: c.version}
	err := scratch.Patch(ctx, ref, kind, patch)
	if c.afterDryRun != nil {
		c.afterDryRun()
	}
	if err != nil {
		return nil, err
	}
	return scratch.objects[ref], nil
}

func deploymentWithDrift(image string) *unstructured.Unstructured {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "rate"}
	return planObject(ref, map[string]any{
		"replicas": float64(1),
		"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "rate", "image": image}}}},
	})
}

type callbackRollout struct{ wait func() error }

func (r callbackRollout) Wait(context.Context, []sdk.ObjectRef) error { return r.wait() }

func reconcilePlan(ref sdk.ObjectRef) program.Plan {
	return program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: ref}}}
}

func TestPlanExecutorToleratesServerDefaultingAndControllerUpdates(t *testing.T) {
	client, ref := newServerPlanClient(deploymentWithDrift("registry.invalid/rate:missing"))
	source := planFakeSource{source: deploymentWithDrift("registry.invalid/rate:good")}
	executor := PlanExecutor{Client: client, Source: source, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
	result, err := executor.Apply(context.Background(), IncidentRequest{RepairPolicy: "recorded-actions", SourceCommit: "accepted"}, reconcilePlan(ref), false)
	if err != nil || !result.Applied || result.RolledBack || !result.VerificationCompleted || !result.VerificationPassed {
		t.Fatalf("defaulting reported as failure: %+v, %v", result, err)
	}
	image, _ := testField(client.objects[ref], "spec.template.spec.containers.0.image")
	if image != "registry.invalid/rate:good" {
		t.Fatalf("image = %v", image)
	}
}

func TestPlanExecutorStillReportsThirdPartyEditOfOwnedField(t *testing.T) {
	client, ref := newServerPlanClient(deploymentWithDrift("registry.invalid/rate:missing"))
	client.afterPatch = func(object *unstructured.Unstructured) {
		_ = program.ApplySetField(object.Object, "spec.template.spec.containers[0].image", "registry.invalid/rate:other")
	}
	source := planFakeSource{source: deploymentWithDrift("registry.invalid/rate:good")}
	executor := PlanExecutor{Client: client, Source: source, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
	_, err := executor.Apply(context.Background(), IncidentRequest{RepairPolicy: "recorded-actions", SourceCommit: "accepted"}, reconcilePlan(ref), false)
	if err == nil || !strings.Contains(err.Error(), "concurrent change after action 0") {
		t.Fatalf("third-party edit not reported: %v", err)
	}
}

func TestPlanExecutorCompensationRetriesAfterResourceVersionBump(t *testing.T) {
	client, ref := newServerPlanClient(deploymentWithDrift("registry.invalid/rate:missing"))
	source := planFakeSource{source: deploymentWithDrift("registry.invalid/rate:good")}
	rollout := callbackRollout{wait: func() error {
		client.raceBeforePatch = 2 // the deployment controller writes status before each restore attempt
		return errors.New("rollout did not become ready")
	}}
	executor := PlanExecutor{Client: client, Source: source, Rollouts: rollout, Verifier: planFakeVerifier{passed: true}}
	result, err := executor.Apply(context.Background(), IncidentRequest{RepairPolicy: "recorded-actions", SourceCommit: "accepted"}, reconcilePlan(ref), false)
	if err == nil || !result.RolledBack || result.RollbackError != "" {
		t.Fatalf("compensation did not recover: %+v, %v", result, err)
	}
	image, _ := testField(client.objects[ref], "spec.template.spec.containers.0.image")
	if image != "registry.invalid/rate:missing" {
		t.Fatalf("image after rollback = %v", image)
	}
}

func TestPlanExecutorCompensationRetriesAreBounded(t *testing.T) {
	client, ref := newServerPlanClient(deploymentWithDrift("registry.invalid/rate:missing"))
	source := planFakeSource{source: deploymentWithDrift("registry.invalid/rate:good")}
	rollout := callbackRollout{wait: func() error {
		client.raceBeforePatch = 1000
		return errors.New("rollout did not become ready")
	}}
	executor := PlanExecutor{Client: client, Source: source, Rollouts: rollout, Verifier: planFakeVerifier{passed: true}}
	result, err := executor.Apply(context.Background(), IncidentRequest{RepairPolicy: "recorded-actions", SourceCommit: "accepted"}, reconcilePlan(ref), false)
	if err == nil || result.RolledBack || !strings.Contains(result.RollbackError, "the object has been modified") {
		t.Fatalf("unbounded or silent compensation: %+v, %v", result, err)
	}
}

func TestPlanExecutorReconcilesPendingWriteDespiteServerDefaulting(t *testing.T) {
	client, ref := newServerPlanClient(deploymentWithDrift("registry.invalid/rate:missing"))
	source := planFakeSource{source: deploymentWithDrift("registry.invalid/rate:good")}
	plan := reconcilePlan(ref)
	dry, err := PlanExecutor{Client: client, Source: source}.Apply(context.Background(), IncidentRequest{RepairPolicy: "recorded-actions", SourceCommit: "accepted"}, plan, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Patch(context.Background(), ref, types.MergePatchType, dry.Patches[0].Patch); err != nil {
		t.Fatal(err)
	}
	pending := 0
	dry.PendingAction = &pending
	reconciled, err := PlanExecutor{Client: client}.Reconcile(context.Background(), dry)
	if err != nil || !reconciled.Applied || len(reconciled.ActionsApplied) != 1 {
		t.Fatalf("pending write not recognized: %+v, %v", reconciled, err)
	}
}

// frontendDeployment mirrors the kompose-generated hotel-reservation frontend
// source manifest, whose CPU limit is written as "1000m".
func frontendDeployment(image string) *unstructured.Unstructured {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "frontend"}
	return planObject(ref, map[string]any{
		"replicas": float64(1),
		"strategy": map[string]any{},
		"template": map[string]any{
			"metadata": map[string]any{"creationTimestamp": nil},
			"spec": map[string]any{
				"restartPolicy": "Always",
				"containers": []any{map[string]any{
					"name":    "hotel-reserv-frontend",
					"image":   image,
					"command": []any{"frontend"},
					"args":    []any{},
					"ports":   []any{map[string]any{"containerPort": float64(5000)}},
					"resources": map[string]any{
						"requests": map[string]any{"cpu": "100m"},
						"limits":   map[string]any{"cpu": "1000m", "memory": "0.5Gi"},
					},
				}},
			},
		},
	})
}

func TestPlanExecutorToleratesServerNormalizedQuantities(t *testing.T) {
	client, ref := newServerPlanClient(frontendDeployment("app-image:latest"))
	source := planFakeSource{source: frontendDeployment("registry.invalid/frontend:good")}
	executor := PlanExecutor{Client: client, Source: source, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
	result, err := executor.Apply(context.Background(), IncidentRequest{RepairPolicy: "recorded-actions", SourceCommit: "accepted"}, reconcilePlan(ref), false)
	if err != nil || !result.Applied || result.RolledBack || !result.VerificationCompleted || !result.VerificationPassed {
		t.Fatalf("quantity normalization reported as failure: %+v, %v", result, err)
	}
	limit, _ := testField(client.objects[ref], "spec.template.spec.containers.0.resources.limits.cpu")
	if limit != "1" {
		t.Fatalf("fake server did not canonicalize the quantity: %v", limit)
	}
}

func TestPlanExecutorNamesTheFieldOfAThirdPartyEdit(t *testing.T) {
	client, ref := newServerPlanClient(frontendDeployment("app-image:latest"))
	client.afterPatch = func(object *unstructured.Unstructured) {
		_ = program.ApplySetField(object.Object, "spec.template.spec.containers[0].resources.limits.cpu", "2")
	}
	source := planFakeSource{source: frontendDeployment("registry.invalid/frontend:good")}
	executor := PlanExecutor{Client: client, Source: source, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
	_, err := executor.Apply(context.Background(), IncidentRequest{RepairPolicy: "recorded-actions", SourceCommit: "accepted"}, reconcilePlan(ref), false)
	want := `concurrent change after action 0: spec.template.spec.containers[0].resources.limits.cpu wrote "1" but live is "2"`
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %v, want %q", err, want)
	}
}

// emptyDirDeployment carries a memory-backed emptyDir whose size limit is not
// written in canonical form, plus a readiness probe the server fills defaults into.
func emptyDirDeployment(image, sizeLimit string) *unstructured.Unstructured {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "cache"}
	return planObject(ref, map[string]any{
		"replicas": float64(1),
		"template": map[string]any{"spec": map[string]any{
			"volumes": []any{map[string]any{"name": "scratch", "emptyDir": map[string]any{"medium": "Memory", "sizeLimit": sizeLimit}}},
			"containers": []any{map[string]any{
				"name":           "cache",
				"image":          image,
				"volumeMounts":   []any{map[string]any{"name": "scratch", "mountPath": "/scratch"}},
				"readinessProbe": map[string]any{"httpGet": map[string]any{"path": "/ready", "port": "http"}},
			}},
		}},
	})
}

func TestPlanExecutorToleratesFieldsWithoutHandWrittenCanonicalization(t *testing.T) {
	client, ref := newServerPlanClient(emptyDirDeployment("cache:old", "1Gi"))
	source := planFakeSource{source: emptyDirDeployment("cache:new", "1024Mi")}
	executor := PlanExecutor{Client: client, Source: source, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
	result, err := executor.Apply(context.Background(), IncidentRequest{RepairPolicy: "recorded-actions", SourceCommit: "accepted"}, reconcilePlan(ref), false)
	if err != nil || !result.Applied || result.RolledBack || !result.VerificationPassed {
		t.Fatalf("server-normalized emptyDir.sizeLimit reported as failure: %+v, %v", result, err)
	}
	limit, _ := testField(client.objects[ref], "spec.template.spec.volumes.0.emptyDir.sizeLimit")
	if limit != "1Gi" {
		t.Fatalf("fake server did not canonicalize sizeLimit: %v", limit)
	}
}

func TestPlanExecutorAfterImageComesFromServerDryRun(t *testing.T) {
	client, ref := newServerPlanClient(emptyDirDeployment("cache:old", "1Gi"))
	source := planFakeSource{source: emptyDirDeployment("cache:new", "1024Mi")}
	before := client.objects[ref].DeepCopy()
	result, err := PlanExecutor{Client: client, Source: source}.Apply(context.Background(), IncidentRequest{RepairPolicy: "recorded-actions", SourceCommit: "accepted"}, reconcilePlan(ref), true)
	if err != nil {
		t.Fatal(err)
	}
	limit, _ := testField(result.Patches[0].After, "spec.template.spec.volumes.0.emptyDir.sizeLimit")
	period, _ := testField(result.Patches[0].After, "spec.template.spec.containers.0.readinessProbe.periodSeconds")
	if limit != "1Gi" || period != float64(10) {
		t.Fatalf("after-image is not the server's: sizeLimit=%v periodSeconds=%v", limit, period)
	}
	if !planValuesEqual(before.Object, client.objects[ref].Object) {
		t.Fatal("server dry-run persisted the change")
	}
}

func TestPlanExecutorStillReportsThirdPartyEditOfNormalizedField(t *testing.T) {
	client, ref := newServerPlanClient(emptyDirDeployment("cache:old", "1Gi"))
	client.afterPatch = func(object *unstructured.Unstructured) {
		_ = program.ApplySetField(object.Object, "spec.template.spec.volumes[0].emptyDir.sizeLimit", "2Gi")
	}
	source := planFakeSource{source: emptyDirDeployment("cache:new", "1024Mi")}
	executor := PlanExecutor{Client: client, Source: source, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
	_, err := executor.Apply(context.Background(), IncidentRequest{RepairPolicy: "recorded-actions", SourceCommit: "accepted"}, reconcilePlan(ref), false)
	want := `spec.template.spec.volumes[0].emptyDir.sizeLimit wrote "1Gi" but live is "2Gi"`
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %v, want %q", err, want)
	}
}

func TestPlanExecutorRetriesStatusChurnBetweenDryRunAndApply(t *testing.T) {
	client, ref := newServerPlanClient(emptyDirDeployment("cache:old", "1Gi"))
	client.afterDryRun = func() { client.bump(client.objects[ref]) }
	source := planFakeSource{source: emptyDirDeployment("cache:new", "1024Mi")}
	executor := PlanExecutor{Client: client, Source: source, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}, Sleep: noSleep}
	result, err := executor.Apply(context.Background(), IncidentRequest{RepairPolicy: "recorded-actions", SourceCommit: "accepted"}, reconcilePlan(ref), false)
	if err != nil || !result.Applied || !result.VerificationPassed {
		t.Fatalf("status churn after dry-run failed the application: %+v, %v", result, err)
	}
	image, _ := testField(client.objects[ref], "spec.template.spec.containers.0.image")
	if image != "cache:new" {
		t.Fatalf("image = %v", image)
	}
}

func TestPlanExecutorDetectsOwnedEditBetweenDryRunAndApply(t *testing.T) {
	client, ref := newServerPlanClient(emptyDirDeployment("cache:old", "1Gi"))
	client.afterDryRun = func() {
		client.bump(client.objects[ref])
		_ = program.ApplySetField(client.objects[ref].Object, "spec.template.spec.containers[0].image", "cache:other")
	}
	source := planFakeSource{source: emptyDirDeployment("cache:new", "1024Mi")}
	executor := PlanExecutor{Client: client, Source: source, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}, Sleep: noSleep}
	result, err := executor.Apply(context.Background(), IncidentRequest{RepairPolicy: "recorded-actions", SourceCommit: "accepted"}, reconcilePlan(ref), false)
	if err == nil || !strings.Contains(err.Error(), "concurrent change before action 0: spec.template.spec.containers[0].image") {
		t.Fatalf("owned edit after dry-run not reported: %v", err)
	}
	if result.Applied {
		t.Fatalf("write landed over a concurrent edit: %+v", result)
	}
}

func TestPlanExecutorReconcilesPendingWriteWithServerNormalizedField(t *testing.T) {
	client, ref := newServerPlanClient(emptyDirDeployment("cache:old", "1Gi"))
	source := planFakeSource{source: emptyDirDeployment("cache:new", "1024Mi")}
	dry, err := PlanExecutor{Client: client, Source: source}.Apply(context.Background(), IncidentRequest{RepairPolicy: "recorded-actions", SourceCommit: "accepted"}, reconcilePlan(ref), true)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Patch(context.Background(), ref, types.MergePatchType, dry.Patches[0].Patch); err != nil {
		t.Fatal(err)
	}
	pending := 0
	dry.PendingAction = &pending
	reconciled, err := PlanExecutor{Client: client}.Reconcile(context.Background(), dry)
	if err != nil || !reconciled.Applied || len(reconciled.ActionsApplied) != 1 {
		t.Fatalf("pending write not recognized: %+v, %v", reconciled, err)
	}
}

// withoutDryRun hides the server dry-run, as a client with no PatchOptions transport does.
type withoutDryRun struct{ PlanObjectClient }

func TestPlanExecutorFallsBackToCanonicalizationWithoutServerDryRun(t *testing.T) {
	client, ref := newServerPlanClient(frontendDeployment("app-image:latest"))
	source := planFakeSource{source: frontendDeployment("registry.invalid/frontend:good")}
	executor := PlanExecutor{Client: withoutDryRun{client}, Source: source, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
	result, err := executor.Apply(context.Background(), IncidentRequest{RepairPolicy: "recorded-actions", SourceCommit: "accepted"}, reconcilePlan(ref), false)
	if err != nil || !result.Applied || result.RolledBack || !result.VerificationPassed {
		t.Fatalf("fallback canonicalization reported quantity as failure: %+v, %v", result, err)
	}
}

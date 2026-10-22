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

func TestPlanExecutorRollbackRetriesAfterResourceVersionBump(t *testing.T) {
	client, ref := newServerPlanClient(deploymentWithDrift("registry.invalid/rate:missing"))
	source := planFakeSource{source: deploymentWithDrift("registry.invalid/rate:good")}
	rollout := callbackRollout{wait: func() error { return errors.New("rollout did not become ready") }}
	executor := PlanExecutor{Client: client, Source: source, Rollouts: rollout, Verifier: planFakeVerifier{passed: true}}
	result, err := executor.Apply(context.Background(), IncidentRequest{RepairPolicy: "recorded-actions", SourceCommit: "accepted"}, reconcilePlan(ref), false)
	if err == nil || !result.Applied || result.RolledBack {
		t.Fatalf("a failed rollout wait must leave the applied plan in place: %+v, %v", result, err)
	}
	client.raceBeforePatch = 2 // the deployment controller writes status before each restore attempt
	if err := executor.rollback(context.Background(), &result); err != nil || !result.RolledBack || result.RollbackError != "" {
		t.Fatalf("rollback did not recover: %+v, %v", result, err)
	}
	image, _ := testField(client.objects[ref], "spec.template.spec.containers.0.image")
	if image != "registry.invalid/rate:missing" {
		t.Fatalf("image after rollback = %v", image)
	}
}

func TestPlanExecutorRollbackRetriesAreBounded(t *testing.T) {
	client, ref := newServerPlanClient(deploymentWithDrift("registry.invalid/rate:missing"))
	source := planFakeSource{source: deploymentWithDrift("registry.invalid/rate:good")}
	executor := PlanExecutor{Client: client, Source: source, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
	result, err := executor.Apply(context.Background(), IncidentRequest{RepairPolicy: "recorded-actions", SourceCommit: "accepted"}, reconcilePlan(ref), false)
	if err != nil || !result.Applied {
		t.Fatalf("apply: %+v, %v", result, err)
	}
	client.raceBeforePatch = 1000
	err = executor.rollback(context.Background(), &result)
	if err == nil || result.RolledBack || !strings.Contains(err.Error(), "the object has been modified") {
		t.Fatalf("unbounded or silent rollback: %+v, %v", result, err)
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

func TestPlanExecutorReconcilesPendingWriteDespiteUnknownServerNormalization(t *testing.T) {
	client, ref := newServerPlanClient(deploymentWithDrift("registry.invalid/rate:missing"))
	// A mutating webhook rewrites a field the local canonicalizer knows nothing about.
	client.afterPatch = func(object *unstructured.Unstructured) {
		containers, _, _ := unstructured.NestedSlice(object.Object, "spec", "template", "spec", "containers")
		containers[0].(map[string]any)["image"] = "mirror.invalid/rate:good"
		_ = unstructured.SetNestedSlice(object.Object, containers, "spec", "template", "spec", "containers")
	}
	source := planFakeSource{source: deploymentWithDrift("registry.invalid/rate:good")}
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
	if err != nil || !reconciled.Applied || reconciled.PendingAction != nil || len(reconciled.ActionsApplied) != 1 {
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

func TestPlanExecutorReturnsWriteConflictWithoutRetry(t *testing.T) {
	client, ref := newServerPlanClient(deploymentWithDrift("registry.invalid/rate:missing"))
	client.raceBeforePatch = 1
	source := planFakeSource{source: deploymentWithDrift("registry.invalid/rate:good")}
	executor := PlanExecutor{Client: client, Source: source, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
	result, err := executor.Apply(context.Background(), IncidentRequest{RepairPolicy: "recorded-actions", SourceCommit: "accepted"}, reconcilePlan(ref), false)
	if err == nil || !apierrors.IsConflict(errors.Unwrap(err)) || result.Applied {
		t.Fatalf("a write conflict must be returned, not retried: %+v, %v", result, err)
	}
	image, _ := testField(client.objects[ref], "spec.template.spec.containers.0.image")
	if image != "registry.invalid/rate:missing" {
		t.Fatalf("image = %v", image)
	}
}

func TestPlanExecutorKeepsAppliedPlanWhenRolloutOrHealthFails(t *testing.T) {
	for name, executor := range map[string]func(*serverPlanClient, planFakeSource) PlanExecutor{
		"rollout": func(c *serverPlanClient, s planFakeSource) PlanExecutor {
			return PlanExecutor{Client: c, Source: s, Rollouts: callbackRollout{wait: func() error { return errors.New("not ready") }}, Verifier: planFakeVerifier{passed: true}}
		},
		"health": func(c *serverPlanClient, s planFakeSource) PlanExecutor {
			return PlanExecutor{Client: c, Source: s, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: false}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			client, ref := newServerPlanClient(deploymentWithDrift("registry.invalid/rate:missing"))
			source := planFakeSource{source: deploymentWithDrift("registry.invalid/rate:good")}
			result, _ := executor(client, source).Apply(context.Background(), IncidentRequest{RepairPolicy: "recorded-actions", SourceCommit: "accepted"}, reconcilePlan(ref), false)
			image, _ := testField(client.objects[ref], "spec.template.spec.containers.0.image")
			if !result.Applied || result.RolledBack || result.VerificationPassed || image != "registry.invalid/rate:good" {
				t.Fatalf("the executor reports and leaves the decision to the caller: %+v image=%v", result, image)
			}
		})
	}
}

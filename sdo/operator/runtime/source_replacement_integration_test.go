//go:build integration

package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"sdo.dev/operator/runtime/envtestio"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// replacementSource serves one accepted Deployment manifest.
type replacementSource struct{ manifest *unstructured.Unstructured }

func (s replacementSource) AcceptedManifest(_ context.Context, _ string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if ref.Kind != s.manifest.GetKind() || ref.Name != s.manifest.GetName() {
		return nil, ErrAcceptedManifestAbsent
	}
	return s.manifest.DeepCopy(), nil
}

func (replacementSource) Revision(context.Context, sdk.ObjectRef, string) (*unstructured.Unstructured, error) {
	return nil, ErrAcceptedManifestAbsent
}

// acceptedWeb is a synthetic accepted Deployment: a TCP readiness probe, two
// declared env entries and declared args.
func acceptedWeb(namespace string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "web", "namespace": namespace, "labels": map[string]any{"app": "web"}},
		"spec": map[string]any{
			"replicas": int64(1),
			"selector": map[string]any{"matchLabels": map[string]any{"app": "web"}},
			"template": map[string]any{
				"metadata": map[string]any{"labels": map[string]any{"app": "web"}},
				"spec": map[string]any{"containers": []any{map[string]any{
					"name": "web", "image": "registry.synthetic.invalid/web:1", "args": []any{"--serve"},
					"env":            []any{map[string]any{"name": "MODE", "value": "a"}, map[string]any{"name": "LEVEL", "value": "1"}},
					"ports":          []any{map[string]any{"containerPort": int64(8080)}},
					"readinessProbe": map[string]any{"tcpSocket": map[string]any{"port": int64(8080)}},
				}}},
			},
		},
	}}
}

// The controller's source comparison is what the object would be if replaced
// by accepted source, never an apply merged into the live object: a handler
// swap, a foreign list entry, a removed declared field and a field added after
// acceptance are drift; server defaults, values present at acceptance and an
// autoscaler's scale are not, and a scale with no autoscaler stays drift. Real
// API server (envtest); the restore SDO plans for a configuration departure
// returns the object to no drift.
func TestSourceComparisonIsAReplacementByAcceptedSource(t *testing.T) {
	if testing.Short() {
		t.Skip("starts an API server (a few seconds); nightly CI runs it without -short")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	env := envtestio.Start(t)
	client := env.Client(t)
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}
	source := acceptedWeb("app")
	env.CreateNamespace(t, "app")

	scaleTo3 := func(t *testing.T, client kubernetes.Interface, namespace string) {
		scale := &autoscalingv1.Scale{ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: namespace}, Spec: autoscalingv1.ScaleSpec{Replicas: 3}}
		if _, err := client.AppsV1().Deployments(namespace).UpdateScale(ctx, "web", scale, metav1.UpdateOptions{FieldManager: "synthetic-autoscaler"}); err != nil {
			t.Fatal(err)
		}
	}
	none := func(*testing.T, kubernetes.Interface, string) {}
	extraEnv := jsonPatch(`[{"op":"add","path":"/spec/template/spec/containers/0/env/-","value":{"name":"EXTRA","value":"x"}}]`)
	resources := jsonPatch(`[{"op":"add","path":"/spec/template/spec/containers/0/resources","value":{"limits":{"cpu":"500m","memory":"64Mi"}}}]`)
	for name, tc := range map[string]struct {
		// before runs ahead of acceptance, fault after it.
		before, fault func(t *testing.T, client kubernetes.Interface, namespace string)
		autoscaled    bool
		drifted       bool
		// restored: the built-in restore returns the object to no drift.
		restored bool
		// keepsAccepted: the restored object still has the env entry and the
		// resources written before acceptance.
		keepsAccepted bool
		// replicas, when set, is the replica count the restore leaves.
		replicas int32
	}{
		"server defaults only":                         {before: none, fault: none},
		"a value another writer set before acceptance": {before: jsonPatch(`[{"op":"add","path":"/spec/minReadySeconds","value":5}]`), fault: none},
		"an env entry present at acceptance":           {before: extraEnv, fault: none},
		"container resources present at acceptance":    {before: resources, fault: none},
		"a restore keeps list entries present at acceptance": {before: func(t *testing.T, client kubernetes.Interface, namespace string) {
			extraEnv(t, client, namespace)
			resources(t, client, namespace)
		}, drifted: true, restored: true, keepsAccepted: true, fault: jsonPatch(`[{"op":"replace","path":"/spec/template/spec/containers/0/readinessProbe","value":{"httpGet":{"path":"/ready","port":8080}}}]`)},
		"scaled by an autoscaler":      {before: none, fault: scaleTo3, autoscaled: true, restored: true, replicas: 3},
		"scaled with no autoscaler":    {before: none, fault: scaleTo3, drifted: true, restored: true, replicas: 1},
		"foreign env entry":            {before: none, drifted: true, restored: true, fault: jsonPatch(`[{"op":"add","path":"/spec/template/spec/containers/0/env/-","value":{"name":"TIMEOUT","value":"-30"}}]`)},
		"swapped probe handler":        {before: none, drifted: true, restored: true, fault: jsonPatch(`[{"op":"replace","path":"/spec/template/spec/containers/0/readinessProbe","value":{"httpGet":{"path":"/ready","port":8080}}}]`)},
		"removed declared field":       {before: none, drifted: true, restored: true, fault: jsonPatch(`[{"op":"remove","path":"/spec/template/spec/containers/0/args"}]`)},
		"field added after acceptance": {before: none, drifted: true, restored: true, fault: jsonPatch(`[{"op":"add","path":"/spec/minReadySeconds","value":5}]`)},
	} {
		t.Run(name, func(t *testing.T) {
			namespace := "app"
			t.Cleanup(func() {
				_ = client.AppsV1().Deployments(namespace).Delete(context.Background(), "web", metav1.DeleteOptions{})
				_ = client.AutoscalingV2().HorizontalPodAutoscalers(namespace).Delete(context.Background(), "web", metav1.DeleteOptions{})
				waitDeploymentGone(t, client, namespace)
			})
			if tc.autoscaled {
				minReplicas := int32(1)
				hpa := &autoscalingv2.HorizontalPodAutoscaler{
					ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: namespace},
					Spec:       autoscalingv2.HorizontalPodAutoscalerSpec{MinReplicas: &minReplicas, MaxReplicas: 5, ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "Deployment", Name: "web"}},
				}
				if _, err := client.AutoscalingV2().HorizontalPodAutoscalers(namespace).Create(ctx, hpa, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			resource, err := resolveAcceptedResource(client, ref)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := resource.apply(ctx, ref, source, false); err != nil {
				t.Fatal(err)
			}
			tc.before(t, client, namespace)
			accept(t, ctx, client, resource, ref, source)
			tc.fault(t, client, namespace)

			diff, err := declaredSourceDiff(liveView(t, ctx, client, source), ref)
			if err != nil {
				t.Fatalf("compare: %v", err)
			}
			if diff.Changed != tc.drifted {
				t.Fatalf("Changed = %v, want %v\n%s", diff.Changed, tc.drifted, diff.Text)
			}
			if !tc.restored {
				return
			}
			// The built-in restore writes the object the comparison compared with.
			live := readUnstructured(t, ctx, resource, ref)
			request := IncidentRequest{Namespace: namespace, SourceCommit: liveViewCommit}
			next, err := (PlanExecutor{Source: replacementSource{source}, Client: TypedPlanClient{Client: client}}).prepareAction(ctx, request, program.ReconcileFromSource{Ref: ref}, live)
			if err != nil {
				t.Fatal(err)
			}
			body, err := json.Marshal(next.Object)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.AppsV1().Deployments(namespace).Update(ctx, decodeDeployment(t, body), metav1.UpdateOptions{FieldManager: "sdo-responder"}); err != nil {
				t.Fatalf("restore: %v", err)
			}
			restored, err := declaredSourceDiff(liveView(t, ctx, client, source), ref)
			if err != nil || restored.Changed {
				t.Fatalf("after restore Changed = %v (%v)\n%s", restored.Changed, err, restored.Text)
			}
			after, err := client.AppsV1().Deployments(namespace).Get(ctx, "web", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if tc.replicas != 0 && *after.Spec.Replicas != tc.replicas {
				t.Fatalf("restore left replicas %d, want %d", *after.Spec.Replicas, tc.replicas)
			}
			if tc.keepsAccepted {
				container := after.Spec.Template.Spec.Containers[0]
				if len(container.Env) != 3 || container.Env[2].Name != "EXTRA" || container.Resources.Limits.Cpu().String() != "500m" {
					t.Fatalf("restore dropped fields present at acceptance: env %v resources %v", container.Env, container.Resources)
				}
			}
		})
	}
}

func jsonPatch(patch string) func(*testing.T, kubernetes.Interface, string) {
	return func(t *testing.T, client kubernetes.Interface, namespace string) {
		if _, err := client.AppsV1().Deployments(namespace).Patch(context.Background(), "web", types.JSONPatchType, []byte(patch), metav1.PatchOptions{FieldManager: "kubectl-patch"}); err != nil {
			t.Fatal(err)
		}
	}
}

// accept stamps the acceptance record SDO's adoption takes over the object.
func accept(t *testing.T, ctx context.Context, client kubernetes.Interface, resource acceptedResource, ref sdk.ObjectRef, source *unstructured.Unstructured) {
	t.Helper()
	live := readUnstructured(t, ctx, resource, ref)
	record, fits, err := buildAcceptanceRecord(source, live, live, nil, false)
	if err != nil || !fits {
		t.Fatalf("acceptance record: fits=%v err=%v", fits, err)
	}
	if err := resource.recordInstanceAcceptance(ctx, ref, record, live); err != nil {
		t.Fatal(err)
	}
}

func readUnstructured(t *testing.T, ctx context.Context, resource acceptedResource, ref sdk.ObjectRef) *unstructured.Unstructured {
	t.Helper()
	live, err := resource.get(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	return live
}

// liveView reads the object as the controller's snapshot holds it: the
// server's JSON form, managed fields included.
func liveView(t *testing.T, ctx context.Context, client kubernetes.Interface, source *unstructured.Unstructured) LiveReadView {
	t.Helper()
	ref := sdk.ObjectRef{APIVersion: source.GetAPIVersion(), Kind: source.GetKind(), Namespace: source.GetNamespace(), Name: source.GetName()}
	resource, err := resolveAcceptedResource(client, ref)
	if err != nil {
		t.Fatal(err)
	}
	live := readUnstructured(t, ctx, resource, ref)
	hpas, err := client.AutoscalingV2().HorizontalPodAutoscalers(ref.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := sdktest.Snapshot{NamespaceName: source.GetNamespace(), Objects: []unstructured.Unstructured{*live}, HPAList: hpas.Items}
	return LiveReadView{Carrier: inputs.New(snapshot), Context: ctx, Client: client, Source: replacementSource{source}, SourceCommit: liveViewCommit}
}

func decodeDeployment(t *testing.T, body []byte) *appsv1.Deployment {
	t.Helper()
	var deployment appsv1.Deployment
	if err := json.Unmarshal(body, &deployment); err != nil {
		t.Fatal(err)
	}
	return &deployment
}

func waitDeploymentGone(t *testing.T, client kubernetes.Interface, namespace string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := client.AppsV1().Deployments(namespace).Get(context.Background(), "web", metav1.GetOptions{}); err != nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// A custom resource compares like a built-in object: its API rejects an update
// without a resourceVersion, so the dry run sends the snapshot's and retries a
// stale one. Real API server (envtest).
func TestSourceComparisonOfACustomResource(t *testing.T) {
	if testing.Short() {
		t.Skip("starts an API server (a few seconds); nightly CI runs it without -short")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	env := envtestio.Start(t)
	client := env.Client(t)
	env.CreateNamespace(t, "app")
	installWidgetCRD(t, ctx, client)
	ref := sdk.ObjectRef{APIVersion: "synthetic.example.com/v1", Kind: "Widget", Namespace: "app", Name: "web"}
	source := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "synthetic.example.com/v1", "kind": "Widget",
		"metadata": map[string]any{"name": "web", "namespace": "app"},
		"spec":     map[string]any{"size": int64(1), "items": []any{"x"}},
	}}
	resource, err := resolveAcceptedResource(client, ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resource.apply(ctx, ref, source, false); err != nil {
		t.Fatal(err)
	}
	accept(t, ctx, client, resource, ref, source)

	view := liveView(t, ctx, client, source)
	// A write after the snapshot was read: the snapshot's resourceVersion is stale.
	if _, err := bodyOf(ctx, resource.client.Patch(types.MergePatchType).AbsPath(resource.path).Body([]byte(`{"status":{"phase":"ready"}}`))); err != nil {
		t.Fatal(err)
	}
	diff, err := declaredSourceDiff(view, ref)
	if err != nil || diff.Changed {
		t.Fatalf("accepted custom resource: Changed = %v (%v)\n%s", diff.Changed, err, diff.Text)
	}

	if _, err := bodyOf(ctx, resource.client.Patch(types.JSONPatchType).AbsPath(resource.path).Body([]byte(`[{"op":"add","path":"/spec/items/-","value":"foreign"}]`))); err != nil {
		t.Fatal(err)
	}
	diff, err = declaredSourceDiff(liveView(t, ctx, client, source), ref)
	if err != nil || !diff.Changed {
		t.Fatalf("foreign list entry: Changed = %v (%v)\n%s", diff.Changed, err, diff.Text)
	}
	live := readUnstructured(t, ctx, resource, ref)
	request := IncidentRequest{Namespace: "app", SourceCommit: liveViewCommit}
	next, err := (PlanExecutor{Source: replacementSource{source}, Client: TypedPlanClient{Client: client}}).prepareAction(ctx, request, program.ReconcileFromSource{Ref: ref}, live)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(next.Object)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bodyOf(ctx, resource.client.Put().AbsPath(resource.path).Body(body)); err != nil {
		t.Fatalf("restore: %v", err)
	}
	restored, err := declaredSourceDiff(liveView(t, ctx, client, source), ref)
	if err != nil || restored.Changed {
		t.Fatalf("after restore Changed = %v (%v)\n%s", restored.Changed, err, restored.Text)
	}
}

// A departure the API server cannot replace (an immutable field written after
// acceptance) is a comparison error, and the responder sees it as one: the
// object is listed as not compared, with the reason and no restore command.
func TestAnImmutableDepartureIsAVisibleComparisonError(t *testing.T) {
	if testing.Short() {
		t.Skip("starts an API server (a few seconds); nightly CI runs it without -short")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	env := envtestio.Start(t)
	client := env.Client(t)
	env.CreateNamespace(t, "app")
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "PersistentVolumeClaim", Namespace: "app", Name: "data"}
	source := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "PersistentVolumeClaim",
		"metadata": map[string]any{"name": "data", "namespace": "app"},
		"spec": map[string]any{
			"accessModes": []any{"ReadWriteOnce"}, "storageClassName": "synthetic",
			"resources": map[string]any{"requests": map[string]any{"storage": "1Gi"}},
		},
	}}
	resource, err := resolveAcceptedResource(client, ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resource.apply(ctx, ref, source, false); err != nil {
		t.Fatal(err)
	}
	accept(t, ctx, client, resource, ref, source)
	if _, err := client.CoreV1().PersistentVolumeClaims("app").Patch(ctx, "data", types.MergePatchType, []byte(`{"spec":{"volumeName":"synthetic-volume"}}`), metav1.PatchOptions{FieldManager: "kubectl-patch"}); err != nil {
		t.Fatal(err)
	}
	view := liveView(t, ctx, client, source)
	_, err = declaredSourceDiff(view, ref)
	var refused *SourceComparisonRefusedError
	if !errors.As(err, &refused) {
		t.Fatalf("compare = %v, want a refused source comparison", err)
	}
	departures := SourceDepartures(DepartureInput{View: view}, []sdk.ObjectRef{ref})
	if len(departures) != 1 || departures[0].Kind != DepartureUncompared || departures[0].Command() != "" || !strings.Contains(departures[0].Text(), "could not be compared") {
		t.Fatalf("departures = %+v", departures)
	}
}

// installWidgetCRD serves a synthetic namespaced custom resource.
func installWidgetCRD(t *testing.T, ctx context.Context, client kubernetes.Interface) {
	t.Helper()
	open := map[string]any{"type": "object", "x-kubernetes-preserve-unknown-fields": true}
	crd := map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
		"metadata": map[string]any{"name": "widgets.synthetic.example.com"},
		"spec": map[string]any{
			"group": "synthetic.example.com", "scope": "Namespaced",
			"names": map[string]any{"plural": "widgets", "singular": "widget", "kind": "Widget", "listKind": "WidgetList"},
			"versions": []any{map[string]any{"name": "v1", "served": true, "storage": true, "schema": map[string]any{"openAPIV3Schema": map[string]any{
				"type": "object", "properties": map[string]any{"spec": open, "status": open},
			}}}},
		},
	}
	body, err := json.Marshal(crd)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bodyOf(ctx, client.Discovery().RESTClient().Post().AbsPath("/apis/apiextensions.k8s.io/v1/customresourcedefinitions").Body(body)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := resolveAcceptedResource(client, sdk.ObjectRef{APIVersion: "synthetic.example.com/v1", Kind: "Widget", Namespace: "app", Name: "probe"}); err == nil {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("custom resource was never served")
}

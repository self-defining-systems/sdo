package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func addedFieldsRef() sdk.ObjectRef {
	return sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "search"}
}

func addedFieldsSource() *unstructured.Unstructured {
	ref := addedFieldsRef()
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": ref.APIVersion, "kind": ref.Kind,
		"metadata": map[string]any{"namespace": ref.Namespace, "name": ref.Name, "labels": map[string]any{"app": "search"}},
		"spec": map[string]any{
			"replicas": int64(1),
			"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{
				"name": "search", "image": "search:1", "ports": []any{map[string]any{"containerPort": int64(8080)}},
			}}}},
		},
	}}
}

// addedFieldsLive is the accepted source as the API server stores it: server
// defaults filled in and accepted fields owned by the deployer's Apply.
func addedFieldsLive() *unstructured.Unstructured {
	live := addedFieldsSource()
	_ = unstructured.SetNestedField(live.Object, int64(600), "spec", "progressDeadlineSeconds")
	_ = unstructured.SetNestedField(live.Object, "ClusterFirst", "spec", "template", "spec", "dnsPolicy")
	containers, _, _ := unstructured.NestedSlice(live.Object, "spec", "template", "spec", "containers")
	container := containers[0].(map[string]any)
	container["terminationMessagePath"] = "/dev/termination-log"
	container["imagePullPolicy"] = "IfNotPresent"
	container["ports"].([]any)[0].(map[string]any)["protocol"] = "TCP"
	_ = unstructured.SetNestedSlice(live.Object, containers, "spec", "template", "spec", "containers")
	_ = unstructured.SetNestedField(live.Object, int64(1), "status", "replicas")
	acceptAddedFieldsLive(live)
	return live
}

// acceptAddedFieldsLive records the acceptance point the way adoption does:
// the record lists every path the accepted object carried.
func acceptAddedFieldsLive(live *unstructured.Unstructured) {
	record, fits, err := buildAcceptanceRecord(addedFieldsSource(), live, live, false)
	if err != nil || !fits {
		panic(fmt.Sprintf("acceptance record: %v %v", fits, err))
	}
	annotations := live.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[acceptedPathsAnnotation] = record
	live.SetAnnotations(annotations)
}

func addAnnotation(live *unstructured.Unstructured, key, value string) {
	annotations := live.GetAnnotations()
	annotations[key] = value
	live.SetAnnotations(annotations)
}

func addedFieldsContainers(object *unstructured.Unstructured) []any {
	containers, _, _ := unstructured.NestedSlice(object.Object, "spec", "template", "spec", "containers")
	return containers
}

// diffEveryView compares without a client, with a server-side apply dry-run
// that (like the API server) keeps fields it does not declare, and offline on
// the corpus capture of the same object, as replay does. Live and replay must
// agree, so the acceptance evidence has to survive capture.
func diffEveryView(t *testing.T, live *unstructured.Unstructured) map[string]bool {
	t.Helper()
	results := map[string]bool{}
	for name, result := range diffEveryViewResults(t, live) {
		if result.err != nil {
			t.Fatalf("%s: %v", name, result.err)
		}
		results[name] = result.changed
	}
	return results
}

type viewResult struct {
	changed bool
	err     error
}

func diffEveryViewResults(t *testing.T, live *unstructured.Unstructured) map[string]viewResult {
	t.Helper()
	ref := addedFieldsRef()
	// Read the object back as the API server serves it.
	var typed appsv1.Deployment
	if err := kruntime.DefaultUnstructuredConverter.FromUnstructured(live.Object, &typed); err != nil {
		t.Fatal(err)
	}
	content, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(&typed)
	if err != nil {
		t.Fatal(err)
	}
	live = &unstructured.Unstructured{Object: content}
	live.SetAPIVersion(ref.APIVersion)
	live.SetKind(ref.Kind)
	snapshot := sdktest.Snapshot{NamespaceName: ref.Namespace, Objects: []unstructured.Unstructured{*live}}
	captured, err := sdktest.CaptureSnapshot(snapshot, ref.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(captured)
	if err != nil {
		t.Fatal(err)
	}
	var stored sdktest.Snapshot
	if err := json.Unmarshal(encoded, &stored); err != nil {
		t.Fatal(err)
	}
	results := map[string]viewResult{}
	for _, name := range []string{"snapshot", "apply-dry-run", "replayed-capture"} {
		view := LiveReadView{DetectionContext: snapshot, Source: planFakeSource{source: addedFieldsSource()}, SourceCommit: "accepted"}
		switch name {
		case "apply-dry-run":
			client := fake.NewSimpleClientset()
			client.PrependReactor("patch", "deployments", func(ktesting.Action) (bool, kruntime.Object, error) {
				return true, typed.DeepCopy(), nil
			})
			view.Client = bridgeFake(t, client)
		case "replayed-capture":
			view.DetectionContext = stored
		}
		diff, err := view.DiffAgainstSource(ref)
		results[name] = viewResult{changed: diff.Changed, err: err}
	}
	return results
}

func setHostPort(live *unstructured.Unstructured) {
	containers := addedFieldsContainers(live)
	containers[0].(map[string]any)["ports"].([]any)[0].(map[string]any)["hostPort"] = int64(8080)
	_ = unstructured.SetNestedSlice(live.Object, containers, "spec", "template", "spec", "containers")
}

func TestSourceDiffChangedForFieldsAddedAfterAcceptance(t *testing.T) {
	cases := map[string]func(*unstructured.Unstructured){
		"container hostPort": setHostPort,
		"sidecar container": func(live *unstructured.Unstructured) {
			containers := append(addedFieldsContainers(live), map[string]any{"name": "proxy", "image": "proxy:1", "terminationMessagePath": "/dev/termination-log"})
			_ = unstructured.SetNestedSlice(live.Object, containers, "spec", "template", "spec", "containers")
		},
		"annotation": func(live *unstructured.Unstructured) { addAnnotation(live, "example.com/maintenance", "true") },
		"label": func(live *unstructured.Unstructured) {
			live.SetLabels(map[string]string{"app": "search", "tier": "canary"})
		},
		"custom finalizer": func(live *unstructured.Unstructured) { live.SetFinalizers([]string{"example.com/hold"}) },
		"pod template field": func(live *unstructured.Unstructured) {
			_ = unstructured.SetNestedStringMap(live.Object, map[string]string{"disk": "ssd"}, "spec", "template", "spec", "nodeSelector")
		},
		// A later acceptance (a source repair re-applied by SDO) keeps an
		// earlier addition visible instead of absorbing it.
		"addition survives re-acceptance": func(live *unstructured.Unstructured) {
			setHostPort(live)
			acceptAddedFieldsLive(live)
		},
	}
	for name, inject := range cases {
		t.Run(name, func(t *testing.T) {
			live := addedFieldsLive()
			inject(live)
			for view, changed := range diffEveryView(t, live) {
				if !changed {
					t.Errorf("%s: added field drift reported unchanged", view)
				}
			}
		})
	}
}

func TestSourceDiffUnchangedForServerDefaultsAndBookkeeping(t *testing.T) {
	cases := map[string]func(*unstructured.Unstructured){
		"applied defaults": func(*unstructured.Unstructured) {},
		// Adopted objects: whatever the creator wrote before SDO accepted
		// the object is part of the acceptance record, however often that
		// creator writes again afterwards.
		"adopted creator fields": func(live *unstructured.Unstructured) {
			annotations := live.GetAnnotations()
			delete(annotations, acceptedPathsAnnotation)
			live.SetAnnotations(annotations)
			_ = unstructured.SetNestedField(live.Object, int64(30), "spec", "template", "spec", "terminationGracePeriodSeconds")
			addAnnotation(live, "meta.helm.sh/release-name", "search")
			acceptAddedFieldsLive(live)
		},
		"daemonset controller generation": func(live *unstructured.Unstructured) {
			addAnnotation(live, "deprecated.daemonset.template.generation", "2")
		},
		"kubernetes bookkeeping annotations": func(live *unstructured.Unstructured) {
			addAnnotation(live, "deployment.kubernetes.io/revision", "3")
			_ = unstructured.SetNestedStringMap(live.Object, map[string]string{"kubectl.kubernetes.io/restartedAt": "2026-10-26T12:00:00Z"}, "spec", "template", "metadata", "annotations")
		},
		"declared field rewritten to its accepted value": func(live *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(live.Object, int64(1), "spec", "replicas")
		},
		"kubernetes finalizer": func(live *unstructured.Unstructured) {
			live.SetFinalizers([]string{"service.kubernetes.io/load-balancer-cleanup"})
		},
		"status written by a controller": func(live *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(live.Object, int64(1), "status", "readyReplicas")
		},
	}
	for name, prepare := range cases {
		t.Run(name, func(t *testing.T) {
			live := addedFieldsLive()
			prepare(live)
			for view, changed := range diffEveryView(t, live) {
				if changed {
					t.Errorf("%s: unchanged object reported as drift", view)
				}
			}
		})
	}
}

// Without an acceptance record the additions are unknown: every view fails
// closed instead of reporting the object unchanged.
func TestSourceDiffWithoutAcceptanceRecordFailsClosed(t *testing.T) {
	live := addedFieldsLive()
	annotations := live.GetAnnotations()
	delete(annotations, acceptedPathsAnnotation)
	live.SetAnnotations(annotations)
	setHostPort(live)
	for view, result := range diffEveryViewResults(t, live) {
		if !errors.Is(result.err, ErrAcceptanceRecordUnavailable) {
			t.Errorf("%s: missing acceptance record compared as known: %v", view, result.err)
		}
	}
}

// Detection, repair and closure agree: every addition Changed reports is
// removed by accepted-source reconciliation, and the same comparison then
// reports the object restored. Map additions (labels, selectors) included.
func TestReconcileFromSourceClearsAddedFieldDrift(t *testing.T) {
	cases := map[string]func(*unstructured.Unstructured){
		"container hostPort": setHostPort,
		"annotation":         func(live *unstructured.Unstructured) { addAnnotation(live, "example.com/maintenance", "true") },
		"label": func(live *unstructured.Unstructured) {
			live.SetLabels(map[string]string{"app": "search", "tier": "canary"})
		},
		"node selector": func(live *unstructured.Unstructured) {
			_ = unstructured.SetNestedStringMap(live.Object, map[string]string{"disk": "ssd"}, "spec", "template", "spec", "nodeSelector")
		},
		"pod template label": func(live *unstructured.Unstructured) {
			_ = unstructured.SetNestedStringMap(live.Object, map[string]string{"canary": "true"}, "spec", "template", "metadata", "labels")
		},
	}
	for name, inject := range cases {
		t.Run(name, func(t *testing.T) {
			ref := addedFieldsRef()
			live := addedFieldsLive()
			inject(live)
			client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: live}}
			diff := func() bool {
				t.Helper()
				snapshot := sdktest.Snapshot{NamespaceName: ref.Namespace, Objects: []unstructured.Unstructured{*client.objects[ref].DeepCopy()}}
				result, err := (LiveReadView{DetectionContext: snapshot, Source: planFakeSource{source: addedFieldsSource()}, SourceCommit: "accepted"}).DiffAgainstSource(ref)
				if err != nil {
					t.Fatal(err)
				}
				return result.Changed
			}
			if !diff() {
				t.Fatal("injected addition not reported as source drift")
			}
			executor := PlanExecutor{Client: client, Source: planFakeSource{source: addedFieldsSource()}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
			result, err := executor.Apply(context.Background(), IncidentRequest{SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: ref}}}, false)
			if err != nil || !result.Applied {
				t.Fatalf("reconcile: %+v %v", result, err)
			}
			if diff() {
				t.Fatalf("reconciled object still reported as drift: %v %v", client.objects[ref].Object["metadata"], client.objects[ref].Object["spec"])
			}
			if _, ok := readAcceptanceRecord(client.objects[ref]); !ok {
				t.Fatal("reconciliation removed the acceptance record")
			}
		})
	}
}

// withAcceptance returns object's annotations plus the acceptance record that
// adoption would take against source over the object as it is now.
func withAcceptance(t testing.TB, object any, source *unstructured.Unstructured) map[string]string {
	t.Helper()
	annotations, err := acceptanceAnnotations(object, source)
	if err != nil {
		t.Fatal(err)
	}
	return annotations
}

func acceptanceAnnotations(object any, source *unstructured.Unstructured) (map[string]string, error) {
	live, ok := object.(*unstructured.Unstructured)
	if !ok {
		content, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(object)
		if err != nil {
			return nil, err
		}
		live = &unstructured.Unstructured{Object: content}
	}
	live = live.DeepCopy()
	if err := RecordAcceptance(live, source); err != nil {
		return nil, err
	}
	return live.GetAnnotations(), nil
}

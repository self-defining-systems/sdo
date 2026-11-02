//go:build integration

package runtime

import (
	"encoding/json"
	"errors"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/sdktest"
)

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
		view := LiveReadView{Carrier: inputs.New(snapshot), Source: planFakeSource{source: addedFieldsSource()}, SourceCommit: "accepted"}
		switch name {
		case "apply-dry-run":
			client := fake.NewSimpleClientset()
			client.PrependReactor("patch", "deployments", func(ktesting.Action) (bool, kruntime.Object, error) {
				return true, typed.DeepCopy(), nil
			})
			client.PrependReactor("create", "deployments", func(ktesting.Action) (bool, kruntime.Object, error) {
				// Isolated source render has API defaults but no live additions.
				var rendered appsv1.Deployment
				if err := kruntime.DefaultUnstructuredConverter.FromUnstructured(addedFieldsLive().Object, &rendered); err != nil {
					t.Fatal(err)
				}
				return true, &rendered, nil
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


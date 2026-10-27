package runtime

import (
	"encoding/json"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

func noiseSource() *unstructured.Unstructured {
	source := addedFieldsSource()
	containers := addedFieldsContainers(source)
	c := containers[0].(map[string]any)
	c["env"] = []any{map[string]any{"name": "MODE", "value": "stable"}}
	c["resources"] = map[string]any{"requests": map[string]any{"cpu": "1000m", "memory": "1Gi"}, "limits": map[string]any{"cpu": "2", "memory": "2Gi"}}
	_ = unstructured.SetNestedSlice(source.Object, containers, "spec", "template", "spec", "containers")
	source.SetAnnotations(map[string]string{"deployment.kubernetes.io/revision": "1"})
	return source
}

func noiseContainer(o *unstructured.Unstructured) map[string]any {
	return o.Object["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)
}

func noiseDiff(t *testing.T, source, live *unstructured.Unstructured, hpas []autoscalingv2.HorizontalPodAutoscaler) sdk.SourceDiff {
	t.Helper()
	snapshot := sdktest.Snapshot{NamespaceName: "app", Objects: []unstructured.Unstructured{*live}, HPAList: hpas}
	captured, err := sdktest.CaptureSnapshot(snapshot, "app")
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(captured)
	if err != nil {
		t.Fatal(err)
	}
	var stored sdktest.Snapshot
	if err := json.Unmarshal(data, &stored); err != nil {
		t.Fatal(err)
	}
	diff, err := (LiveReadView{DetectionContext: stored, Source: planFakeSource{source: source}, SourceCommit: "accepted"}).DiffAgainstSource(sdk.ObjectRef{APIVersion: source.GetAPIVersion(), Kind: source.GetKind(), Namespace: source.GetNamespace(), Name: source.GetName()})
	if err != nil {
		t.Fatal(err)
	}
	return diff
}

func TestSourceComparisonNoiseAndRealChanges(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mutate  func(*unstructured.Unstructured)
		changed bool
	}{
		{"revision bump", func(o *unstructured.Unstructured) { addAnnotation(o, "deployment.kubernetes.io/revision", "9") }, false},
		{"equivalent quantities", func(o *unstructured.Unstructured) {
			c := noiseContainer(o)
			r := c["resources"].(map[string]any)
			r["requests"] = map[string]any{"cpu": "1", "memory": "1024Mi"}
			r["limits"] = map[string]any{"cpu": "2000m", "memory": "2147483648"}
		}, false},
		{"image", func(o *unstructured.Unstructured) { noiseContainer(o)["image"] = "search:broken" }, true},
		{"env", func(o *unstructured.Unstructured) {
			noiseContainer(o)["env"].([]any)[0].(map[string]any)["value"] = "broken"
		}, true},
		{"resource value", func(o *unstructured.Unstructured) {
			noiseContainer(o)["resources"].(map[string]any)["requests"].(map[string]any)["cpu"] = "2"
		}, true},
		{"probe", func(o *unstructured.Unstructured) {
			noiseContainer(o)["readinessProbe"] = map[string]any{"httpGet": map[string]any{"path": "/bad", "port": int64(8080)}}
		}, true},
		{"affinity", func(o *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(o.Object, map[string]any{"nodeAffinity": map[string]any{"requiredDuringSchedulingIgnoredDuringExecution": map[string]any{"nodeSelectorTerms": []any{map[string]any{"matchExpressions": []any{map[string]any{"key": "disk", "operator": "In", "values": []any{"ssd"}}}}}}}}, "spec", "template", "spec", "affinity")
		}, true},
		{"volumes", func(o *unstructured.Unstructured) {
			_ = unstructured.SetNestedSlice(o.Object, []any{map[string]any{"name": "data", "emptyDir": map[string]any{}}}, "spec", "template", "spec", "volumes")
		}, true},
		{"numeric env remains text", func(o *unstructured.Unstructured) {
			noiseContainer(o)["env"].([]any)[0].(map[string]any)["value"] = "1000m"
		}, true},
		{"replicas without HPA", func(o *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(o.Object, int64(5), "spec", "replicas")
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := noiseSource()
			live := source.DeepCopy()
			if err := RecordAcceptance(live, source); err != nil {
				t.Fatal(err)
			}
			tc.mutate(live)
			if diff := noiseDiff(t, source, live, nil); diff.Changed != tc.changed {
				t.Fatalf("changed=%v want %v: %s", diff.Changed, tc.changed, diff.Text)
			}
		})
	}
}

func TestSourceComparisonHPATargetIdentity(t *testing.T) {
	for _, tc := range []struct {
		name, namespace, kind, version, target string
		changed                                bool
	}{
		{"matching", "app", "Deployment", "apps/v1", "search", false},
		{"other namespace", "other", "Deployment", "apps/v1", "search", true},
		{"other kind", "app", "StatefulSet", "apps/v1", "search", true},
		{"other API group", "app", "Deployment", "example.com/v1", "search", true},
		{"other name", "app", "Deployment", "apps/v1", "other", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := noiseSource()
			live := source.DeepCopy()
			if err := RecordAcceptance(live, source); err != nil {
				t.Fatal(err)
			}
			_ = unstructured.SetNestedField(live.Object, int64(5), "spec", "replicas")
			hpa := autoscalingv2.HorizontalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Namespace: tc.namespace, Name: "scale"}, Spec: autoscalingv2.HorizontalPodAutoscalerSpec{ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{APIVersion: tc.version, Kind: tc.kind, Name: tc.target}}}
			if diff := noiseDiff(t, source, live, []autoscalingv2.HorizontalPodAutoscaler{hpa}); diff.Changed != tc.changed {
				t.Fatalf("changed=%v want %v: %s", diff.Changed, tc.changed, diff.Text)
			}
		})
	}
}

// Ratchet: controller revision metadata is absent from the comparison itself,
// including when source declares an old revision, rather than merely hiding Changed.
func TestSourceComparisonRevisionNeverAppearsInDiff(t *testing.T) {
	source := noiseSource()
	live := source.DeepCopy()
	if err := RecordAcceptance(live, source); err != nil {
		t.Fatal(err)
	}
	addAnnotation(live, "deployment.kubernetes.io/revision", "20")
	diff := noiseDiff(t, source, live, nil)
	if diff.Changed || strings.Contains(diff.Text, "deployment.kubernetes.io/revision") {
		t.Fatalf("controller revision leaked into source diff: %+v", diff)
	}
}

// Existing records may hold pre-normalization quantity spelling for a default
// (for example a LimitRange-injected request). Equivalent spelling stays equal.
func TestSourceComparisonRecordedQuantityValueCompatibility(t *testing.T) {
	source := addedFieldsSource()
	live := source.DeepCopy()
	noiseContainer(live)["resources"] = map[string]any{"requests": map[string]any{"cpu": "1"}}
	if err := RecordAcceptance(live, source); err != nil {
		t.Fatal(err)
	}
	record, ok := readAcceptanceRecord(live)
	if !ok {
		t.Fatal("missing acceptance")
	}
	const path = "spec.template.spec.containers[0].resources.requests.cpu"
	record.Values[path] = "1000m"
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	annotations := live.GetAnnotations()
	annotations[acceptedPathsAnnotation] = string(data)
	live.SetAnnotations(annotations)
	if diff := noiseDiff(t, source, live, nil); diff.Changed {
		t.Fatalf("equivalent recorded quantity reported drift: %+v", diff)
	}
	noiseContainer(live)["resources"].(map[string]any)["requests"].(map[string]any)["cpu"] = "2"
	if diff := noiseDiff(t, source, live, nil); !diff.Changed {
		t.Fatal("changed recorded quantity hidden")
	}
}

func TestSourceComparisonDryRunIgnoresControllerAndQuantityNoise(t *testing.T) {
	source := noiseSource()
	live := source.DeepCopy()
	if err := RecordAcceptance(live, source); err != nil {
		t.Fatal(err)
	}
	addAnnotation(live, "deployment.kubernetes.io/revision", "4")
	noiseContainer(live)["resources"].(map[string]any)["requests"].(map[string]any)["cpu"] = "1"
	var actual, normalized appsv1.Deployment
	if err := kruntime.DefaultUnstructuredConverter.FromUnstructured(live.Object, &actual); err != nil {
		t.Fatal(err)
	}
	if err := kruntime.DefaultUnstructuredConverter.FromUnstructured(source.Object, &normalized); err != nil {
		t.Fatal(err)
	}
	client := fake.NewSimpleClientset()
	called := false
	client.PrependReactor("patch", "deployments", func(ktesting.Action) (bool, kruntime.Object, error) {
		called = true
		return true, normalized.DeepCopy(), nil
	})
	view := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app", DeploymentList: []appsv1.Deployment{actual}}, Source: planFakeSource{source: source}, SourceCommit: "accepted", Client: bridgeFake(t, client)}
	if diff, err := view.DiffAgainstSource(addedFieldsRef()); err != nil || diff.Changed || !called {
		t.Fatalf("dry run comparison: %+v %v called=%v", diff, err, called)
	}
}

func TestSourceComparisonEquivalentQuantitySpellingsInEnvAreDifferent(t *testing.T) {
	source := noiseSource()
	noiseContainer(source)["env"].([]any)[0].(map[string]any)["value"] = "1000m"
	live := source.DeepCopy()
	if err := RecordAcceptance(live, source); err != nil {
		t.Fatal(err)
	}
	noiseContainer(live)["env"].([]any)[0].(map[string]any)["value"] = "1"
	if diff := noiseDiff(t, source, live, nil); !diff.Changed {
		t.Fatal("env values were compared as quantities")
	}
}

func TestSourceComparisonQuantityFieldsAcrossWorkloads(t *testing.T) {
	for _, kind := range []string{"Pod", "Deployment", "StatefulSet", "DaemonSet", "ReplicaSet", "ReplicationController", "Job", "CronJob", "PersistentVolumeClaim", "PersistentVolume"} {
		t.Run(kind, func(t *testing.T) {
			source := noiseSource()
			source.SetKind(kind)
			switch kind {
			case "Pod", "PersistentVolumeClaim", "PersistentVolume", "ReplicationController":
				source.SetAPIVersion("v1")
			case "Job", "CronJob":
				source.SetAPIVersion("batch/v1")
			}
			if kind == "Pod" {
				template, _, _ := unstructured.NestedMap(source.Object, "spec", "template")
				source.Object["spec"] = template["spec"]
			}
			if kind == "CronJob" {
				source.Object["spec"] = map[string]any{"jobTemplate": map[string]any{"spec": source.Object["spec"]}}
			}
			quantity := func(value string) map[string]any { return map[string]any{"requests": map[string]any{"storage": value}} }
			if kind == "PersistentVolumeClaim" {
				source.Object["spec"] = map[string]any{"resources": quantity("1Gi")}
			}
			if kind == "PersistentVolume" {
				source.Object["spec"] = map[string]any{"capacity": map[string]any{"storage": "1Gi"}}
			}
			live := source.DeepCopy()
			if err := RecordAcceptance(live, source); err != nil {
				t.Fatal(err)
			}
			var podSpec map[string]any
			switch kind {
			case "Pod":
				podSpec = live.Object["spec"].(map[string]any)
			case "CronJob":
				podSpec = live.Object["spec"].(map[string]any)["jobTemplate"].(map[string]any)["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
			case "PersistentVolumeClaim":
				live.Object["spec"].(map[string]any)["resources"] = quantity("1024Mi")
			case "PersistentVolume":
				live.Object["spec"].(map[string]any)["capacity"] = map[string]any{"storage": "1073741824"}
			default:
				podSpec = live.Object["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
			}
			if podSpec != nil {
				podSpec["containers"].([]any)[0].(map[string]any)["resources"].(map[string]any)["requests"].(map[string]any)["memory"] = "1024Mi"
			}
			if diff := noiseDiff(t, source, live, nil); diff.Changed {
				t.Fatalf("equivalent quantity in %s: %+v", kind, diff)
			}
		})
	}
}

func TestSourceComparisonPreservesUnknownKubernetesAnnotationChanges(t *testing.T) {
	source := noiseSource()
	live := source.DeepCopy()
	if err := RecordAcceptance(live, source); err != nil {
		t.Fatal(err)
	}
	addAnnotation(live, "security.alpha.kubernetes.io/example", "changed")
	if diff := noiseDiff(t, source, live, nil); !diff.Changed {
		t.Fatal("unknown Kubernetes annotation was blanket-ignored")
	}
}

func TestSourceComparisonSchemaQuantityCases(t *testing.T) {
	for _, tc := range []struct {
		name    string
		prepare func(*unstructured.Unstructured)
		change  func(*unstructured.Unstructured)
		changed bool
	}{
		{"numeric YAML quantity", func(o *unstructured.Unstructured) {
			r := noiseContainer(o)["resources"].(map[string]any)["requests"].(map[string]any)
			r["cpu"] = int64(1)
			r["memory"] = int64(1073741824)
		}, func(o *unstructured.Unstructured) {
			r := noiseContainer(o)["resources"].(map[string]any)["requests"].(map[string]any)
			r["cpu"] = "1000m"
			r["memory"] = "1Gi"
		}, false},
		{"emptyDir size limit", func(o *unstructured.Unstructured) {
			_ = unstructured.SetNestedSlice(o.Object, []any{map[string]any{"name": "scratch", "emptyDir": map[string]any{"sizeLimit": "1Gi"}}}, "spec", "template", "spec", "volumes")
		}, func(o *unstructured.Unstructured) {
			v, _, _ := unstructured.NestedSlice(o.Object, "spec", "template", "spec", "volumes")
			v[0].(map[string]any)["emptyDir"].(map[string]any)["sizeLimit"] = "1024Mi"
			_ = unstructured.SetNestedSlice(o.Object, v, "spec", "template", "spec", "volumes")
		}, false},
		{"resourceFieldRef divisor", func(o *unstructured.Unstructured) {
			noiseContainer(o)["env"] = []any{map[string]any{"name": "CPU", "valueFrom": map[string]any{"resourceFieldRef": map[string]any{"containerName": "search", "resource": "limits.cpu", "divisor": "1000m"}}}}
		}, func(o *unstructured.Unstructured) {
			noiseContainer(o)["env"].([]any)[0].(map[string]any)["valueFrom"].(map[string]any)["resourceFieldRef"].(map[string]any)["divisor"] = "1"
		}, false},
		{"unregistered schema string", func(o *unstructured.Unstructured) { o.SetAPIVersion("example.com/v1"); o.SetKind("Widget") }, func(o *unstructured.Unstructured) {
			noiseContainer(o)["resources"].(map[string]any)["requests"].(map[string]any)["cpu"] = "1"
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := noiseSource()
			tc.prepare(source)
			live := source.DeepCopy()
			if err := RecordAcceptance(live, source); err != nil {
				t.Fatal(err)
			}
			tc.change(live)
			if diff := noiseDiff(t, source, live, nil); diff.Changed != tc.changed {
				t.Fatalf("changed=%v want=%v: %+v", diff.Changed, tc.changed, diff)
			}
		})
	}
}

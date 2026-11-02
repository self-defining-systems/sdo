package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

const acceptedValueDeployment = "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: web}\nspec:\n  selector: {matchLabels: {app: web}}\n  template:\n    metadata: {labels: {app: web}}\n    spec:\n      containers:\n      - name: web\n        image: example/web:1\n"

const acceptedValueService = "apiVersion: v1\nkind: Service\nmetadata: {name: web}\nspec:\n  selector: {app: web}\n  ports:\n  - port: 80\n    targetPort: 8080\n"

var (
	acceptedValueDeploymentRef = sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}
	acceptedValueServiceRef    = sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "web"}
)

// adoptedValueApp is an application adopted by install-existing whose live
// objects carry the defaults the API server fills in for fields that accepted
// source leaves out.
type adoptedValueApp struct {
	client       *fake.Clientset
	sourceClient acceptedSourceBridge
	source       GitPlanSource
	commit       string
	view         func(*testing.T) program.ReadView
	// liveView reads the objects as the controller's informers serve them,
	// with their managed fields; view reads their corpus capture.
	liveView func(*testing.T) program.ReadView
}

func (a adoptedValueApp) reconcile(t *testing.T, ref sdk.ObjectRef) {
	t.Helper()
	provider := fastloopObjects{client: a.client}
	executor := PlanExecutor{Client: TypedPlanClient{Client: a.sourceClient}, Source: a.source, Rollouts: &planFakeRollout{}, Verifier: PlanHealthVerifier{Detectors: []sdk.Detector{unschedulable{}}, Provider: provider}}
	result, err := executor.Apply(context.Background(), IncidentRequest{IncidentID: "value-drift", Namespace: "app", SourceCommit: a.commit, RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: ref}}}, false)
	if err != nil || !result.Applied {
		t.Fatalf("ReconcileFromSource did not apply: %+v %v", result, err)
	}
}

// liveLeaf reads one flattened configuration path of the live object.
func (a adoptedValueApp) liveLeaf(t *testing.T, ref sdk.ObjectRef, path string) any {
	t.Helper()
	var object kruntime.Object
	var err error
	if ref.Kind == "Deployment" {
		object, err = a.client.AppsV1().Deployments("app").Get(context.Background(), ref.Name, metav1.GetOptions{})
	} else {
		object, err = a.client.CoreV1().Services("app").Get(context.Background(), ref.Name, metav1.GetOptions{})
	}
	if err != nil {
		t.Fatal(err)
	}
	content, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(object)
	if err != nil {
		t.Fatal(err)
	}
	return flattenDriftObject(&unstructured.Unstructured{Object: content})[path]
}

func encodedLeaf(value any) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

// TestRecreatedObjectKeepsItsServerAssignedFields: a Service's clusterIP is
// assigned once and the API server never lets it change, so a different
// value means the object was recreated. The record belongs to the old UID; the
// new object has no acceptance point until adoption records one, and nothing
// is restored over its assigned address.
func TestRecreatedObjectKeepsItsServerAssignedFields(t *testing.T) {
	source := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"name": "web", "namespace": "app"},
		"spec": map[string]any{"selector": map[string]any{"app": "web"}, "ports": []any{map[string]any{"port": int64(80)}}},
	}}
	build := func(uid, clusterIP string) *unstructured.Unstructured {
		object := source.DeepCopy()
		object.SetUID(types.UID(uid))
		_ = unstructured.SetNestedField(object.Object, clusterIP, "spec", "clusterIP")
		_ = unstructured.SetNestedStringSlice(object.Object, []string{clusterIP}, "spec", "clusterIPs")
		_ = unstructured.SetNestedSlice(object.Object, []any{map[string]any{"port": int64(80), "nodePort": int64(30080)}}, "spec", "ports")
		return object
	}
	old := build("old-uid", "10.0.0.7")
	if err := RecordAcceptance(old, source); err != nil {
		t.Fatal(err)
	}
	recreated := build("new-uid", "10.0.0.9")
	recreated.SetAnnotations(old.GetAnnotations())
	if _, err := addedSourceFields(recreated, source); !errors.Is(err, ErrAcceptanceRecordUnavailable) {
		t.Fatalf("a recreated object inherited its predecessor's record: %v", err)
	}
	next := recreated.DeepCopy()
	removeAddedFields(next, recreated, source)
	if got, _, _ := unstructured.NestedString(next.Object, "spec", "clusterIP"); got != "10.0.0.9" {
		t.Fatalf("reconciliation restored the old clusterIP over the recreated object: %q", got)
	}
	// The unchanged original still compares clean.
	if added, err := addedSourceFields(old, source); err != nil || len(added) != 0 {
		t.Fatalf("accepted object reports additions: %v %v", added, err)
	}
}

// TestAcceptanceRecordKeepsValuesOfConfigurationOnly: payloads (Secret and
// ConfigMap data), metadata and status never enter the record, and a record
// that does not fit with values still records its paths.
func TestAcceptanceRecordKeepsValuesOfConfigurationOnly(t *testing.T) {
	source := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "settings", "namespace": "app"},
		"data": map[string]any{"declared": "yes"},
	}}
	live := source.DeepCopy()
	live.SetUID("uid")
	live.SetLabels(map[string]string{"owner": "team"})
	_ = unstructured.SetNestedField(live.Object, "token-value", "data", "generated")
	_ = unstructured.SetNestedField(live.Object, "key-value", "binaryData", "blob")
	_ = unstructured.SetNestedField(live.Object, "Bound", "status", "phase")
	_ = unstructured.SetNestedField(live.Object, "extra", "spec", "mode")
	encoded, fits, err := buildAcceptanceRecord(source, live, live, nil, false)
	if err != nil || !fits {
		t.Fatalf("record: %v %v", fits, err)
	}
	var record acceptanceRecord
	if err := json.Unmarshal([]byte(encoded), &record); err != nil {
		t.Fatal(err)
	}
	if len(record.Values) != 1 || encodedLeaf(record.Values["spec.mode"]) != `"extra"` {
		t.Fatalf("record values are not the undeclared configuration scalars: %v", record.Values)
	}
	if strings.Contains(encoded, "token-value") || strings.Contains(encoded, "key-value") {
		t.Fatalf("record carries payload data: %s", encoded)
	}

	big := source.DeepCopy()
	big.SetUID("uid")
	for i := range 800 {
		_ = unstructured.SetNestedField(big.Object, strings.Repeat("v", 40), "spec", "knobs", string(rune('a'+i%26))+strings.Repeat("k", i/26+1))
	}
	encoded, fits, err = buildAcceptanceRecord(source, big, big, nil, false)
	if err != nil || !fits {
		t.Fatalf("a large object lost its whole record: %v %v", fits, err)
	}
	var trimmed acceptanceRecord
	if err := json.Unmarshal([]byte(encoded), &trimmed); err != nil || len(trimmed.Paths) < 800 {
		t.Fatalf("large record dropped its paths: %d %v", len(trimmed.Paths), err)
	}
}

// installSyntheticAdoptedDefaults models the isolated CREATE contract for these
// toy fixtures. It defaults only submitted source and never reads the faulted
// live object or persists the dry-run probe.
func installSyntheticAdoptedDefaults(t *testing.T, client *fake.Clientset) {
	t.Helper()
	client.PrependReactor("create", "deployments", func(action ktesting.Action) (bool, kruntime.Object, error) {
		values, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(action.(ktesting.CreateAction).GetObject())
		if err != nil {
			return true, nil, err
		}
		object := &unstructured.Unstructured{Object: values}
		// Typed conversion retains an empty strategy object for omitted source.
		// Default its type independently, as the API does.
		if strategy, found, _ := unstructured.NestedString(object.Object, "spec", "strategy", "type"); !found || strategy == "" {
			_ = unstructured.SetNestedField(object.Object, "RollingUpdate", "spec", "strategy", "type")
		}
		defaults := map[string]any{"replicas": int64(1)}
		for key, value := range defaults {
			if _, found, _ := unstructured.NestedFieldNoCopy(object.Object, "spec", key); !found {
				_ = unstructured.SetNestedField(object.Object, value, "spec", key)
			}
		}
		defaults = map[string]any{"dnsPolicy": "ClusterFirst", "restartPolicy": "Always", "schedulerName": "default-scheduler", "terminationGracePeriodSeconds": int64(30), "enableServiceLinks": true}
		for key, value := range defaults {
			if _, found, _ := unstructured.NestedFieldNoCopy(object.Object, "spec", "template", "spec", key); !found {
				_ = unstructured.SetNestedField(object.Object, value, "spec", "template", "spec", key)
			}
		}
		containers := addedFieldsContainers(object)
		for _, item := range containers {
			container := item.(map[string]any)
			if _, found := container["imagePullPolicy"]; !found {
				container["imagePullPolicy"] = "IfNotPresent"
			}
		}
		_ = unstructured.SetNestedSlice(object.Object, containers, "spec", "template", "spec", "containers")
		return true, object, nil
	})
	client.PrependReactor("create", "services", func(action ktesting.Action) (bool, kruntime.Object, error) {
		values, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(action.(ktesting.CreateAction).GetObject())
		if err != nil {
			return true, nil, err
		}
		object := &unstructured.Unstructured{Object: values}
		for key, value := range map[string]any{"type": "ClusterIP", "sessionAffinity": "None", "internalTrafficPolicy": "Cluster"} {
			if _, found, _ := unstructured.NestedFieldNoCopy(object.Object, "spec", key); !found {
				_ = unstructured.SetNestedField(object.Object, value, "spec", key)
			}
		}
		ports, _, _ := unstructured.NestedSlice(object.Object, "spec", "ports")
		for _, item := range ports {
			port := item.(map[string]any)
			if _, found := port["protocol"]; !found {
				port["protocol"] = "TCP"
			}
		}
		_ = unstructured.SetNestedSlice(object.Object, ports, "spec", "ports")
		return true, object, nil
	})
}

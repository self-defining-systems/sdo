package runtime

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// Use the versioned source input, never a run capture or learned memory. The
// harness must deploy these same bytes; controller bookkeeping over two
// minutes of virtual observation time cannot invent a baseline finding.
func TestAstronomyBaselineSourceComparisonTwoMinutes(t *testing.T) {
	file, err := os.Open("../../../apps/astronomy_shop/kubernetes/astronomy-shop.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	decoder := kyaml.NewYAMLOrJSONDecoder(file, 4096)
	checked := 0
	for {
		source := &unstructured.Unstructured{}
		err := decoder.Decode(&source.Object)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		switch source.GetKind() {
		case "Deployment", "StatefulSet", "DaemonSet", "ConfigMap":
		default:
			continue
		}
		checked++
		t.Run(source.GetKind()+"/"+source.GetName(), func(t *testing.T) {
			live := source.DeepCopy()
			if err := RecordAcceptance(live, source); err != nil {
				t.Fatal(err)
			}
			for seconds := 0; seconds <= 120; seconds += 15 {
				if source.GetKind() == "DaemonSet" {
					annotations := live.GetAnnotations()
					annotations["deprecated.daemonset.template.generation"] = fmt.Sprint(seconds/15 + 1)
					live.SetAnnotations(annotations)
				}
				snapshot := sdktest.Snapshot{NamespaceName: source.GetNamespace(), Objects: []unstructured.Unstructured{*live}}
				view := LiveReadView{DetectionContext: snapshot, Source: planFakeSource{source: source}, SourceCommit: "accepted"}
				ref := sdk.ObjectRef{APIVersion: source.GetAPIVersion(), Kind: source.GetKind(), Namespace: source.GetNamespace(), Name: source.GetName()}
				diff, err := declaredSourceDiff(view, ref)
				if err != nil || diff.Changed {
					t.Fatalf("baseline at %ds: %+v %v", seconds, diff, err)
				}
				event, err := (DriftClassifier{}).Classify(context.Background(), ObjectChange{Old: source, New: live, Adoption: true}, view, time.Unix(int64(seconds), 0))
				if err != nil || event != nil {
					t.Fatalf("baseline finding at %ds: %+v %v", seconds, event, err)
				}
			}
			// A source-owned mutation still fires after the quiet baseline interval.
			if source.GetKind() == "ConfigMap" {
				data, _, _ := unstructured.NestedStringMap(live.Object, "data")
				if len(data) == 0 {
					return
				}
				for key := range data {
					data[key] += "\nsynthetic change"
					break
				}
				if err := unstructured.SetNestedStringMap(live.Object, data, "data"); err != nil {
					t.Fatal(err)
				}
			} else {
				containers, _, _ := unstructured.NestedSlice(live.Object, "spec", "template", "spec", "containers")
				containers[0].(map[string]any)["image"] = "synthetic.invalid/app:changed"
				if err := unstructured.SetNestedSlice(live.Object, containers, "spec", "template", "spec", "containers"); err != nil {
					t.Fatal(err)
				}
			}
			snapshot := sdktest.Snapshot{NamespaceName: source.GetNamespace(), Objects: []unstructured.Unstructured{*live}}
			view := LiveReadView{DetectionContext: snapshot, Source: planFakeSource{source: source}, SourceCommit: "accepted"}
			ref := sdk.ObjectRef{APIVersion: source.GetAPIVersion(), Kind: source.GetKind(), Namespace: source.GetNamespace(), Name: source.GetName()}
			diff, err := declaredSourceDiff(view, ref)
			if err != nil || !diff.Changed {
				t.Fatalf("source-owned mutation lost: %+v %v", diff, err)
			}
		})
	}
	if checked < 30 {
		t.Fatalf("baseline source coverage unexpectedly shrank: %d", checked)
	}
}

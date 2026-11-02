//go:build integration

package runtime

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/sdktest"
)

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
	view := LiveReadView{Carrier: inputs.New(sdktest.Snapshot{NamespaceName: "app", DeploymentList: []appsv1.Deployment{actual}}), Source: planFakeSource{source: source}, SourceCommit: "accepted", Client: bridgeFake(t, client)}
	if diff, err := view.DiffAgainstSource(addedFieldsRef()); err != nil || diff.Changed || !called {
		t.Fatalf("dry run comparison: %+v %v called=%v", diff, err, called)
	}
}


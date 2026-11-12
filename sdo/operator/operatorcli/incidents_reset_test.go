package operatorcli

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/utils/clock"

	"sdo.dev/operator/install"
)

// The harness reset is a request the running controller acknowledges: the
// command records a fresh generation and returns once the controller wrote
// the same generation back.
func TestIncidentsResetWaitsForTheControllerAcknowledgement(t *testing.T) {
	cfg, err := ParseIncidentsResetLaunch([]string{"incidents", "reset", "--control-namespace", "app-sdo", "--timeout", "10s"})
	if err != nil {
		t.Fatal(err)
	}
	client := fake.NewSimpleClientset()
	// The stand-in controller acknowledges a request as it is written.
	acknowledge := func(action k8stesting.Action) (bool, runtime.Object, error) {
		if object, ok := action.(k8stesting.CreateAction); ok {
			if configMap, ok := object.GetObject().(*corev1.ConfigMap); ok && configMap.Name == install.ResetConfigMap {
				configMap.Data["acknowledged"] = configMap.Data["requested"]
			}
		}
		return false, nil, nil
	}
	client.PrependReactor("create", "configmaps", acknowledge)
	client.PrependReactor("update", "configmaps", acknowledge)
	resets := install.ResetRequests{Client: client, Namespace: "app-sdo"}
	ctx := context.Background()
	result, err := requestIncidentsReset(ctx, client, clock.RealClock{}, cfg)
	if err != nil || !result.Acknowledged || result.Generation == "" {
		t.Fatalf("result %+v, err %v", result, err)
	}
	if pending, err := resets.Pending(ctx); err != nil || pending != "" {
		t.Fatalf("an acknowledged reset is still pending: %q %v", pending, err)
	}
}

func TestIncidentsResetRequiresControlNamespace(t *testing.T) {
	if _, err := ParseIncidentsResetLaunch([]string{"incidents", "reset"}); err == nil {
		t.Fatal("a reset without a control namespace was accepted")
	}
}

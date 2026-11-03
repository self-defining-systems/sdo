package runtime

import (
	"errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
)

func TestLiveObjectDistinguishesUncapturedFromConfirmedAbsence(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "present", Namespace: "app"}})
	view := LiveReadView{Carrier: inputs.New(sdktest.Snapshot{NamespaceName: "app"}), Client: client}
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "present"}
	if obj, err := view.Object(ref); err != nil || obj == nil {
		t.Fatalf("uncaptured live object: %v %v", obj, err)
	}
	ref.Name = "missing"
	if _, err := view.Object(ref); !apierrors.IsNotFound(err) {
		t.Fatalf("absence lacks API proof: %v", err)
	}
	if _, err := view.snapshotObject(ref); !errors.Is(err, errSnapshotObjectAbsent) {
		t.Fatalf("diff cannot recognize API absence: %v", err)
	}
	view.Client = nil
	if _, err := view.snapshotObject(ref); !errors.Is(err, sdk.ErrObjectNotCaptured) {
		t.Fatalf("uncaptured offline object became absence: %v", err)
	}
}

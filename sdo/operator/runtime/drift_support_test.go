package runtime

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
)

type driftReadView struct {
	*sdktest.ReplayReadView
	diff program.Diff
}

func (v driftReadView) DiffAgainstSource(sdk.ObjectRef) (program.Diff, error) { return v.diff, nil }

func driftService(target int32) *corev1.Service {
	return &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app", ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "sdo-deployer", Operation: metav1.ManagedFieldsOperationApply, FieldsType: "FieldsV1", FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:spec":{"f:ports":{"k:{\"port\":80,\"protocol\":\"TCP\"}":{"f:targetPort":{}}}}}`)}}}}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80, Protocol: corev1.ProtocolTCP, TargetPort: intstr.FromInt32(target)}}}}
}

func objectChange(t *testing.T, old, next any, version, kind string) ObjectChange {
	t.Helper()
	return ObjectChange{Old: driftObject(old, sdk.WatchKind{APIVersion: version, Kind: kind}), New: driftObject(next, sdk.WatchKind{APIVersion: version, Kind: kind})}
}

package runtime

import (
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk/sdktest"
)

const foreignInvariantRunner = `package main
import (
 "context"
 "encoding/json"
 "os"
 candidate "foreign-invariant/candidate"
 "sdo.dev/operator/sdk/program"
 "sdo.dev/operator/sdk/sdktest"
)
func main() {
 var input struct {Snapshot sdktest.Snapshot; Calls []sdktest.PrimitiveCall; Bindings program.Bindings}
 if err:=json.NewDecoder(os.Stdin).Decode(&input);err!=nil {panic(err)}
 view:=&sdktest.ReplayReadView{Snapshot:input.Snapshot,Calls:input.Calls}
 verdict,err:=(candidate.Program{}).Diagnose(context.Background(),view,input.Bindings)
 if err!=nil {panic(err)}
 if err:=json.NewEncoder(os.Stdout).Encode(verdict);err!=nil {panic(err)}
}
`

// policyWorkloadSnapshot serves foreignSource's Service as adoption left it.
func policyWorkloadSnapshot(policies ...networkingv1.NetworkPolicy) sdktest.Snapshot {
	service := corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "api"}, Ports: []corev1.ServicePort{{Port: 80, TargetPort: driftService(8080).Spec.Ports[0].TargetPort}}}}
	accepted := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"name": "api"}, "spec": map[string]any{"selector": map[string]any{"app": "api"}, "ports": []any{map[string]any{"port": int64(80), "targetPort": int64(8080)}}}}}
	annotations, err := acceptanceAnnotations(&service, accepted)
	if err != nil {
		panic(err)
	}
	service.Annotations = annotations
	return sdktest.Snapshot{NamespaceName: "app", NetworkPolicyList: policies,
		ServiceList: []corev1.Service{service},
		PodList:     []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "api-pod", Namespace: "app", Labels: map[string]string{"app": "api"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "api", Ports: []corev1.ContainerPort{{ContainerPort: 8080}}}}}}},
	}
}

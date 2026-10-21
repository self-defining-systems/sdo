package runtime

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/program/testdata/targetport"
	"sdo.dev/operator/sdk/sdktest"
)

func TestTargetPortFixtureCandidateClearsAfterRepair(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	snapshot := sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "api"}, Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(9999)}}}}}, PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "api-pod", Namespace: "app", Labels: map[string]string{"app": "api"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "api", Ports: []corev1.ContainerPort{{ContainerPort: 8080}}}}}}}}
	registry, err := program.NewRegistry([]program.Diagnoser{targetport.Program{}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	finding := sdk.Finding{Status: sdk.FindingActive, PrimaryResource: ref}
	broken := SelectProgramCandidates(context.Background(), registry, &sdktest.ReplayReadView{Snapshot: snapshot}, []sdk.Finding{finding}, time.Second)
	if len(broken) != 1 || broken[0].Verdict.Result != program.Match {
		t.Fatalf("broken targetPort: %+v", broken)
	}
	snapshot.ServiceList[0].Spec.Ports[0].TargetPort = intstr.FromInt32(8080)
	fixed := SelectProgramCandidates(context.Background(), registry, &sdktest.ReplayReadView{Snapshot: snapshot}, []sdk.Finding{finding}, time.Second)
	if len(fixed) != 1 || fixed[0].Verdict.Result != program.NoMatch {
		t.Fatalf("fixed targetPort: %+v", fixed)
	}
}

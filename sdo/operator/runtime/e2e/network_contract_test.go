//go:build integration && controller_e2e

package e2e

import (
 "context"
 "net"
 "path/filepath"
 "strconv"
 "testing"
 "time"

 corev1 "k8s.io/api/core/v1"
 metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
 "k8s.io/apimachinery/pkg/util/intstr"
 "k8s.io/client-go/kubernetes/fake"
)

func TestServiceTCPProbeTracksTargetPort(t *testing.T) {
 port := int32(freePort(t))
 pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name:"app", Namespace:"app", UID:"pod", Labels:map[string]string{"app":"test"}, OwnerReferences:controllerRef("ReplicaSet", "apps/v1", "app", "rs")}, Spec:corev1.PodSpec{Containers:[]corev1.Container{{Name:"app", Ports:[]corev1.ContainerPort{{ContainerPort:port}}}}}, Status:corev1.PodStatus{Phase:corev1.PodRunning, PodIP:"127.1.250.1", Conditions:[]corev1.PodCondition{{Type:corev1.PodReady, Status:corev1.ConditionTrue}}}}
 svc := &corev1.Service{ObjectMeta:metav1.ObjectMeta{Name:"app", Namespace:"app"}, Spec:corev1.ServiceSpec{ClusterIP:"10.0.0.1", Selector:pod.Labels, Ports:[]corev1.ServicePort{{Port:port, TargetPort:intstr.FromInt32(port)}}}}
 client := fake.NewSimpleClientset(pod,svc)
 k := &Kubelet{cluster:&Cluster{Client:client}, net:newNetwork(filepath.Join(t.TempDir(),"hosts"))}
 t.Cleanup(k.net.close)
 for _, broken := range []bool{false,true,false} {
  target:=port
  if broken { target=int32(freePort(t)) }
  svc.Spec.Ports[0].TargetPort=intstr.FromInt32(target)
  if _,err:=client.CoreV1().Services("app").Update(context.Background(),svc,metav1.UpdateOptions{}); err!=nil {t.Fatal(err)}
  if err:=k.reconcileNetwork(context.Background());err!=nil {t.Fatal(err)}
  conn,err:=net.DialTimeout("tcp",net.JoinHostPort(k.net.serviceIP("app/app"),strconv.Itoa(int(port))),time.Second)
  if conn!=nil {_=conn.Close()}
  if (err!=nil)!=broken {t.Fatalf("broken=%v: TCP probe error=%v",broken,err)}
 }
}

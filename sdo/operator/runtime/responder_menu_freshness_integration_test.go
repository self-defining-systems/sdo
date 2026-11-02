//go:build integration

package runtime

import (
	"context"
	"encoding/json"

	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/cache"
)

// Replay the missed-watch schedule deterministically: an older healthy cache
// entry must not witness the wrong-port write, even when delivery is delayed.
func TestProductionMenuFixtureRejectsStaleHealthyWriteWitness(t *testing.T) {
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: menuService.Name, Namespace: "app", UID: "fixture-uid", ResourceVersion: "1"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt(9999)}}}}
	client := productionMenuClient(t, service)
	informer := cache.NewSharedIndexInformer(&cache.ListWatch{}, &corev1.Service{}, 0, cache.Indexers{})
	stale := service.DeepCopy()
	stale.Spec.Ports[0].TargetPort = intstr.FromInt(8080)
	if err := informer.GetStore().Add(stale); err != nil {
		t.Fatal(err)
	}
	observer := &KubernetesCache{informers: map[string]cache.SharedIndexInformer{"v1/Service": informer}}
	updated, err := client.CoreV1().Services("app").Patch(context.Background(), service.Name, types.MergePatchType, []byte(`{"spec":{"ports":[{"port":80,"targetPort":7777}]}}`), metav1.PatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	writes := []ObservedWrite{{Ref: menuService, ResourceVersion: updated.ResourceVersion, UID: string(updated.UID), Strict: true}}
	if observer.observed(writes) {
		t.Fatal("stale healthy snapshot certified the wrong-port write before watch delivery")
	}
	if err := informer.GetStore().Update(updated); err != nil {
		t.Fatal(err)
	}
	if !observer.observed(writes) || updated.Spec.Ports[0].TargetPort.IntVal != 7777 {
		t.Fatal("delivered write must retain the unresolved wrong port")
	}
}

// Source normalization must project admission defaults without repairing faults.
func TestProductionMenuFixtureDryRunDoesNotRepairService(t *testing.T) {
	for _, transport := range []string{"typed", "discovery"} {
		t.Run(transport, func(t *testing.T) {
			service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: menuService.Name, Namespace: "app", ResourceVersion: "1"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt(9999)}}}}
			client := productionMenuClient(t, service)
			body := []byte(`{"apiVersion":"v1","kind":"Service","metadata":{"name":"backend","namespace":"app"},"spec":{"ports":[{"port":80,"targetPort":8080}]}}`)
			var projected *corev1.Service
			var err error
			if transport == "typed" {
				projected, err = client.CoreV1().Services("app").Patch(context.Background(), service.Name, types.ApplyPatchType, body, metav1.PatchOptions{DryRun: []string{metav1.DryRunAll}})
			} else {
				var data []byte
				data, err = client.Discovery().RESTClient().Patch(types.ApplyPatchType).AbsPath("/api/v1/namespaces/app/services/backend").Param("dryRun", metav1.DryRunAll).Body(body).Do(context.Background()).Raw()
				if err == nil {
					projected = &corev1.Service{}
					err = json.Unmarshal(data, projected)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			stored, err := client.CoreV1().Services("app").Get(context.Background(), service.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if projected.Spec.Ports[0].TargetPort.IntVal != 8080 {
				t.Fatal("dry-run must return the projected Service")
			}
			if stored.Spec.Ports[0].TargetPort.IntVal != 9999 || stored.ResourceVersion != "1" {
				t.Fatalf("source normalization silently repaired the live fixture: %+v", stored)
			}
		})
	}
}


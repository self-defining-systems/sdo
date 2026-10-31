package runtime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	"reflect"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"strings"
	"testing"
	"time"
)

func TestExtendedInformerChangesTriggerEvaluation(t *testing.T) {
	tests := []struct {
		kind, version, resource string
		object                  kruntime.Object
	}{
		{"StatefulSet", "apps/v1", "statefulsets", &appsv1.StatefulSet{}},
		{"DaemonSet", "apps/v1", "daemonsets", &appsv1.DaemonSet{}},
		{"Job", "batch/v1", "jobs", &batchv1.Job{}},
		{"CronJob", "batch/v1", "cronjobs", &batchv1.CronJob{}},
		{"PersistentVolumeClaim", "v1", "persistentvolumeclaims", &corev1.PersistentVolumeClaim{}},
		{"ServiceAccount", "v1", "serviceaccounts", &corev1.ServiceAccount{}},
		{"HorizontalPodAutoscaler", "autoscaling/v2", "horizontalpodautoscalers", &autoscalingv2.HorizontalPodAutoscaler{}},
		{"PodDisruptionBudget", "policy/v1", "poddisruptionbudgets", &policyv1.PodDisruptionBudget{}},
		{"Ingress", "networking.k8s.io/v1", "ingresses", &networkingv1.Ingress{}},
		{"Node", "v1", "nodes", &corev1.Node{}},
		{"Secret", "v1", "secrets", &corev1.Secret{}},
	}
	for _, tt := range tests {
		t.Run(tt.kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			client := fake.NewSimpleClientset()
			watch := sdk.WatchKind{APIVersion: tt.version, Kind: tt.kind}
			detector := scheduledDetector{spec: sdk.DetectorSpec{ID: tt.kind, Interval: time.Hour, Watches: []sdk.WatchKind{watch}}}
			c, err := NewKubernetesCache(KubernetesCacheConfig{Namespace: "demo", Client: client, TrackDrift: true}, []sdk.Detector{detector})
			if err != nil {
				t.Fatal(err)
			}
			c.Start(ctx)
			defer c.Stop()
			if err = c.WaitForSync(ctx); err != nil {
				t.Fatal(err)
			}
			drainWatchEvents(c)
			meta := tt.object.(metav1.Object)
			meta.SetName("test")
			meta.SetResourceVersion("1")
			namespace := "demo"
			if tt.kind == "Node" {
				namespace = ""
			}
			meta.SetNamespace(namespace)
			if s, ok := tt.object.(*corev1.Secret); ok {
				s.Data = map[string][]byte{"password": []byte("synthetic-value-do-not-record")}
				s.StringData = map[string]string{"token": "synthetic-value-do-not-record"}
				s.Annotations = map[string]string{"applied": "synthetic-value-do-not-record"}
			}
			gv, err := schema.ParseGroupVersion(tt.version)
			if err != nil {
				t.Fatal(err)
			}
			gvr := gv.WithResource(tt.resource)
			if err = client.Tracker().Create(gvr, tt.object, namespace); err != nil {
				t.Fatal(err)
			}
			watch.Namespace = namespace
			awaitWatchEvent(t, c, watch)
			// Move the interval into the future, then prove a real informer update schedules evaluation.
			scheduler := NewScheduler([]sdk.Detector{detector}, time.Time{})
			now := time.Now()
			scheduler.SelectEvents(now, nil)
			drainWatchEvents(c)
			meta.SetLabels(map[string]string{"changed": "yes"})
			meta.SetResourceVersion("2")
			if err = client.Tracker().Update(gvr, tt.object, namespace); err != nil {
				t.Fatal(err)
			}
			select {
			case <-c.Notifications():
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			var selected []sdk.Detector
			for len(selected) == 0 {
				events := c.TakeEvents()
				selected = scheduler.SelectEvents(now.Add(time.Second), events)
				if len(selected) > 0 {
					break
				}
				select {
				case <-c.Notifications():
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			actionsBefore := len(client.Actions())
			view, err := c.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(client.Actions()) != actionsBefore {
				t.Fatal("snapshot polled the Kubernetes API")
			}
			for _, d := range selected {
				if _, err = d.Detect(ctx, view); err != nil {
					t.Fatal(err)
				}
			}
			method := tt.resource
			methods := map[string]string{"persistentvolumeclaims": "PersistentVolumeClaims", "serviceaccounts": "ServiceAccounts", "horizontalpodautoscalers": "HorizontalPodAutoscalers", "poddisruptionbudgets": "PodDisruptionBudgets", "statefulsets": "StatefulSets", "daemonsets": "DaemonSets", "cronjobs": "CronJobs"}
			method = strings.ToUpper(method[:1]) + method[1:]
			if mapped, ok := methods[tt.resource]; ok {
				method = mapped
			}
			if count := reflect.ValueOf(view).MethodByName(method).Call(nil)[0].Len(); count != 1 {
				t.Fatalf("%s count: %d", method, count)
			}
			captured, err := sdktest.CaptureSnapshot(view, "demo")
			if err != nil {
				t.Fatal(err)
			}
			if tt.kind == "Secret" {
				if len(view.Secrets()) != 1 || len(view.Secrets()[0].DataKeys) != 2 {
					t.Fatalf("secret metadata: %#v", view.Secrets())
				}
			}
			// The informer store, change queue, snapshot and corpus must all be value-free.
			for _, value := range []any{c.informers[tt.version+"/"+tt.kind].GetStore().List(), c.TakeObjectChanges(), view, captured} {
				data, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				for _, bad := range []string{"synthetic-value-do-not-record", base64.StdEncoding.EncodeToString([]byte("synthetic-value-do-not-record"))} {
					if strings.Contains(string(data), bad) {
						t.Fatalf("secret value in serialized boundary: %s", data)
					}
				}
			}
		})
	}
}

//go:build integration

package runtime

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/sdk"
)

func TestKubernetesCacheAwaitsWritesBeforeVerification(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "demo", ResourceVersion: "5"}}
		client := fake.NewSimpleClientset(service)
		cache, err := newCacheForTest(t, KubernetesCacheConfig{Namespace: "demo", Client: client, ResyncPeriod: time.Hour},
			[]sdk.Detector{scheduledDetector{spec: sdk.DetectorSpec{ID: "svc", Interval: time.Second, Watches: []sdk.WatchKind{{APIVersion: "v1", Kind: "Service"}}}}})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cache.Start(ctx)
		if err := cache.WaitForSync(ctx); err != nil {
			t.Fatal(err)
		}
		ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "demo", Name: "api"}
		if !cache.observed([]ObservedWrite{{Ref: ref, ResourceVersion: "5"}}) {
			t.Fatal("cache does not show the version it was listed at")
		}
		if cache.observed([]ObservedWrite{{Ref: ref, ResourceVersion: "6"}}) {
			t.Fatal("cache claims to have observed a write it has not received")
		}
		if !cache.observed([]ObservedWrite{{Ref: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "demo", Name: "x"}, ResourceVersion: "9"}}) {
			t.Fatal("an unwatched kind must not be waited for")
		}
		updated := service.DeepCopy()
		updated.ResourceVersion = "6"
		updated.Labels = map[string]string{"changed": "yes"}
		done := make(chan error, 1)
		go func() { done <- cache.AwaitWrites(ctx, []ObservedWrite{{Ref: ref, ResourceVersion: "6"}}) }()
		if _, err := client.CoreV1().Services("demo").Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-done:
			if err != nil || !cache.observed([]ObservedWrite{{Ref: ref, ResourceVersion: "6"}}) {
				t.Fatalf("AwaitWrites returned before the write was observed: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("AwaitWrites did not return after the cache observed the write")
		}
	})
}

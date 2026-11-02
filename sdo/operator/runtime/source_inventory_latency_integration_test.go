//go:build integration

package runtime

import (
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
)

func BenchmarkSourceInventoryInformerHotel(b *testing.B) {
	declared, live := hotelInventory()
	c, err := newCacheForTest(b, KubernetesCacheConfig{Namespace: "app", Client: fake.NewSimpleClientset(), TrackDrift: true}, nil)
	if err != nil {
		b.Fatal(err)
	}
	for _, o := range live {
		key := o.GetAPIVersion() + "/" + o.GetKind()
		if informer := c.informers[key]; informer != nil {
			if err := informer.GetStore().Add(o); err != nil {
				b.Fatal(err)
			}
		}
	}
	index := NewSourceInventory("app", "accepted", declared)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if findings := index.Findings(c.SourceDriftObjects(), time.Unix(100, 0)); len(findings) != 0 {
			b.Fatal(findings)
		}
	}
}

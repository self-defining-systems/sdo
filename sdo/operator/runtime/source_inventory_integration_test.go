//go:build integration

package runtime

import (
	"testing"

	"k8s.io/client-go/kubernetes/fake"
)

func TestSourceInventoryCacheOnlyExposesNamespaceBoundClusterRBAC(t *testing.T) {
	c, err := newCacheForTest(t, KubernetesCacheConfig{Namespace: "app", Client: fake.NewSimpleClientset(), TrackDrift: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	objects := latentInventoryObjects()
	for _, o := range objects[3:] {
		if err := c.informers[o.GetAPIVersion()+"/"+o.GetKind()].GetStore().Add(o); err != nil {
			t.Fatal(err)
		}
	}
	unrelated := objects[3].DeepCopy()
	unrelated.SetName("other-namespace-role")
	if err := c.informers[unrelated.GetAPIVersion()+"/"+unrelated.GetKind()].GetStore().Add(unrelated); err != nil {
		t.Fatal(err)
	}
	got := c.SourceDriftObjects()
	if len(got) != 2 {
		t.Fatalf("inventory exposed unrelated cluster authorization: %+v", got)
	}
	for _, o := range got {
		if o.GetName() == unrelated.GetName() {
			t.Fatal("unrelated cluster role leaked into app inventory")
		}
	}
}

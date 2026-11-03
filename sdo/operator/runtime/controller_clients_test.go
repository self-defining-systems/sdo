package runtime

import (
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"testing"
)

func TestNativeAndEmbeddedClientBudgets(t *testing.T) {
	for _, embedded := range []bool{false, true} {
		configured := fake.NewSimpleClientset()
		calls := 0
		clients, err := newControllerClients("app", configured, embedded, func() (kubernetes.Interface, error) { calls++; return fake.NewSimpleClientset(), nil })
		if err != nil {
			t.Fatal(err)
		}
		if embedded {
			if calls != 0 || clients.snapshot.Client != configured || clients.state != configured || clients.lease != configured || clients.incidentRead != configured {
				t.Fatal("embedded execution escaped its injected API boundary")
			}
		} else {
			if calls != 4 {
				t.Fatalf("native clients must own four budgets, got %d", calls)
			}
			seen := map[kubernetes.Interface]bool{configured: true}
			for _, client := range []kubernetes.Interface{clients.snapshot.Client, clients.state, clients.lease, clients.incidentRead} {
				if seen[client] {
					t.Fatal("native observation, state, leadership or incident reads share a token bucket")
				}
				seen[client] = true
			}
		}
	}
}

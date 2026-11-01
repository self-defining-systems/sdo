//go:build integration

package runtime

import (
	"context"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"strings"
	"testing"
	"time"
)

// This host-process budget measures CPU work, which virtual time cannot observe.
func TestStateChangesStayFastAndBounded(t *testing.T) {
	objects := hotelObjects()
	for index := 0; index < 400; index++ {
		objects = append(objects, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("config-%03d", index), Namespace: hotel},
			Data:       map[string]string{"value": strings.Repeat("x", 2048)},
		})
	}
	client := fake.NewSimpleClientset(objects...)
	tracker := startedTracker(t, client)
	start := time.Now().UTC()
	tracker.Observe(start, true)
	for index := 0; index < 400; index++ {
		_ = client.CoreV1().ConfigMaps(hotel).Delete(context.Background(), fmt.Sprintf("config-%03d", index), metav1.DeleteOptions{})
		if index%40 == 39 {
			// The fake watcher buffers only 100 events; let the informer drain.
			deleted := index + 1
			eventually(t, "the informer to catch up", func() bool {
				changes := tracker.Changes(start)
				return changes != nil && len(changes.Changes)+changes.Omitted == deleted
			})
		}
	}
	var changes *StateChanges
	eventually(t, "the deletions", func() bool {
		changes = tracker.Changes(start)
		return changes != nil && len(changes.Changes)+changes.Omitted == 400
	})
	began := time.Now()
	changes = tracker.Changes(start)
	if took := time.Since(began); took > time.Second {
		t.Fatalf("a diff must take well under a second, took %s", took)
	}
	if len(changes.Changes) > maxStateChanges || changes.Omitted != 400-len(changes.Changes) {
		t.Fatalf("the diff must be capped and count what it omits: %d shown, %d omitted", len(changes.Changes), changes.Omitted)
	}
	if changes.Changes[0].Change != StateChangeRemoved {
		t.Fatalf("removals are reported: %+v", changes.Changes[0])
	}
}

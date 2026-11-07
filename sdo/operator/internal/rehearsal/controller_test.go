package rehearsal

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

type maintenance struct {
	paused     bool
	generation string
	acked      []string
}

func (m *maintenance) model(t *testing.T, drain int, objects ...any) (*ControllerModel, *fake.Clientset) {
	t.Helper()
	client := fake.NewSimpleClientset()
	for _, o := range objects {
		switch v := o.(type) {
		case *corev1.Service:
			_, _ = client.CoreV1().Services("ns").Create(context.Background(), v, metav1.CreateOptions{})
		case *corev1.Pod:
			_, _ = client.CoreV1().Pods("ns").Create(context.Background(), v, metav1.CreateOptions{})
		case *appsv1.StatefulSet:
			_, _ = client.AppsV1().StatefulSets("ns").Create(context.Background(), v, metav1.CreateOptions{})
		}
	}
	now := time.Unix(100, 0)
	return &ControllerModel{
		Client: client, Namespace: "ns", Now: func() time.Time { now = now.Add(5 * time.Second); return now }, Enabled: true, DrainTicks: drain,
		Maintenance: func(context.Context) (bool, string, error) { return m.paused, m.generation, nil },
		Acknowledge: func(_ context.Context, g string) error { m.acked = append(m.acked, g); return nil },
	}, client
}

func service() *corev1.Service {
	return &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "store"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "store"}}}
}

func pod(ready bool) *corev1.Pod {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "store-0", Labels: map[string]string{"app": "store"}}, Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: status}}}}
}

// An active controller opens an incident for a service with no ready pod, as
// the generic service-endpoints detector does.
func TestControllerModelOpensAnIncidentForAServiceWithoutEndpoints(t *testing.T) {
	m := &maintenance{}
	c, _ := m.model(t, 0, service(), pod(false))
	c.Tick(context.Background())
	c.Tick(context.Background())
	if len(c.Incidents) != 1 || !strings.Contains(c.Incidents[0].Findings[0], "service-endpoints Service/ns/store") {
		t.Fatalf("incidents = %+v", c.Incidents)
	}
}

// While paused the loop acknowledges once the in-flight evaluation drained and
// then evaluates nothing, so nothing opens an incident after the barrier.
func TestControllerModelStopsAtThePauseBarrier(t *testing.T) {
	m := &maintenance{paused: true, generation: "upstream-advance-paused-1"}
	c, _ := m.model(t, 2, service(), pod(true))
	ctx := context.Background()
	c.Tick(ctx)
	if len(m.acked) != 0 {
		t.Fatalf("acknowledged inside the drain: %v", m.acked)
	}
	c.Tick(ctx)
	c.Tick(ctx)
	if len(m.acked) != 1 || m.acked[0] != m.generation {
		t.Fatalf("acked = %v", m.acked)
	}
	// The rollout starts after the acknowledgement.
	if err := c.Client.CoreV1().Pods("ns").Delete(ctx, "store-0", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	for range 5 {
		c.Tick(ctx)
	}
	if len(c.Incidents) != 0 || len(m.acked) != 1 {
		t.Fatalf("a paused controller opened %+v / re-acked %v", c.Incidents, m.acked)
	}
	m.paused = false
	c.Tick(ctx)
	if len(c.Incidents) != 1 {
		t.Fatalf("a resumed controller must detect a service still without endpoints: %+v", c.Incidents)
	}
}

// An evaluation that began before the pause still runs through the drain: the
// model keeps the race the real loop has, so a caller that does not wait for
// the acknowledgement is caught.
func TestControllerModelStillEvaluatesWhileDrainingAPause(t *testing.T) {
	m := &maintenance{paused: true, generation: "g"}
	c, _ := m.model(t, 2, service(), pod(false))
	c.Tick(context.Background())
	if len(c.Incidents) != 1 {
		t.Fatalf("the in-flight pass found nothing: %+v", c.Incidents)
	}
}

func TestBlockersNameAnIncidentAnAcceptedAdvanceOpened(t *testing.T) {
	r := Release{Name: "r1", Adopted: true, Outcome: "accepted", ControllerIncidents: []string{"service-endpoints Service/ns/store"}}
	if got := strings.Join(r.Blockers(), "\n"); !strings.Contains(got, "incident opened by an accepted advance: service-endpoints Service/ns/store") {
		t.Fatalf("blockers = %s", got)
	}
	held := Release{Name: "r2", Outcome: "held", Hold: "health", Cause: "source_fault", Owner: "responder", ControllerIncidents: []string{"x"}}
	if got := strings.Join(held.Blockers(), "\n"); strings.Contains(got, "accepted advance") {
		t.Fatalf("a held advance's incident is the expected one: %s", got)
	}
}

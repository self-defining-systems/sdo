//go:build integration

package runtime

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
)

func startedTracker(t *testing.T, client *fake.Clientset) *StateTracker {
	t.Helper()
	tracker := newTrackerForTest(t, StateTrackerConfig{Client: client, Namespace: hotel, Settle: time.Minute, ResyncPeriod: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	t.Cleanup(tracker.Stop)
	if err := tracker.Start(ctx); err != nil {
		t.Fatalf("start tracker: %v", err)
	}
	return tracker
}

func TestStateChangesShowOnlyWhatChangedSinceTheHealthyBaseline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		client := fake.NewSimpleClientset(hotelObjects()...)
		// Deployment provenance must not exempt an application Service from drift.
		deployed, _ := client.CoreV1().Services(hotel).Get(ctx, "frontend", metav1.GetOptions{})
		deployed.ManagedFields = []metav1.ManagedFieldsEntry{{Manager: "sdo-deployer", Operation: metav1.ManagedFieldsOperationApply}}
		_, _ = client.CoreV1().Services(hotel).Update(ctx, deployed, metav1.UpdateOptions{})
		tracker := startedTracker(t, client)
		start := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
		tracker.Observe(start, true)
		if changes := tracker.Changes(start); changes == nil || len(changes.Changes) != 0 {
			t.Fatalf("no change right after the baseline: %+v", changes)
		}

		// wrong_service_selector, a new deny-all NetworkPolicy, a scaled
		// Deployment with a changed env value, and changed ConfigMap data.
		service, _ := client.CoreV1().Services(hotel).Get(ctx, "frontend", metav1.GetOptions{})
		service.Spec.Selector["current_service_name"] = "frontend"
		_, _ = client.CoreV1().Services(hotel).Update(ctx, service, metav1.UpdateOptions{})
		_, _ = client.NetworkingV1().NetworkPolicies(hotel).Create(ctx, &networkingv1.NetworkPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "deny-all", Namespace: hotel},
			Spec:       networkingv1.NetworkPolicySpec{PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}},
		}, metav1.CreateOptions{})
		deployment, _ := client.AppsV1().Deployments(hotel).Get(ctx, "frontend", metav1.GetOptions{})
		zero := int32(0)
		deployment.Spec.Replicas = &zero
		deployment.Spec.Template.Spec.Containers[0].Env[1].Value = "wrong"
		_, _ = client.AppsV1().Deployments(hotel).Update(ctx, deployment, metav1.UpdateOptions{})
		configMap, _ := client.CoreV1().ConfigMaps(hotel).Get(ctx, "mongo-geo-script", metav1.GetOptions{})
		configMap.Data["k8s-geo-mongo.sh"] = "echo broken"
		_, _ = client.CoreV1().ConfigMaps(hotel).Update(ctx, configMap, metav1.UpdateOptions{})
		state, _ := client.CoreV1().ConfigMaps(hotel).Get(ctx, "sdo-controller-state", metav1.GetOptions{})
		state.Data["state"] = "v2"
		_, _ = client.CoreV1().ConfigMaps(hotel).Update(ctx, state, metav1.UpdateOptions{})

		var changes *StateChanges
		eventually(t, "four application changes", func() bool {
			changes = tracker.Changes(start.Add(5 * time.Second))
			return changes != nil && len(changes.Changes) == 4
		})
		if !changes.BaselineAt.Equal(start) {
			t.Fatalf("baseline time: %s", changes.BaselineAt)
		}
		selector := findChange(changes, "Service", "frontend")
		if selector == nil || selector.Change != StateChangeModified {
			t.Fatalf("the selector change must be reported: %+v", changes)
		}
		if got := field(selector, "selector"); got == nil || got.Before != "io.kompose.service=frontend" ||
			got.After != "current_service_name=frontend,io.kompose.service=frontend" {
			t.Fatalf("selector before/after: %+v", selector.Fields)
		}
		if policy := findChange(changes, "NetworkPolicy", "deny-all"); policy == nil || policy.Change != StateChangeAdded {
			t.Fatalf("a new NetworkPolicy must be reported as added: %+v", changes)
		}
		workload := findChange(changes, "Deployment", "frontend")
		if workload == nil || field(workload, "replicas") == nil || field(workload, "replicas").After != "0" {
			t.Fatalf("replicas must be reported: %+v", workload)
		}
		env := field(workload, "container[hotel-reserv-frontend].env")
		if env == nil || strings.Contains(env.After, "wrong") || strings.Contains(env.Before, "hunter2") ||
			!strings.Contains(env.After, "JAEGER_SAMPLE_RATIO=1") {
			t.Fatalf("env changes are shown, secret-looking values only as digests: %+v", env)
		}
		data := findChange(changes, "ConfigMap", "mongo-geo-script")
		if data == nil || field(data, "data[k8s-geo-mongo.sh]") == nil || field(data, "data[extra.sh]") != nil {
			t.Fatalf("a ConfigMap change names the changed key only: %+v", data)
		}
		for _, change := range changes.Changes {
			if strings.HasPrefix(change.Name, "failure-admin") || strings.HasPrefix(change.Name, "sdo-") {
				t.Fatalf("decoys present at baseline and SDO's own state must not appear: %+v", change)
			}
		}
		if len(changes.UnobservedKinds) != 1 || changes.UnobservedKinds[0] != "Secret" {
			t.Fatalf("the fake typed client has no Secret metadata transport: %v", changes.UnobservedKinds)
		}
	})
}

func TestStateBaselineRollsForwardOnlyAfterSustainedHealth(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		client := fake.NewSimpleClientset(hotelObjects()...)
		tracker := startedTracker(t, client)
		start := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
		tracker.Observe(start, true)

		service, _ := client.CoreV1().Services(hotel).Get(ctx, "frontend", metav1.GetOptions{})
		service.Spec.Ports[0].TargetPort = intstr.FromInt(5001)
		_, _ = client.CoreV1().Services(hotel).Update(ctx, service, metav1.UpdateOptions{})
		eventually(t, "the port change", func() bool { return findChange(tracker.Changes(start), "Service", "frontend") != nil })

		// A fault's first, not yet persisted, observation interrupts health, and
		// a later healthy blip must not absorb the change into the baseline.
		tracker.Observe(start.Add(2*time.Second), false)
		tracker.Observe(start.Add(3*time.Second), true)
		tracker.Observe(start.Add(50*time.Second), true)
		if findChange(tracker.Changes(start.Add(51*time.Second)), "Service", "frontend") == nil {
			t.Fatalf("a change must stay visible until the new state has been healthy for the settle period")
		}
		// Healthy for longer than the settle period: the change is accepted.
		tracker.Observe(start.Add(64*time.Second), true)
		if changes := tracker.Changes(start.Add(65 * time.Second)); findChange(changes, "Service", "frontend") != nil ||
			!changes.BaselineAt.Equal(start.Add(3*time.Second)) {
			t.Fatalf("a settled healthy state becomes the new baseline: %+v", changes)
		}

		tracker.Reset()
		if changes := tracker.Changes(start.Add(70 * time.Second)); changes != nil {
			t.Fatalf("after a maintenance reset there is no baseline until the next healthy observation: %+v", changes)
		}
	})
}

// A typed Secret client alone never authorizes a Secret read.
func TestStateTrackerReportsMissingSecretMetadataClient(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := fake.NewSimpleClientset(hotelObjects()...)
		tracker := startedTracker(t, client)
		start := time.Unix(1700000000, 0).UTC()
		tracker.Observe(start, true)
		changes := tracker.Changes(start)
		if changes == nil || len(changes.UnobservedKinds) != 1 || changes.UnobservedKinds[0] != "Secret" {
			t.Fatalf("an unreadable kind is named, not silently skipped: %+v", changes)
		}
	})
}

func TestStateChangesBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		objects := hotelObjects()
		for index := 0; index < 400; index++ {
			objects = append(objects, &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("config-%03d", index), Namespace: hotel},
				Data:       map[string]string{"value": strings.Repeat("x", 2048)},
			})
		}
		client := fake.NewSimpleClientset(objects...)
		tracker := startedTracker(t, client)
		start := time.Unix(1700000000, 0).UTC()
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
		changes = tracker.Changes(start)
		if len(changes.Changes) > maxStateChanges || changes.Omitted != 400-len(changes.Changes) {
			t.Fatalf("the diff must be capped and count what it omits: %d shown, %d omitted", len(changes.Changes), changes.Omitted)
		}
		if changes.Changes[0].Change != StateChangeRemoved {
			t.Fatalf("removals are reported: %+v", changes.Changes[0])
		}
	})
}

// The controller starts the tracker with a context that only bounds the
// initial sync (run.go cancels it when Start returns). Observation must
// continue until Stop: a live run reported "no change" for a deny-all
// NetworkPolicy created 1.5 s before dispatch because the informers had
// stopped with the start context.
func TestStateTrackerKeepsObservingAfterItsStartContextEnds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := fake.NewSimpleClientset(hotelObjects()...)
		tracker := newTrackerForTest(t, StateTrackerConfig{Client: client, Namespace: hotel, ResyncPeriod: time.Hour})
		startCtx, cancelStart := context.WithTimeout(context.Background(), 30*time.Second)
		if err := tracker.Start(startCtx); err != nil {
			t.Fatal(err)
		}
		cancelStart()
		t.Cleanup(tracker.Stop)
		start := time.Date(2026, 9, 28, 7, 58, 49, 0, time.UTC)
		tracker.Observe(start, true)

		_, _ = client.NetworkingV1().NetworkPolicies(hotel).Create(context.Background(), &networkingv1.NetworkPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "deny-all-recommendation", Namespace: hotel},
			Spec: networkingv1.NetworkPolicySpec{
				PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress},
			},
		}, metav1.CreateOptions{})

		eventually(t, "the NetworkPolicy created after the start context ended", func() bool {
			policy := findChange(tracker.Changes(start.Add(3*time.Second)), "NetworkPolicy", "deny-all-recommendation")
			return policy != nil && policy.Change == StateChangeAdded
		})
	})
}

// Stop still ends observation when the start context lives on.
func TestStateTrackerStopEndsObservation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := fake.NewSimpleClientset(hotelObjects()...)
		tracker := newTrackerForTest(t, StateTrackerConfig{Client: client, Namespace: hotel, ResyncPeriod: time.Hour})
		if err := tracker.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		tracker.Observe(time.Date(2026, 9, 28, 8, 0, 0, 0, time.UTC), true)
		tracker.Stop()
		if changes := tracker.Changes(time.Date(2026, 9, 28, 8, 0, 1, 0, time.UTC)); changes != nil {
			t.Fatalf("a stopped tracker reports no diff: %+v", changes)
		}
	})
}

func TestHealthyBaselineDoesNotAbsorbAChangeAtTheEndOfSettle(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset(hotelObjects()...)
	tracker := startedTracker(t, client)
	start := time.Unix(100, 0)
	tracker.Observe(start, true)
	tracker.Observe(start.Add(time.Second), true)
	// A new change can still be below detector persistence on this observation.
	// Only the candidate captured at the beginning of the clear stretch qualified.
	service, _ := client.CoreV1().Services(hotel).Get(ctx, "frontend", metav1.GetOptions{})
	service.Spec.Ports[0].TargetPort = intstr.FromInt(5001)
	if _, err := client.CoreV1().Services(hotel).Update(ctx, service, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the late port change", func() bool { return findChange(tracker.Changes(start), "Service", "frontend") != nil })
	tracker.Observe(start.Add(62*time.Second), true)
	if findChange(tracker.Changes(start.Add(62*time.Second)), "Service", "frontend") == nil {
		t.Fatal("late change was absorbed without its own healthy settle")
	}
}


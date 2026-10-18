package core

import (
	"context"
	"reflect"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestKubernetesSnapshotProviderListsNamespaceState(t *testing.T) {
	client := fake.NewSimpleClientset(
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "api-config", Namespace: "demo"},
			Data:       map[string]string{"endpoint": "http://backend"},
		},
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "demo"},
			Spec:       corev1.ServiceSpec{Selector: map[string]string{"app": "api"}},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "api-1", Namespace: "demo", Labels: map[string]string{"app": "api"}},
		},
		&corev1.Endpoints{
			ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "demo"},
			Subsets: []corev1.EndpointSubset{
				{Addresses: []corev1.EndpointAddress{{IP: "10.0.0.10"}}},
			},
		},
		&discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{Name: "api-slice", Namespace: "demo"},
		},
		&networkingv1.NetworkPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "deny-api", Namespace: "demo"},
		},
		&corev1.Event{
			ObjectMeta:     metav1.ObjectMeta{Name: "api-event", Namespace: "demo"},
			InvolvedObject: corev1.ObjectReference{Kind: "Service", Namespace: "demo", Name: "api"},
		},
	)
	provider := KubernetesSnapshotProvider{Namespace: "demo", Client: client}

	snapshot, err := provider.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	if got := snapshot.Namespace(); got != "demo" {
		t.Fatalf("unexpected namespace %q", got)
	}
	configMaps := snapshot.ConfigMaps()
	if len(configMaps) != 1 || configMaps[0].Name != "api-config" {
		t.Fatalf("expected ConfigMap, got %#v", configMaps)
	}
	configMaps[0].Data["endpoint"] = "mutated"
	if got := snapshot.ConfigMaps()[0].Data["endpoint"]; got != "http://backend" {
		t.Fatalf("ConfigMaps leaked mutable snapshot state: %q", got)
	}
	if got := snapshot.ReadyEndpointCountForService("demo", "api"); got != 1 {
		t.Fatalf("expected one endpoint, got %d", got)
	}
	if got := snapshot.PodsForService("demo", "api"); len(got) != 1 || got[0].Name != "api-1" {
		t.Fatalf("expected matching pod, got %#v", got)
	}
	if got := snapshot.RecentEventsFor("demo", "Service", "api"); len(got) != 1 || got[0].Name != "api-event" {
		t.Fatalf("expected service event, got %#v", got)
	}
	policies := snapshot.NetworkPolicies()
	if len(policies) != 1 || policies[0].Name != "deny-api" {
		t.Fatalf("expected NetworkPolicy, got %#v", policies)
	}
}

func TestDetectionSnapshotResourceAccessorsAreSortedAndDefensiveCopies(t *testing.T) {
	snapshot := detectionSnapshotFixture()

	assertCoreSortedDefensiveCopies(t, "Services", snapshot.Services, func(item corev1.Service) string { return item.Name }, func(item *corev1.Service) {
		item.Spec.Selector["state"] = "mutated"
	}, func(item corev1.Service) string { return item.Spec.Selector["state"] })
	assertCoreSortedDefensiveCopies(t, "Pods", snapshot.Pods, func(item corev1.Pod) string { return item.Name }, func(item *corev1.Pod) {
		item.Labels["state"] = "mutated"
	}, func(item corev1.Pod) string { return item.Labels["state"] })
	assertCoreSortedDefensiveCopies(t, "Deployments", snapshot.Deployments, func(item appsv1.Deployment) string { return item.Name }, func(item *appsv1.Deployment) {
		item.Spec.Template.Labels["state"] = "mutated"
	}, func(item appsv1.Deployment) string { return item.Spec.Template.Labels["state"] })
	assertCoreSortedDefensiveCopies(t, "ReplicaSets", snapshot.ReplicaSets, func(item appsv1.ReplicaSet) string { return item.Name }, func(item *appsv1.ReplicaSet) {
		item.Spec.Selector.MatchLabels["state"] = "mutated"
	}, func(item appsv1.ReplicaSet) string { return item.Spec.Selector.MatchLabels["state"] })
	assertCoreSortedDefensiveCopies(t, "Endpoints", snapshot.Endpoints, func(item corev1.Endpoints) string { return item.Name }, func(item *corev1.Endpoints) {
		item.Subsets[0].Addresses[0].IP = "mutated"
	}, func(item corev1.Endpoints) string { return item.Subsets[0].Addresses[0].IP })
	assertCoreSortedDefensiveCopies(t, "EndpointSlices", snapshot.EndpointSlices, func(item discoveryv1.EndpointSlice) string { return item.Name }, func(item *discoveryv1.EndpointSlice) {
		item.Endpoints[0].Addresses[0] = "mutated"
	}, func(item discoveryv1.EndpointSlice) string { return item.Endpoints[0].Addresses[0] })
	assertCoreSortedDefensiveCopies(t, "NetworkPolicies", snapshot.NetworkPolicies, func(item networkingv1.NetworkPolicy) string { return item.Name }, func(item *networkingv1.NetworkPolicy) {
		item.Spec.PodSelector.MatchLabels["state"] = "mutated"
	}, func(item networkingv1.NetworkPolicy) string { return item.Spec.PodSelector.MatchLabels["state"] })
	assertCoreSortedDefensiveCopies(t, "Events", snapshot.Events, func(item corev1.Event) string { return item.Name }, func(item *corev1.Event) {
		item.Annotations["state"] = "mutated"
	}, func(item corev1.Event) string { return item.Annotations["state"] })
}

func TestDetectionSnapshotHelperResultsAreSortedAndDefensiveCopies(t *testing.T) {
	snapshot := detectionSnapshotFixture()

	pods := snapshot.PodsForService("demo", "a")
	if got := []string{pods[0].Name, pods[1].Name}; !reflect.DeepEqual(got, []string{"a", "z"}) {
		t.Fatalf("expected sorted service pods, got %v", got)
	}
	pods[0].Labels["state"] = "mutated"
	if got := snapshot.PodsForService("demo", "a")[0].Labels["state"]; got != "original" {
		t.Fatalf("PodsForService leaked mutable snapshot state: %q", got)
	}

	events := snapshot.RecentEventsFor("demo", "Service", "a")
	if got := []string{events[0].Name, events[1].Name}; !reflect.DeepEqual(got, []string{"a", "z"}) {
		t.Fatalf("expected sorted recent events, got %v", got)
	}
	events[0].Annotations["state"] = "mutated"
	if got := snapshot.RecentEventsFor("demo", "Service", "a")[0].Annotations["state"]; got != "original" {
		t.Fatalf("RecentEventsFor leaked mutable snapshot state: %q", got)
	}
}

func detectionSnapshotFixture() DetectionSnapshot {
	metadata := func(name string) metav1.ObjectMeta {
		return metav1.ObjectMeta{
			Name: name, Namespace: "demo",
			Labels: map[string]string{"app": "api", "state": "original"}, Annotations: map[string]string{"state": "original"},
		}
	}
	return DetectionSnapshot{
		ServiceList: []corev1.Service{
			{ObjectMeta: metadata("z"), Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "api", "state": "original"}}},
			{ObjectMeta: metadata("a"), Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "api", "state": "original"}}},
		},
		PodList: []corev1.Pod{{ObjectMeta: metadata("z")}, {ObjectMeta: metadata("a")}},
		DeploymentList: []appsv1.Deployment{
			{ObjectMeta: metadata("z"), Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metadata("template-z")}}},
			{ObjectMeta: metadata("a"), Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metadata("template-a")}}},
		},
		ReplicaSetList: []appsv1.ReplicaSet{
			{ObjectMeta: metadata("z"), Spec: appsv1.ReplicaSetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"state": "original"}}}},
			{ObjectMeta: metadata("a"), Spec: appsv1.ReplicaSetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"state": "original"}}}},
		},
		EndpointList: []corev1.Endpoints{
			{ObjectMeta: metadata("z"), Subsets: []corev1.EndpointSubset{{Addresses: []corev1.EndpointAddress{{IP: "original"}}}}},
			{ObjectMeta: metadata("a"), Subsets: []corev1.EndpointSubset{{Addresses: []corev1.EndpointAddress{{IP: "original"}}}}},
		},
		EndpointSliceList: []discoveryv1.EndpointSlice{
			{ObjectMeta: metadata("z"), Endpoints: []discoveryv1.Endpoint{{Addresses: []string{"original"}}}},
			{ObjectMeta: metadata("a"), Endpoints: []discoveryv1.Endpoint{{Addresses: []string{"original"}}}},
		},
		NetworkPolicyList: []networkingv1.NetworkPolicy{
			{ObjectMeta: metadata("z"), Spec: networkingv1.NetworkPolicySpec{PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"state": "original"}}}},
			{ObjectMeta: metadata("a"), Spec: networkingv1.NetworkPolicySpec{PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"state": "original"}}}},
		},
		EventList: []corev1.Event{
			{ObjectMeta: metadata("z"), InvolvedObject: corev1.ObjectReference{Kind: "Service", Namespace: "demo", Name: "a"}},
			{ObjectMeta: metadata("a"), InvolvedObject: corev1.ObjectReference{Kind: "Service", Namespace: "demo", Name: "a"}},
		},
	}
}

func assertCoreSortedDefensiveCopies[T any](
	t *testing.T,
	name string,
	accessor func() []T,
	itemName func(T) string,
	mutate func(*T),
	nestedValue func(T) string,
) {
	t.Helper()
	first := accessor()
	if got := []string{itemName(first[0]), itemName(first[1])}; !reflect.DeepEqual(got, []string{"a", "z"}) {
		t.Fatalf("%s returned nondeterministic order: %v", name, got)
	}
	mutate(&first[0])
	if got := nestedValue(accessor()[0]); got != "original" {
		t.Fatalf("%s leaked mutable snapshot state: %q", name, got)
	}
}

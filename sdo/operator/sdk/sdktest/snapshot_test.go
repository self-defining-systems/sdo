package sdktest

import (
	"context"
	"reflect"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/sdk"
)

func TestSnapshotHelpers(t *testing.T) {
	snapshot := Snapshot{
		NamespaceName: "demo",
		ServiceList: []corev1.Service{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "demo"},
				Spec:       corev1.ServiceSpec{Selector: map[string]string{"app": "api"}},
			},
		},
		PodList: []corev1.Pod{
			{ObjectMeta: metav1.ObjectMeta{Name: "api-1", Namespace: "demo", Labels: map[string]string{"app": "api"}}},
			{ObjectMeta: metav1.ObjectMeta{Name: "worker-1", Namespace: "demo", Labels: map[string]string{"app": "worker"}}},
		},
		EndpointList: []corev1.Endpoints{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "demo"},
				Subsets: []corev1.EndpointSubset{
					{Addresses: []corev1.EndpointAddress{{IP: "10.0.0.1"}, {IP: "10.0.0.2"}}},
				},
			},
		},
		EventList: []corev1.Event{
			{
				ObjectMeta:     metav1.ObjectMeta{Name: "event-1", Namespace: "demo"},
				InvolvedObject: corev1.ObjectReference{Kind: "Service", Namespace: "demo", Name: "api"},
			},
		},
	}

	if got := snapshot.ReadyEndpointCountForService("demo", "api"); got != 2 {
		t.Fatalf("expected 2 ready endpoints, got %d", got)
	}
	if got := snapshot.PodsForService("demo", "api"); len(got) != 1 || got[0].Name != "api-1" {
		t.Fatalf("expected api pod, got %#v", got)
	}
	if got := snapshot.RecentEventsFor("demo", "Service", "api"); len(got) != 1 || got[0].Name != "event-1" {
		t.Fatalf("expected service event, got %#v", got)
	}
}

func TestSnapshotConfigMapsAreSortedAndDefensiveCopies(t *testing.T) {
	snapshot := Snapshot{
		NamespaceName: "demo",
		ConfigMapList: []corev1.ConfigMap{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "z-config", Namespace: "demo"},
				Data:       map[string]string{"setting": "original"},
			},
			{
				ObjectMeta: metav1.ObjectMeta{Name: "a-config", Namespace: "demo"},
				Data:       map[string]string{"setting": "first"},
			},
		},
	}

	first := snapshot.ConfigMaps()
	if got := []string{first[0].Name, first[1].Name}; !reflect.DeepEqual(got, []string{"a-config", "z-config"}) {
		t.Fatalf("expected deterministically sorted ConfigMaps, got %v", got)
	}
	first[1].Name = "mutated"
	first[1].Data["setting"] = "mutated"

	second := snapshot.ConfigMaps()
	if second[1].Name != "z-config" || second[1].Data["setting"] != "original" {
		t.Fatalf("ConfigMaps leaked mutable snapshot state: %#v", second[1])
	}
}

func TestSnapshotResourceAccessorsAreSortedAndDefensiveCopies(t *testing.T) {
	snapshot := resourceSnapshotFixture()

	assertSortedDefensiveCopies(t, "Services", snapshot.Services, func(item corev1.Service) string { return item.Name }, func(item *corev1.Service) {
		item.Spec.Selector["state"] = "mutated"
	}, func(item corev1.Service) string { return item.Spec.Selector["state"] })
	assertSortedDefensiveCopies(t, "Pods", snapshot.Pods, func(item corev1.Pod) string { return item.Name }, func(item *corev1.Pod) {
		item.Labels["state"] = "mutated"
	}, func(item corev1.Pod) string { return item.Labels["state"] })
	assertSortedDefensiveCopies(t, "Deployments", snapshot.Deployments, func(item appsv1.Deployment) string { return item.Name }, func(item *appsv1.Deployment) {
		item.Spec.Template.Labels["state"] = "mutated"
	}, func(item appsv1.Deployment) string { return item.Spec.Template.Labels["state"] })
	assertSortedDefensiveCopies(t, "ReplicaSets", snapshot.ReplicaSets, func(item appsv1.ReplicaSet) string { return item.Name }, func(item *appsv1.ReplicaSet) {
		item.Spec.Selector.MatchLabels["state"] = "mutated"
	}, func(item appsv1.ReplicaSet) string { return item.Spec.Selector.MatchLabels["state"] })
	assertSortedDefensiveCopies(t, "Endpoints", snapshot.Endpoints, func(item corev1.Endpoints) string { return item.Name }, func(item *corev1.Endpoints) {
		item.Subsets[0].Addresses[0].IP = "mutated"
	}, func(item corev1.Endpoints) string { return item.Subsets[0].Addresses[0].IP })
	assertSortedDefensiveCopies(t, "EndpointSlices", snapshot.EndpointSlices, func(item discoveryv1.EndpointSlice) string { return item.Name }, func(item *discoveryv1.EndpointSlice) {
		item.Endpoints[0].Addresses[0] = "mutated"
	}, func(item discoveryv1.EndpointSlice) string { return item.Endpoints[0].Addresses[0] })
	assertSortedDefensiveCopies(t, "Events", snapshot.Events, func(item corev1.Event) string { return item.Name }, func(item *corev1.Event) {
		item.Annotations["state"] = "mutated"
	}, func(item corev1.Event) string { return item.Annotations["state"] })
}

func TestSnapshotHelperResultsAreSortedAndDefensiveCopies(t *testing.T) {
	snapshot := resourceSnapshotFixture()

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

func resourceSnapshotFixture() Snapshot {
	metadata := func(name string) metav1.ObjectMeta {
		return metav1.ObjectMeta{Name: name, Namespace: "demo", Labels: map[string]string{"app": "api", "state": "original"}, Annotations: map[string]string{"state": "original"}}
	}
	return Snapshot{
		ServiceList: []corev1.Service{
			{ObjectMeta: metadata("z"), Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "api", "state": "original"}}},
			{ObjectMeta: metadata("a"), Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "api", "state": "original"}}},
		},
		PodList: []corev1.Pod{
			{ObjectMeta: metadata("z")},
			{ObjectMeta: metadata("a")},
		},
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
		EventList: []corev1.Event{
			{ObjectMeta: metadata("z"), InvolvedObject: corev1.ObjectReference{Kind: "Service", Namespace: "demo", Name: "a"}},
			{ObjectMeta: metadata("a"), InvolvedObject: corev1.ObjectReference{Kind: "Service", Namespace: "demo", Name: "a"}},
		},
	}
}

func assertSortedDefensiveCopies[T any](
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

func TestMissingConfigMapDetectorMatchesRequiredReferencesAcrossResourceNames(t *testing.T) {
	for _, test := range []struct {
		name       string
		deployment string
		configMap  string
	}{
		{name: "original resource names", deployment: "geo", configMap: "geo-config"},
		{name: "parameter-shifted resource names", deployment: "profile-v2", configMap: "profile-settings-v7"},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot := Snapshot{
				NamespaceName: "demo",
				DeploymentList: []appsv1.Deployment{
					deploymentWithConfigMapRef("demo", test.deployment, test.configMap, false),
				},
			}

			findings, err := detectMissingConfigMaps(context.Background(), snapshot)
			if err != nil {
				t.Fatalf("detect missing ConfigMaps: %v", err)
			}
			if len(findings) != 1 {
				t.Fatalf("expected one finding, got %#v", findings)
			}
			finding := findings[0]
			if finding.PrimaryResource.Name != test.deployment {
				t.Fatalf("expected deployment binding %q, got %#v", test.deployment, finding.PrimaryResource)
			}
			if got := finding.ParameterBindings["target_resource"]; got.Name != test.deployment || got.Kind != "Deployment" {
				t.Fatalf("unexpected target_resource binding: %#v", got)
			}
			if got := finding.ParameterBindings["missing_config_map"]; got.Name != test.configMap || got.Kind != "ConfigMap" {
				t.Fatalf("unexpected missing_config_map binding: %#v", got)
			}
		})
	}
}

func TestMissingConfigMapDetectorIgnoresOptionalAndExistingReferences(t *testing.T) {
	snapshot := Snapshot{
		NamespaceName: "demo",
		DeploymentList: []appsv1.Deployment{
			deploymentWithConfigMapRef("demo", "optional-user", "optional-config", true),
			deploymentWithConfigMapRef("demo", "existing-user", "existing-config", false),
		},
		ConfigMapList: []corev1.ConfigMap{
			{ObjectMeta: metav1.ObjectMeta{Name: "existing-config", Namespace: "demo"}},
		},
	}

	findings, err := detectMissingConfigMaps(context.Background(), snapshot)
	if err != nil {
		t.Fatalf("detect missing ConfigMaps: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected no findings, got %#v", findings)
	}
}

func deploymentWithConfigMapRef(namespace string, deployment string, configMap string, optional bool) appsv1.Deployment {
	return appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: deployment, Namespace: namespace},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "app",
							EnvFrom: []corev1.EnvFromSource{
								{
									ConfigMapRef: &corev1.ConfigMapEnvSource{
										LocalObjectReference: corev1.LocalObjectReference{Name: configMap},
										Optional:             &optional,
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func detectMissingConfigMaps(_ context.Context, snapshot sdk.DetectionContext) ([]sdk.Finding, error) {
	existing := make(map[string]struct{})
	for _, configMap := range snapshot.ConfigMaps() {
		existing[configMap.Namespace+"/"+configMap.Name] = struct{}{}
	}

	findings := make([]sdk.Finding, 0)
	for _, deployment := range snapshot.Deployments() {
		for _, reference := range sdk.ConfigMapReferencesForDeployment(deployment) {
			if reference.Optional {
				continue
			}
			if _, ok := existing[deployment.Namespace+"/"+reference.Name]; ok {
				continue
			}

			deploymentRef := sdk.ObjectRefFrom("Deployment", "apps/v1", &deployment)
			configMapRef := sdk.ObjectRef{
				APIVersion: "v1",
				Kind:       "ConfigMap",
				Namespace:  deployment.Namespace,
				Name:       reference.Name,
			}
			findings = append(findings, sdk.Finding{
				RuleID:          "missing-configmap",
				Status:          sdk.FindingActive,
				Severity:        sdk.SeverityCritical,
				Summary:         "deployment references a missing required ConfigMap",
				Evidence:        deployment.Name + " requires ConfigMap " + reference.Name,
				PrimaryResource: deploymentRef,
				RelatedResources: []sdk.ObjectRef{
					configMapRef,
				},
				Playbooks: []string{"missing-configmap"},
				ParameterBindings: map[string]sdk.ObjectRef{
					"target_resource":    deploymentRef,
					"missing_config_map": configMapRef,
				},
			})
		}
	}
	return findings, nil
}

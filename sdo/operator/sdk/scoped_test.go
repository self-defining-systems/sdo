package sdk_test

import (
	"encoding/json"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"net/url"
	"reflect"
	"sdo.dev/operator/sdk"
	"strings"
	"testing"
)

func TestObservationCeilingCannotBeWidened(t *testing.T) {
	target := sdk.ObservationTarget{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "foreign"}
	allowed := sdk.ObservationScope{Version: 1, Targets: []sdk.ObservationTarget{target}}
	if err := allowed.Within(sdk.ObservationScope{}); err == nil {
		t.Fatal("missing installer ceiling accepted")
	}
	if err := allowed.Within(allowed); err != nil {
		t.Fatal(err)
	}
	wider := allowed
	wider.Targets = append(append([]sdk.ObservationTarget{}, allowed.Targets...), sdk.ObservationTarget{APIVersion: "v1", Kind: "Service", Namespace: "other"})
	if err := wider.Within(allowed); err == nil {
		t.Fatal("architecture widened ceiling")
	}
}
func TestForeignDeploymentProjectionExcludesCredentials(t *testing.T) {
	checkProjection(t, "canary-credential")
}
func FuzzForeignDeploymentProjection(f *testing.F) {
	f.Add("canary-credential")
	f.Fuzz(func(t *testing.T, canary string) {
		if len(canary) < 8 || len(canary) > 4096 {
			t.Skip()
		}
		checkProjection(t, canary)
	})
}
func checkProjection(t *testing.T, canary string) {
	t.Helper()
	canary = "credential-canary-" + canary + "-end"
	object := appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "backend", Namespace: "foreign", Annotations: map[string]string{"note": canary}}}
	object.Spec.Template.Annotations = map[string]string{"note": canary}
	object.Spec.Template.Spec.Containers = []corev1.Container{{Name: "backend", Image: "backend:v1", Args: []string{canary}, Command: []string{canary}, Env: []corev1.EnvVar{{Name: "DATABASE_URL", Value: canary}}, EnvFrom: []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: canary}}}}}}
	projected := sdk.ProjectForeignDeployment(object)
	encoded, _ := json.Marshal(projected)
	if strings.Contains(string(encoded), canary) {
		t.Fatal("credential escaped projection")
	}
	credentialURL := "https://user:" + url.QueryEscape(canary) + "@backend.example/" + url.PathEscape(canary) + "?token=" + url.QueryEscape(canary)
	mutating := ProjectForTestWebhook(canary, credentialURL)
	projectedWebhook := sdk.ProjectMutatingWebhook(mutating)
	if !reflect.DeepEqual(projectedWebhook, sdk.ProjectMutatingWebhook(projectedWebhook)) {
		t.Fatal("webhook projection is not idempotent")
	}
	encoded, _ = json.Marshal(projectedWebhook)
	if strings.Contains(string(encoded), canary) || strings.Contains(string(encoded), url.QueryEscape(canary)) {
		t.Fatal("credential escaped webhook projection")
	}

	for _, other := range []any{
		sdk.ProjectForeignService(corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "backend", Namespace: "foreign", Annotations: map[string]string{"credential": canary}}}),
		sdk.ProjectForeignEndpoints(corev1.Endpoints{ObjectMeta: metav1.ObjectMeta{Name: "backend", Namespace: "foreign", Annotations: map[string]string{"credential": canary}}, Subsets: []corev1.EndpointSubset{{Addresses: []corev1.EndpointAddress{{IP: "127.0.0.1", TargetRef: &corev1.ObjectReference{Kind: "Secret", Name: canary}}}}}}),
		sdk.ProjectValidatingWebhook(admissionv1.ValidatingWebhookConfiguration{ObjectMeta: mutating.ObjectMeta, Webhooks: []admissionv1.ValidatingWebhook{{Name: "policy.example", ClientConfig: mutating.Webhooks[0].ClientConfig}}}),
	} {
		encoded, _ := json.Marshal(other)
		if strings.Contains(string(encoded), canary) {
			t.Fatal("credential escaped a newly observed kind")
		}
	}
	if object.Spec.Template.Spec.Containers[0].Env[0].Value != canary {
		t.Fatal("source mutated")
	}
}

func ProjectForTestWebhook(canary, credentialURL string) admissionv1.MutatingWebhookConfiguration {
	return admissionv1.MutatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: "policy", Annotations: map[string]string{"note": canary}}, Webhooks: []admissionv1.MutatingWebhook{{Name: "policy.example", ClientConfig: admissionv1.WebhookClientConfig{URL: &credentialURL, CABundle: []byte(canary)}}}}
}

func TestScopedPortCopiesAreIndependent(t *testing.T) {
	protocol := "http"
	service := corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "backend", Namespace: "foreign"}, Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "http", Port: 80, AppProtocol: &protocol}}}}
	endpoints := corev1.Endpoints{ObjectMeta: service.ObjectMeta, Subsets: []corev1.EndpointSubset{{Ports: []corev1.EndpointPort{{Name: "http", Port: 80, AppProtocol: &protocol}}}}}
	targets := []sdk.ObservationTarget{{APIVersion: "v1", Kind: "Service", Namespace: "foreign"}, {APIVersion: "v1", Kind: "Endpoints", Namespace: "foreign"}}
	snapshot := sdk.ScopedSnapshot{Version: 1, Scope: sdk.ObservationScope{Version: 1, Targets: targets}, Statuses: []sdk.ObservationStatus{{Target: targets[0], State: sdk.ObservationObserved}, {Target: targets[1], State: sdk.ObservationObserved}}, Services: []corev1.Service{service}, Endpoints: []corev1.Endpoints{endpoints}}
	services, err := snapshot.ScopedServices("foreign")
	if err != nil {
		t.Fatal(err)
	}
	*services[0].Spec.Ports[0].AppProtocol = "changed"
	again, _ := snapshot.ScopedServices("foreign")
	if *again[0].Spec.Ports[0].AppProtocol != "http" || protocol != "http" {
		t.Fatal("Service accessor mutated frozen/source ports")
	}
	eps, err := snapshot.ScopedEndpoints("foreign")
	if err != nil {
		t.Fatal(err)
	}
	*eps[0].Subsets[0].Ports[0].AppProtocol = "changed"
	againEps, _ := snapshot.ScopedEndpoints("foreign")
	if *againEps[0].Subsets[0].Ports[0].AppProtocol != "http" || protocol != "http" {
		t.Fatal("Endpoints accessor mutated frozen/source ports")
	}
}

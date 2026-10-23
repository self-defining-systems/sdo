package core

import (
	"context"
	"encoding/json"
	"errors"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"reflect"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"strings"
	"testing"
)

func TestScopedReadDenialRegrantAndCredentialSafeReplay(t *testing.T) {
	const canary = "sk2-canary-credential-value"
	url := "https://user:" + canary + "@example.org/path?token=" + canary
	deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "same", Namespace: "foreign", Annotations: map[string]string{"credential": canary}}}
	deployment.Spec.Template.Annotations = map[string]string{"credential": canary}
	deployment.Spec.Template.Spec.Containers = []corev1.Container{{Name: "backend", Image: "backend:v1", Env: []corev1.EnvVar{{Name: "DATABASE_URL", Value: canary}}, Args: []string{canary}, EnvFrom: []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: canary}}}}}}
	webhook := &admissionv1.ValidatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: "policy", Annotations: map[string]string{"credential": canary}}, Webhooks: []admissionv1.ValidatingWebhook{{Name: "policy.example", ClientConfig: admissionv1.WebhookClientConfig{URL: &url, CABundle: []byte(canary)}}}}
	client := fake.NewSimpleClientset(deployment, webhook)
	targets := []sdk.ObservationTarget{{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "foreign"}, {APIVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingWebhookConfiguration", Name: "policy"}}
	scope := sdk.ObservationScope{Version: 1, Targets: targets}
	denied := true
	client.PrependReactor("list", "deployments", func(a ktesting.Action) (bool, runtime.Object, error) {
		if denied {
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Group: "apps", Resource: "deployments"}, "", errors.New(canary))
		}
		return false, nil, nil
	})
	first, err := ObserveScoped(context.Background(), client, scope, scope, "commit")
	if err != nil {
		t.Fatal(err)
	}
	firstJSON, _ := json.Marshal(first)
	if strings.Contains(string(firstJSON), canary) {
		t.Fatal("server error credential leaked into snapshot/status")
	}
	if _, err = first.ScopedDeployments("foreign"); err == nil {
		t.Fatal("denied read appeared observed")
	}
	denied = false
	observed, err := ObserveScoped(context.Background(), client, scope, scope, "commit")
	if err != nil {
		t.Fatal(err)
	}
	objects, err := observed.ScopedDeployments("foreign")
	if err != nil || len(objects) != 1 {
		t.Fatalf("regrant: %v %+v", err, objects)
	}
	view := DetectionSnapshot{NamespaceName: "app", ScopedSnapshot: observed, DeploymentList: []appsv1.Deployment{{ObjectMeta: metav1.ObjectMeta{Name: "same", Namespace: "app"}}}}
	capture, err := sdktest.CaptureSnapshot(view, "app")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(capture)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), canary) {
		t.Fatal("credential leaked into capture")
	}
	var replay sdktest.Snapshot
	if err = json.Unmarshal(encoded, &replay); err != nil {
		t.Fatal(err)
	}
	replayObjects, err := replay.ScopedDeployments("foreign")
	if err != nil || len(replayObjects) != 1 || len(replay.Deployments()) != 1 {
		t.Fatalf("qualified replay failed: %v", err)
	}
	if !reflect.DeepEqual(observed, replay.ScopedSnapshot) {
		t.Fatal("scoped capture/replay changed frozen observations")
	}
	hooks, err := replay.ValidatingWebhookConfigurations()
	if err != nil || len(hooks) != 1 || !strings.HasPrefix(string(hooks[0].Webhooks[0].ClientConfig.CABundle), "sha256:") {
		t.Fatalf("webhook replay: %v", err)
	}
	if deployment.Spec.Template.Spec.Containers[0].Env[0].Value != canary {
		t.Fatal("source mutated")
	}
	for _, a := range client.Actions() {
		if a.GetResource().Resource == "secrets" {
			t.Fatal("foreign Secret was read")
		}
		if a.GetResource().Resource == "validatingwebhookconfigurations" && a.GetVerb() != "get" {
			t.Fatal("webhook read was not named")
		}
	}
}
func TestScopedNotFoundWebhookIsObservedAbsence(t *testing.T) {
	target := sdk.ObservationTarget{APIVersion: "admissionregistration.k8s.io/v1", Kind: "MutatingWebhookConfiguration", Name: "missing"}
	scope := sdk.ObservationScope{Version: 1, Targets: []sdk.ObservationTarget{target}}
	observed, err := ObserveScoped(context.Background(), fake.NewSimpleClientset(), scope, scope, "commit")
	if err != nil {
		t.Fatal(err)
	}
	objects, err := observed.MutatingWebhookConfigurations()
	if err != nil || len(objects) != 0 {
		t.Fatalf("not found became unavailable: %v", err)
	}
}

func TestScopedMissingWebhookAPIIsUnknown(t *testing.T) {
	target := sdk.ObservationTarget{APIVersion: "admissionregistration.k8s.io/v1", Kind: "MutatingWebhookConfiguration", Name: "policy"}
	scope := sdk.ObservationScope{Version: 1, Targets: []sdk.ObservationTarget{target}}
	client := fake.NewSimpleClientset()
	client.PrependReactor("get", "mutatingwebhookconfigurations", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewNotFound(schema.GroupResource{Group: "admissionregistration.k8s.io", Resource: "mutatingwebhookconfigurations"}, "")
	})
	observed, err := ObserveScoped(context.Background(), client, scope, scope, "commit")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = observed.MutatingWebhookConfigurations(); err == nil {
		t.Fatal("missing API became observed absence")
	}
}

func TestScopedCapturePreservesMixedWebhookEvidence(t *testing.T) {
	for _, kind := range []string{"MutatingWebhookConfiguration", "ValidatingWebhookConfiguration"} {
		t.Run(kind, func(t *testing.T) {
			a := sdk.ObservationTarget{APIVersion: "admissionregistration.k8s.io/v1", Kind: kind, Name: "readable"}
			b := a
			b.Name = "denied"
			scope := sdk.ObservationScope{Version: 1, Targets: []sdk.ObservationTarget{a, b}}
			client := fake.NewSimpleClientset(
				&admissionv1.MutatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: a.Name}},
				&admissionv1.ValidatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: a.Name}},
			)
			client.PrependReactor("get", "*", func(action ktesting.Action) (bool, runtime.Object, error) {
				if action.(ktesting.GetAction).GetName() == b.Name {
					return true, nil, apierrors.NewForbidden(action.GetResource().GroupResource(), b.Name, errors.New("denied"))
				}
				return false, nil, nil
			})
			observed, err := ObserveScoped(context.Background(), client, scope, scope, "commit")
			if err != nil {
				t.Fatal(err)
			}
			capture, err := sdktest.CaptureSnapshot(DetectionSnapshot{NamespaceName: "app", ScopedSnapshot: observed}, "app")
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(capture)
			if err != nil {
				t.Fatal(err)
			}
			var replay sdktest.Snapshot
			if err := json.Unmarshal(encoded, &replay); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(observed, replay.ScopedSnapshot) {
				t.Fatal("mixed availability discarded successfully observed webhook evidence")
			}
			if kind == "MutatingWebhookConfiguration" {
				if _, err := replay.MutatingWebhookConfigurations(); err == nil {
					t.Fatal("partial aggregate appeared complete")
				}
			} else {
				if _, err := replay.ValidatingWebhookConfigurations(); err == nil {
					t.Fatal("partial aggregate appeared complete")
				}
			}
		})
	}
}

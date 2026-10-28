package runtime

import (
	"encoding/json"
	"fmt"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"strings"
	"testing"
)

func int32Ptr(v int32) *int32 { return &v }

// admissionCluster is a social-network-shaped cluster: a mutating webhook whose
// backend Service lives in another namespace, a validating webhook that exempts
// the app, and one that watches only Deployments.
func admissionCluster() *fake.Clientset {
	fail, ignore := admissionv1.Fail, admissionv1.Ignore
	podCreate := []admissionv1.RuleWithOperations{{Operations: []admissionv1.OperationType{admissionv1.Create}, Rule: admissionv1.Rule{APIGroups: []string{""}, APIVersions: []string{"v1"}, Resources: []string{"pods"}}}}
	port := int32(8443)
	return fake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "app", Labels: map[string]string{"team": "shop"}}},
		&admissionv1.MutatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: "pod-policy"}, Webhooks: []admissionv1.MutatingWebhook{{
			Name: "pod-policy.validation.k8s.io", FailurePolicy: &fail, Rules: podCreate,
			NamespaceSelector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "team", Operator: metav1.LabelSelectorOpIn, Values: []string{"shop"}}}},
			ClientConfig:      admissionv1.WebhookClientConfig{CABundle: []byte("synthetic-ca"), Service: &admissionv1.ServiceReference{Namespace: "platform", Name: "pod-policy", Port: &port}},
		}}},
		&admissionv1.ValidatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: "exempt"}, Webhooks: []admissionv1.ValidatingWebhook{{
			Name: "exempt.example", FailurePolicy: &ignore, Rules: podCreate,
			NamespaceSelector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "team", Operator: metav1.LabelSelectorOpNotIn, Values: []string{"shop"}}}},
			ClientConfig:      admissionv1.WebhookClientConfig{Service: &admissionv1.ServiceReference{Namespace: "platform", Name: "exempt"}},
		}}},
		&admissionv1.ValidatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: "deployments-only"}, Webhooks: []admissionv1.ValidatingWebhook{{
			Name: "deployments.example", FailurePolicy: &fail,
			Rules:        []admissionv1.RuleWithOperations{{Operations: []admissionv1.OperationType{admissionv1.Create}, Rule: admissionv1.Rule{APIGroups: []string{"apps"}, APIVersions: []string{"v1"}, Resources: []string{"deployments"}}}},
			ClientConfig: admissionv1.WebhookClientConfig{URL: func() *string { u := "https://example.invalid/validate"; return &u }()},
		}}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "pod-policy"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "pod-policy"}}},
		&corev1.Endpoints{ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "pod-policy"}, Subsets: []corev1.EndpointSubset{{Addresses: []corev1.EndpointAddress{{IP: "10.0.0.1"}}}}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "pod-policy-0", Labels: map[string]string{"app": "pod-policy"}}, Spec: corev1.PodSpec{Volumes: []corev1.Volume{
			{Name: "tls", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "pod-policy-tls", Items: []corev1.KeyToPath{{Key: "tls.crt", Path: "server.crt"}}}}},
			{Name: "cfg", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "unrelated"}}}},
		}}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "platform", Name: "pod-policy-tls"}, Data: map[string][]byte{"tls.crt": []byte("private-canary")}},
	)
}

func TestAdmissionWebhooksFollowFailingNamespaceAndWorkload(t *testing.T) {
	client := admissionCluster()
	var secretReads int
	client.PrependReactor("*", "secrets", func(ktesting.Action) (bool, kruntime.Object, error) {
		secretReads++
		return false, nil, nil
	})
	view := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app"}, Client: client}
	for _, subject := range []sdk.ObjectRef{
		{APIVersion: "v1", Kind: "Namespace", Name: "app"},
		{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"},
	} {
		got, err := view.AdmissionWebhooksFor(subject)
		if err != nil {
			t.Fatalf("%s: %v", subject.Kind, err)
		}
		if len(got) != 1 {
			t.Fatalf("%s: want only the pod-policy entry, got %+v", subject.Kind, got)
		}
		w := got[0]
		if w.Configuration.Kind != "MutatingWebhookConfiguration" || w.Configuration.Name != "pod-policy" || w.Webhook != "pod-policy.validation.k8s.io" || w.FailurePolicy != "Fail" {
			t.Fatalf("%s: wrong entry %+v", subject.Kind, w)
		}
		if w.Service == nil || w.Service.Namespace != "platform" || w.Service.Name != "pod-policy" || w.Port != 8443 || w.ReadyEndpoints != 1 {
			t.Fatalf("%s: wrong backend %+v", subject.Kind, w)
		}
		if len(w.ServingSecrets) != 1 || w.ServingSecrets[0] != (program.WebhookSecretKey{Namespace: "platform", Name: "pod-policy-tls", Key: "tls.crt"}) {
			t.Fatalf("%s: wrong serving secrets %+v", subject.Kind, w.ServingSecrets)
		}
		if err := w.CATarget(w.ServingSecrets[0]).Validate(); err != nil {
			t.Fatalf("%s: relation does not yield a valid CA target: %v", subject.Kind, err)
		}
		raw, _ := json.Marshal(got)
		if strings.Contains(string(raw), "private-canary") || strings.Contains(string(raw), "synthetic-ca") || !strings.Contains(w.CABundle, "sha256:") {
			t.Fatalf("%s: private bytes crossed the relation: %s", subject.Kind, raw)
		}
	}
	if secretReads != 0 {
		t.Fatalf("relation read %d Secrets", secretReads)
	}
}

func TestAdmissionWebhooksAreBounded(t *testing.T) {
	var objects []kruntime.Object
	for i := 0; i < program.MaxAdmissionWebhooks+8; i++ {
		objects = append(objects, &admissionv1.ValidatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("cfg-%02d", i)}, Webhooks: []admissionv1.ValidatingWebhook{{
			Name: fmt.Sprintf("w%02d.example", i), ClientConfig: admissionv1.WebhookClientConfig{URL: func() *string { u := "https://example.invalid"; return &u }()},
			Rules: []admissionv1.RuleWithOperations{{Operations: []admissionv1.OperationType{"*"}, Rule: admissionv1.Rule{APIGroups: []string{"*"}, APIVersions: []string{"*"}, Resources: []string{"*"}}}},
		}}})
	}
	objects = append(objects, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "app"}})
	view := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app"}, Client: fake.NewSimpleClientset(objects...)}
	got, err := view.AdmissionWebhooksFor(sdk.ObjectRef{APIVersion: "v1", Kind: "Namespace", Name: "app"})
	if err != nil || len(got) != program.MaxAdmissionWebhooks {
		t.Fatalf("got %d err=%v, want %d", len(got), err, program.MaxAdmissionWebhooks)
	}
}

func TestAdmissionWebhooksRejectSubjectsWithoutANamespace(t *testing.T) {
	view := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app"}, Client: admissionCluster()}
	for _, subject := range []sdk.ObjectRef{{APIVersion: "v1", Kind: "Node", Name: "n1"}, {APIVersion: "v1", Kind: "Secret", Namespace: "app", Name: "s"}, {}} {
		if _, err := view.AdmissionWebhooksFor(subject); err == nil {
			t.Fatalf("subject %+v accepted", subject)
		}
	}
}

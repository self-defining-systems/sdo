package runtime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"google.golang.org/protobuf/encoding/protojson"
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
		&admissionv1.MutatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: "pod-policy", UID: "uid-1", ResourceVersion: "1"}, Webhooks: []admissionv1.MutatingWebhook{{
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

// webhookRepair is the shape a learned program takes: it binds only the
// failing Namespace or workload and finds the webhook through the relation.
type webhookRepair struct{}

func (webhookRepair) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "webhook-repair", Params: []program.ParamSpec{
		{Name: "namespace", Kind: "Namespace"}, {Name: "workload", Kind: "Deployment"},
	}}
}

func (webhookRepair) Mitigate(_ context.Context, view program.ReadView, b program.Bindings) (program.Plan, error) {
	subject := b.Objects["namespace"]
	if subject.Name == "" {
		subject = b.Objects["workload"]
	}
	webhooks, err := view.AdmissionWebhooksFor(subject)
	if err != nil {
		return program.Plan{}, err
	}
	var plan program.Plan
	for _, w := range webhooks {
		for _, secret := range w.ServingSecrets {
			target := w.CATarget(secret)
			if matches, err := view.WebhookCAMatchesSecret(target); err == nil && !matches {
				plan.Actions = append(plan.Actions, program.SyncWebhookCA{Target: target, Reason: "trust the serving certificate"})
			}
		}
	}
	return plan, nil
}

// A finding shape that admission failures really produce (the controller's
// admission-blocked rollout finding on a workload, and a detector's finding on
// the Namespace) binds the program, and the relation reaches the webhook
// configuration and Secret the repair needs.
func TestAdmissionFindingsBindWebhookRepairAndRepair(t *testing.T) {
	findings := map[string]sdk.Finding{
		"workload finding":  deploymentRolloutFinding(sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}, "admission_blocked", "x509: certificate signed by unknown authority"),
		"namespace finding": {PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Namespace", Name: "app"}, Playbooks: []string{"webhook-repair"}},
	}
	for name, finding := range findings {
		t.Run(name, func(t *testing.T) {
			bindings, err := program.BindFinding((webhookRepair{}).Spec(), finding)
			if err != nil {
				t.Fatalf("finding does not bind the program: %v", err)
			}
			client := admissionCluster()
			view := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app"}, Client: client}
			plan, err := (webhookRepair{}).Mitigate(context.Background(), view, bindings)
			if err != nil || len(plan.Actions) != 1 {
				t.Fatalf("plan %+v err=%v", plan, err)
			}
			executor := PlanExecutor{Client: TypedPlanClient{Client: client}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
			if result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, plan, false); err != nil || !result.Applied {
				t.Fatalf("apply: %v", err)
			}
			config, err := client.AdmissionregistrationV1().MutatingWebhookConfigurations().Get(context.Background(), "pod-policy", metav1.GetOptions{})
			if err != nil || string(config.Webhooks[0].ClientConfig.CABundle) != "private-canary" {
				t.Fatalf("webhook CA not repaired: %v", err)
			}
			if again, _ := (webhookRepair{}).Mitigate(context.Background(), view, bindings); len(again.Actions) != 0 {
				t.Fatal("repaired webhook still planned")
			}
		})
	}
}

// Repairing the service reference or failure policy of a webhook is an ordinary
// PatchObject; it must not rewrite the private caBundle it never names, and the
// durable receipt must not carry that bundle.
func TestWebhookEntryPatchLeavesPrivateCABundle(t *testing.T) {
	const canary = "synthetic-private-ca"
	encoded := base64.StdEncoding.EncodeToString([]byte(canary))
	ref := sdk.ObjectRef{APIVersion: "admissionregistration.k8s.io/v1", Kind: "MutatingWebhookConfiguration", Name: "pod-policy"}
	client := generalDiscoveryClient(ref, "mutatingwebhookconfigurations", false)
	executor := PlanExecutor{Client: client, Rollouts: generalNoRollouts{}, Verifier: generalHealthy{}}
	incident := IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}
	desired := map[string]any{"apiVersion": ref.APIVersion, "kind": ref.Kind, "metadata": map[string]any{"name": ref.Name}, "webhooks": []any{map[string]any{
		"name": "pod-policy.validation.k8s.io", "failurePolicy": "Fail",
		"clientConfig": map[string]any{"caBundle": encoded, "service": map[string]any{"namespace": "platform", "name": "pod-policy"}},
	}}}
	if _, err := executor.Apply(context.Background(), incident, program.Plan{Actions: []program.Action{program.ApplyObject{Ref: ref, Mode: "create", Object: desired}}}, false); err != nil {
		t.Fatal(err)
	}
	patch := program.PatchObject{Ref: ref, Type: "json", Patch: json.RawMessage(`[{"op":"replace","path":"/webhooks/0/failurePolicy","value":"Ignore"},{"op":"replace","path":"/webhooks/0/clientConfig/service/name","value":"pod-policy-v2"}]`), Reason: "restore the webhook backend reference"}
	result, err := executor.Apply(context.Background(), incident, program.Plan{Actions: []program.Action{patch}}, false)
	if err != nil || !result.Applied {
		t.Fatalf("patch: %v", err)
	}
	got, err := client.Get(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	webhook := got.Object["webhooks"].([]any)[0].(map[string]any)
	config := webhook["clientConfig"].(map[string]any)
	if webhook["failurePolicy"] != "Ignore" || config["service"].(map[string]any)["name"] != "pod-policy-v2" || config["caBundle"] != encoded {
		t.Fatalf("patch wrong or clobbered the CA: %+v", webhook)
	}
	wire, err := result.Proto()
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := protojson.Marshal(wire)
	if strings.Contains(string(raw), canary) || strings.Contains(string(raw), encoded) {
		t.Fatal("CA bytes reached the durable receipt")
	}
	// Undo restores the policy and reference and leaves the bundle as it was.
	if err := executor.Rollback(context.Background(), result); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	got, _ = client.Get(context.Background(), ref)
	webhook = got.Object["webhooks"].([]any)[0].(map[string]any)
	config = webhook["clientConfig"].(map[string]any)
	if webhook["failurePolicy"] != "Fail" || config["service"].(map[string]any)["name"] != "pod-policy" || config["caBundle"] != encoded {
		t.Fatalf("rollback wrong or changed the CA: %+v", webhook)
	}
}

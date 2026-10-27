package runtime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/protobuf/encoding/protojson"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	ktesting "k8s.io/client-go/testing"
	"net/http"
	"net/http/httptest"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"strings"
	"testing"
)

func TestWebhookCASyncPrivateRuntimeBoundary(t *testing.T) {
	const canary = "synthetic-private-ca-canary"
	ref := sdk.ObjectRef{APIVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingWebhookConfiguration", Name: "policy"}
	target := program.WebhookCA{Ref: ref, Webhook: "policy.example", SecretNamespace: "other", SecretName: "serving", SecretKey: "ca.crt"}
	client := fake.NewSimpleClientset(&admissionv1.ValidatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: ref.Name, UID: "one", ResourceVersion: "1"}, Webhooks: []admissionv1.ValidatingWebhook{{Name: target.Webhook, ClientConfig: admissionv1.WebhookClientConfig{CABundle: []byte("wrong")}}}}, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "other", Name: "serving"}, Data: map[string][]byte{"ca.crt": []byte(canary)}})
	view := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app"}, Client: client}
	record := &sdktest.RecordingReadView{ReadView: view}
	matched, err := record.WebhookCAMatchesSecret(target)
	if err != nil || matched {
		t.Fatalf("before match=%v err=%v", matched, err)
	}
	executor := PlanExecutor{Client: TypedPlanClient{Client: client}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
	result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.SyncWebhookCA{Target: target, Reason: "restore webhook trust"}}}, false)
	if err != nil || !result.Applied {
		t.Fatalf("sync failed: %v", err)
	}
	matched, err = record.WebhookCAMatchesSecret(target)
	if err != nil || !matched {
		t.Fatalf("after match=%v err=%v", matched, err)
	}
	got, err := client.AdmissionregistrationV1().ValidatingWebhookConfigurations().Get(context.Background(), ref.Name, metav1.GetOptions{})
	if err != nil || string(got.Webhooks[0].ClientConfig.CABundle) != canary {
		t.Fatal("CA was not synchronized")
	}
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	calls, _ := json.Marshal(record.RecordedReads())
	for _, data := range [][]byte{raw, calls} {
		if strings.Contains(string(data), canary) || strings.Contains(string(data), base64.StdEncoding.EncodeToString([]byte(canary))) {
			t.Fatal("Secret bytes leaked")
		}
	}
	replay := &sdktest.ReplayReadView{Calls: record.RecordedReads()}
	if matched, err = replay.WebhookCAMatchesSecret(target); err != nil || matched {
		t.Fatal("open replay mismatch")
	}
	if matched, err = replay.WebhookCAMatchesSecret(target); err != nil || !matched {
		t.Fatal("fixed replay mismatch")
	}
	wire, err := result.Proto()
	if err != nil {
		t.Fatal(err)
	}
	if len(wire.GetActionReports()) != 1 || !strings.HasPrefix(wire.GetActionReports()[0].GetWebhookCaDigest(), "sha256:") {
		t.Fatal("durable digest missing")
	}
	restoredResult := planApplicationState(wire)
	if restoredResult.Result.ActionReports[0].WebhookCADigest != result.ActionReports[0].WebhookCADigest {
		t.Fatal("digest did not survive durable receipt")
	}
	var jsonResult PlanApplicationResult
	if err = json.Unmarshal(raw, &jsonResult); err != nil {
		t.Fatal(err)
	}
	if _, ok := jsonResult.Plan.Actions[0].(program.SyncWebhookCA); !ok {
		t.Fatal("private action lost its type on restore")
	}

	// Feed the actual post-write object through the capture boundary too.
	got.APIVersion = ref.APIVersion
	got.Kind = ref.Kind
	objectJSON, _ := json.Marshal(got)
	var object unstructured.Unstructured
	if err = json.Unmarshal(objectJSON, &object); err != nil {
		t.Fatal(err)
	}
	view.DetectionContext = sdktest.Snapshot{NamespaceName: "app", Objects: []unstructured.Unstructured{object}, ObjectSnapshotVersion: 1}
	record.ReadView = view
	wireJSON, err := protojson.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(wireJSON), canary) || strings.Contains(string(wireJSON), base64.StdEncoding.EncodeToString([]byte(canary))) {
		t.Fatal("private CA leaked to durable receipt")
	}
	snapshot, err := sdktest.CaptureSnapshot(record, "app", ref)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Objects) != 1 {
		t.Fatal("capture did not exercise post-write object")
	}
	capture, _ := json.Marshal(snapshot)
	if strings.Contains(string(capture), canary) || strings.Contains(string(capture), base64.StdEncoding.EncodeToString([]byte(canary))) {
		t.Fatal("Secret CA bytes leaked to capture")
	}

}

func TestWebhookCAKindsKeysAndFailurePrivacy(t *testing.T) {
	for _, kind := range []string{"ValidatingWebhookConfiguration", "MutatingWebhookConfiguration"} {
		for _, key := range []string{"ca.crt", "tls.crt"} {
			for _, failure := range []string{"", "secret", "configuration", "patch", "readback", "duplicate", "missing-key", "protected"} {
				t.Run(kind+"/"+key+"/"+failure, func(t *testing.T) {
					const canary = "synthetic-private-serving-ca"
					target := program.WebhookCA{Ref: sdk.ObjectRef{APIVersion: "admissionregistration.k8s.io/v1", Kind: kind, Name: "policy"}, Webhook: "policy.example", SecretNamespace: "other", SecretName: "serving", SecretKey: key}
					meta := metav1.ObjectMeta{Name: "policy", UID: "one", ResourceVersion: "1"}
					valid := &admissionv1.ValidatingWebhookConfiguration{ObjectMeta: meta, Webhooks: []admissionv1.ValidatingWebhook{{Name: target.Webhook, ClientConfig: admissionv1.WebhookClientConfig{CABundle: []byte("wrong")}}}}
					mutate := &admissionv1.MutatingWebhookConfiguration{ObjectMeta: meta, Webhooks: []admissionv1.MutatingWebhook{{Name: target.Webhook, ClientConfig: admissionv1.WebhookClientConfig{CABundle: []byte("wrong")}}}}
					if failure == "duplicate" {
						valid.Webhooks = append(valid.Webhooks, valid.Webhooks[0])
						mutate.Webhooks = append(mutate.Webhooks, mutate.Webhooks[0])
					}
					var object kruntime.Object = valid
					if kind == "MutatingWebhookConfiguration" {
						object = mutate
					}
					secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "other", Name: "serving"}, Data: map[string][]byte{key: []byte(canary)}}
					if failure == "missing-key" {
						secret.Data = nil
					}
					client := fake.NewSimpleClientset(object, secret)
					resource := "validatingwebhookconfigurations"
					if kind == "MutatingWebhookConfiguration" {
						resource = "mutatingwebhookconfigurations"
					}
					reads := 0
					client.PrependReactor("*", "*", func(a ktesting.Action) (bool, kruntime.Object, error) {
						if a.GetVerb() == "get" && a.GetResource().Resource == resource {
							reads++
						}
						fail := failure == "secret" && a.GetResource().Resource == "secrets" || failure == "configuration" && a.GetVerb() == "get" && a.GetResource().Resource == resource || failure == "patch" && a.GetVerb() == "patch" || failure == "readback" && reads > 1 && a.GetVerb() == "get" && a.GetResource().Resource == resource
						if fail {
							return true, nil, fmt.Errorf("private error: %s", canary)
						}
						return false, nil, nil
					})
					transport := TypedPlanClient{Client: client}
					if failure == "protected" {
						transport.ProtectedNamespace = "other"
					}
					digest, err := transport.SyncWebhookCA(context.Background(), target, false)
					if failure == "" {
						if err != nil || !strings.HasPrefix(digest, "sha256:") {
							t.Fatalf("sync: %v", err)
						}
						before := len(client.Actions())
						again, e := transport.SyncWebhookCA(context.Background(), target, false)
						if e != nil || again != digest {
							t.Fatal("sync not idempotent")
						}
						for _, a := range client.Actions()[before:] {
							if a.GetVerb() == "patch" {
								t.Fatal("idempotent sync wrote again")
							}
						}
					} else {
						if err == nil || strings.Contains(err.Error(), canary) {
							t.Fatal("failure leaked private data or was suppressed")
						}
						if (failure == "patch" || failure == "readback") != errors.Is(err, errWebhookCAWriteUnknown) {
							t.Fatal("write uncertainty lost")
						}
					}
					for _, a := range client.Actions() {
						if failure != "" && failure != "patch" && failure != "readback" && a.GetVerb() == "patch" {
							t.Fatal("invalid target wrote configuration")
						}
						if a.GetResource().Resource == "secrets" && a.GetNamespace() != "other" {
							t.Fatal("Secret read escaped named namespace")
						}
					}
				})
			}
		}
	}
}

func TestWebhookRawRuntimeReadRedactsBeforeClientAndCapture(t *testing.T) {
	const canary = "synthetic-http-private-ca"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingWebhookConfiguration", "metadata": map[string]any{"name": "policy"}, "webhooks": []any{map[string]any{"name": "policy.example", "clientConfig": map[string]any{"caBundle": base64.StdEncoding.EncodeToString([]byte(canary))}}}})
	}))
	defer server.Close()
	view := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app"}, DatabaseExecConfig: &rest.Config{Host: server.URL}}
	record := &sdktest.RecordingReadView{ReadView: view}
	client, err := program.Kubernetes(record)
	if err != nil {
		t.Fatal(err)
	}
	hook, err := client.AdmissionregistrationV1().ValidatingWebhookConfigurations().Get(context.Background(), "policy", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(hook.Webhooks[0].ClientConfig.CABundle), "sha256:") {
		t.Fatal("runtime returned raw CA")
	}
	calls := record.RecordedReads()
	raw, _ := json.Marshal(calls)
	if strings.Contains(string(raw), canary) || strings.Contains(string(raw), base64.StdEncoding.EncodeToString([]byte(canary))) {
		t.Fatal("API recording leaked CA")
	}
	replayClient, err := program.Kubernetes(&sdktest.ReplayReadView{Calls: calls})
	if err != nil {
		t.Fatal(err)
	}
	restored, err := replayClient.AdmissionregistrationV1().ValidatingWebhookConfigurations().Get(context.Background(), "policy", metav1.GetOptions{})
	if err != nil || string(restored.Webhooks[0].ClientConfig.CABundle) != string(hook.Webhooks[0].ClientConfig.CABundle) {
		t.Fatal("typed read replay changed CA projection")
	}
}

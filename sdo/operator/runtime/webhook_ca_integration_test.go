//go:build integration

package runtime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"google.golang.org/protobuf/encoding/protojson"
	"io"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"net/http"
	"net/http/httptest"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"strings"
	"testing"
)

func TestWebhookRawRuntimeReadRedactsBeforeClientAndCapture(t *testing.T) {
	const canary = "synthetic-http-private-ca"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingWebhookConfiguration", "metadata": map[string]any{"name": "policy", "annotations": map[string]any{"kubectl.kubernetes.io/last-applied-configuration": fmt.Sprintf(`{"caBundle":%q}`, base64.StdEncoding.EncodeToString([]byte(canary)))}}, "webhooks": []any{map[string]any{"name": "policy.example", "clientConfig": map[string]any{"caBundle": base64.StdEncoding.EncodeToString([]byte(canary))}}}})
	}))
	defer server.Close()
	view := LiveReadView{Carrier: inputs.New(sdktest.Snapshot{NamespaceName: "app"}), DatabaseExecConfig: &rest.Config{Host: server.URL}}
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
	// The observation boundary is content based, including objects with
	// caBundle outside admissionregistration resources.
	generic, readErr := record.KubernetesRead(program.KubernetesRequest{Method: "GET", Path: "/api/v1/namespaces/app/configmaps/example"})
	if readErr != nil {
		t.Fatal(readErr)
	}
	assertWebhookArtifactsPrivate(t, canary, generic)
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

func TestWebhookCASyncComposedActionPublication(t *testing.T) {
	for _, operation := range []string{"api-patch", "patch", "delete"} {
		t.Run(operation, func(t *testing.T) {
			const canary = "synthetic-composed-private-ca"
			ref := sdk.ObjectRef{APIVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingWebhookConfiguration", Name: "policy"}
			target := program.WebhookCA{Ref: ref, Webhook: "policy.example", SecretNamespace: "other", SecretName: "serving", SecretKey: "ca.crt"}
			original := &admissionv1.ValidatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: ref.Name, UID: "one", ResourceVersion: "1"}, Webhooks: []admissionv1.ValidatingWebhook{{Name: target.Webhook, ClientConfig: admissionv1.WebhookClientConfig{CABundle: []byte("wrong")}}}}
			embedded, _ := json.Marshal(map[string]any{"webhooks": []any{map[string]any{"clientConfig": map[string]any{"caBundle": base64.StdEncoding.EncodeToString([]byte(canary))}}}})
			original.Annotations = map[string]string{"kubectl.kubernetes.io/last-applied-configuration": string(embedded)}
			client := fake.NewSimpleClientset(original, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "other", Name: "serving"}, Data: map[string][]byte{"ca.crt": []byte(canary)}})
			client.Resources = []*metav1.APIResourceList{{GroupVersion: "admissionregistration.k8s.io/v1", APIResources: []metav1.APIResource{{Name: "validatingwebhookconfigurations", Kind: "ValidatingWebhookConfiguration", Verbs: metav1.Verbs{"get", "patch", "delete"}}}}}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				hooks := client.AdmissionregistrationV1().ValidatingWebhookConfigurations()
				var object any
				var err error
				switch req.Method {
				case "GET":
					object, err = hooks.Get(req.Context(), ref.Name, metav1.GetOptions{})
				case "PATCH":
					body, _ := io.ReadAll(req.Body)
					object, err = hooks.Patch(req.Context(), ref.Name, types.PatchType(req.Header.Get("Content-Type")), body, metav1.PatchOptions{})
				case "DELETE":
					err = hooks.Delete(req.Context(), ref.Name, metav1.DeleteOptions{})
					object = map[string]any{"kind": "Status", "status": "Success"}
				}
				w.Header().Set("Content-Type", "application/json")
				if err != nil {
					w.WriteHeader(404)
					_ = json.NewEncoder(w).Encode(map[string]any{"kind": "Status", "status": "Failure", "reason": "NotFound", "code": 404})
					return
				}
				_ = json.NewEncoder(w).Encode(object)
			}))
			defer server.Close()
			var calls []sdktest.PrimitiveCall
			executor := PlanExecutor{Client: TypedPlanClient{Client: client, ExecConfig: &rest.Config{Host: server.URL}}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}, RecordPrimitive: func(c sdktest.PrimitiveCall) error { calls = append(calls, c); return nil }}
			incident := IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}
			synced, err := executor.Apply(context.Background(), incident, program.Plan{Actions: []program.Action{program.SyncWebhookCA{Target: target, Reason: "repair"}}}, false)
			if err != nil {
				t.Fatal(err)
			}
			// Separate applications exercise the boundary after the private sync returns.
			var action program.Action = program.KubernetesRequest{Ref: ref, Method: "PATCH", Path: "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations/policy", ContentType: "application/merge-patch+json", Body: []byte(`{"metadata":{"labels":{"repaired":"true"}}}`), Reason: "label"}
			if operation == "patch" {
				action = program.PatchObject{Ref: ref, Type: "merge", Patch: json.RawMessage(`{"metadata":{"labels":{"repaired":"true"}}}`), Reason: "label"}
			}
			if operation == "delete" {
				action = program.DeleteObject{Ref: ref, Reason: "remove"}
			}
			result, err := executor.Apply(context.Background(), incident, program.Plan{Actions: []program.Action{action}}, false)
			if err != nil {
				t.Fatal(err)
			}
			if operation == "patch" {
				published, marshalErr := json.Marshal(result)
				if marshalErr != nil {
					t.Fatal(marshalErr)
				}
				var restored PlanApplicationResult
				if err = json.Unmarshal(published, &restored); err != nil {
					t.Fatal(err)
				}
				if !restored.Patches[0].Irreversible {
					t.Fatal("digest preimage retained rollback authority")
				}
				if result.Patches[0].Irreversible {
					t.Fatal("publication mutated in-process compensation authority")
				}
				if rollbackErr := executor.Rollback(context.Background(), restored); rollbackErr == nil {
					t.Fatal("restored digest preimage was written back")
				}
			}
			wire, err := result.Proto()
			if err != nil {
				t.Fatal(err)
			}
			wireJSON, err := protojson.Marshal(wire)
			if err != nil {
				t.Fatal(err)
			}
			var durable any
			if err = json.Unmarshal(wireJSON, &durable); err != nil {
				t.Fatal(err)
			}

			outcome := &contracts.OutcomeRecord{PlanApplications: []*contracts.PlanApplication{wire}}
			outcomeJSON, err := protojson.Marshal(outcome)
			if err != nil {
				t.Fatal(err)
			}
			var durableOutcome any
			if err = json.Unmarshal(outcomeJSON, &durableOutcome); err != nil {
				t.Fatal(err)
			}
			var capture any
			if operation != "delete" {
				got, getErr := client.AdmissionregistrationV1().ValidatingWebhookConfigurations().Get(context.Background(), ref.Name, metav1.GetOptions{})
				if getErr != nil {
					t.Fatal(getErr)
				}
				got.APIVersion = ref.APIVersion
				got.Kind = ref.Kind
				raw, _ := json.Marshal(got)
				var object unstructured.Unstructured
				if err = json.Unmarshal(raw, &object); err != nil {
					t.Fatal(err)
				}
				snapshot, captureErr := sdktest.CaptureSnapshot(sdktest.Snapshot{NamespaceName: "app", Objects: []unstructured.Unstructured{object}, ObjectSnapshotVersion: 1}, "app", ref)
				if captureErr != nil {
					t.Fatal(captureErr)
				}
				capture = snapshot
			}
			assertWebhookArtifactsPrivate(t, canary, synced, result, result.Patches, result.PreImages, result.ObjectEffects, calls, durable, durableOutcome, capture)
		})
	}
}

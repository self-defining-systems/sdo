package runtime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/protobuf/encoding/protojson"
	"io"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	ktesting "k8s.io/client-go/testing"
	"net/http"
	"net/http/httptest"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
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
	assertWebhookArtifactsPrivate(t, canary, result, record.RecordedReads(), snapshot, wire)
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
		_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingWebhookConfiguration", "metadata": map[string]any{"name": "policy", "annotations": map[string]any{"kubectl.kubernetes.io/last-applied-configuration": fmt.Sprintf(`{"caBundle":%q}`, base64.StdEncoding.EncodeToString([]byte(canary)))}}, "webhooks": []any{map[string]any{"name": "policy.example", "clientConfig": map[string]any{"caBundle": base64.StdEncoding.EncodeToString([]byte(canary))}}}})
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

// Every durable artifact produced by sync and composed ordinary actions is
// scanned, including nested protobuf JSON byte fields (which are base64).
func assertWebhookArtifactsPrivate(t *testing.T, canary string, artifacts ...any) {
	t.Helper()
	var scan func(any)
	scan = func(v any) {
		switch x := v.(type) {
		case string:
			if strings.Contains(x, canary) || strings.Contains(x, base64.StdEncoding.EncodeToString([]byte(canary))) {
				t.Fatal("synthetic CA survived publication")
			}
			if b, err := base64.StdEncoding.DecodeString(x); err == nil {
				if strings.Contains(string(b), canary) || strings.Contains(string(b), base64.StdEncoding.EncodeToString([]byte(canary))) {
					t.Fatal("synthetic CA survived encoded publication")
				}
			}
		case []any:
			for _, item := range x {
				scan(item)
			}
		case map[string]any:
			for _, item := range x {
				scan(item)
			}
		}
	}
	for _, artifact := range artifacts {
		raw, err := json.Marshal(artifact)
		if err != nil {
			t.Fatal(err)
		}
		var tree any
		if err = json.Unmarshal(raw, &tree); err != nil {
			t.Fatal(err)
		}
		scan(tree)
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

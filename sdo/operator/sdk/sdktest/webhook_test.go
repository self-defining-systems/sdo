package sdktest

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"strings"
	"testing"
)

func TestWebhookCaptureRedactsCABundlesIdempotently(t *testing.T) {
	for i := 0; i < 128; i++ {
		ca := fmt.Sprintf("synthetic-private-ca-%d-%s", i, strings.Repeat("x", i))
		encoded := base64.StdEncoding.EncodeToString([]byte(ca))
		for _, kind := range []string{"ValidatingWebhookConfiguration", "MutatingWebhookConfiguration"} {
			raw := []byte(fmt.Sprintf(`{"apiVersion":"admissionregistration.k8s.io/v1","kind":%q,"metadata":{"name":"policy"},"webhooks":[{"name":"policy.example","clientConfig":{"caBundle":%q}}]}`, kind, encoded))
			first, err := ProjectCapturedObject(raw)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(first), ca) || strings.Contains(string(first), encoded) {
				t.Fatal("private CA survived projection")
			}
			second, err := ProjectCapturedObject(first)
			if err != nil || string(first) != string(second) {
				t.Fatal("CA projection is not idempotent")
			}
		}
	}
}

type webhookRawView struct {
	program.ReadView
	payload json.RawMessage
}

func (v webhookRawView) KubernetesRead(program.KubernetesRequest) (json.RawMessage, error) {
	return v.payload, nil
}

func TestWebhookRawReadsRedactAllFramesAndReplay(t *testing.T) {
	const canary = "synthetic-raw-private-ca"
	payload := fmt.Sprintf(`{"apiVersion":"admissionregistration.k8s.io/v1","kind":"ValidatingWebhookConfiguration","metadata":{"name":"policy","resourceVersion":"123"},"webhooks":[{"name":"policy.example","clientConfig":{"caBundle":%q}}]}`, base64.StdEncoding.EncodeToString([]byte(canary)))
	var embedded map[string]any
	if err := json.Unmarshal([]byte(payload), &embedded); err != nil {
		t.Fatal(err)
	}
	embedded["metadata"].(map[string]any)["annotations"] = map[string]any{"kubectl.kubernetes.io/last-applied-configuration": payload}
	withAnnotation, _ := json.Marshal(embedded)
	payload = string(withAnnotation)
	for _, raw := range []string{payload, `{"kind":"List","items":[` + payload + `]}`, `{"type":"ADDED","object":` + payload + "}\n" + `{"type":"MODIFIED","object":` + payload + "}", fmt.Sprintf("%q", `{"type":"ADDED","object":`+payload+"}")} {
		projected, err := sdk.ProjectWebhookCAJSON([]byte(raw))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(projected), canary) || strings.Contains(string(projected), base64.StdEncoding.EncodeToString([]byte(canary))) {
			t.Fatal("raw read leaked CA")
		}
		if strings.Count(string(projected), "policy.example") != strings.Count(raw, "policy.example") {
			t.Fatal("projection lost stream frames")
		}
		// The API transport wraps newline-delimited watch bodies in a JSON
		// string before returning its RawMessage contract.
		transportBody := json.RawMessage(raw)
		if !json.Valid(transportBody) {
			transportBody, err = json.Marshal(raw)
			if err != nil {
				t.Fatal(err)
			}
		}
		expectedObservation, err := sdk.ProjectWebhookCAJSON(transportBody)
		if err != nil {
			t.Fatal(err)
		}
		record := &RecordingReadView{ReadView: webhookRawView{payload: transportBody}}
		request := program.KubernetesRequest{Method: "GET", Path: "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations"}
		observed, observeErr := record.KubernetesRead(request)
		if observeErr != nil || string(observed) != string(expectedObservation) {
			t.Fatal("recording boundary changed projection", observeErr)
		}
		calls, marshalErr := json.Marshal(record.RecordedReads())
		if marshalErr != nil || strings.Contains(string(calls), canary) || strings.Contains(string(calls), base64.StdEncoding.EncodeToString([]byte(canary))) {
			t.Fatal("recording retained annotation CA", marshalErr)
		}
		replay := &ReplayReadView{Calls: record.RecordedReads()}
		replayed, replayErr := replay.KubernetesRead(request)
		if replayErr != nil || string(replayed) != string(expectedObservation) {
			t.Fatal("replay changed projected frames", replayErr)
		}
		again, err := sdk.ProjectWebhookCAJSON(projected)
		if err != nil || string(projected) != string(again) {
			t.Fatal("raw projection not idempotent")
		}
	}
}

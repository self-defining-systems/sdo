package sdktest

import (
	"encoding/base64"
	"fmt"
	"sdo.dev/operator/sdk"
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

func TestWebhookRawReadsRedactAllFramesAndReplay(t *testing.T) {
	const canary = "synthetic-raw-private-ca"
	payload := fmt.Sprintf(`{"apiVersion":"admissionregistration.k8s.io/v1","kind":"ValidatingWebhookConfiguration","metadata":{"name":"policy","resourceVersion":"123"},"webhooks":[{"name":"policy.example","clientConfig":{"caBundle":%q}}]}`, base64.StdEncoding.EncodeToString([]byte(canary)))
	for _, raw := range []string{payload, `{"type":"ADDED","object":` + payload + "}\n" + `{"type":"MODIFIED","object":` + payload + "}", fmt.Sprintf("%q", `{"type":"ADDED","object":`+payload+"}")} {
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
		again, err := sdk.ProjectWebhookCAJSON(projected)
		if err != nil || string(projected) != string(again) {
			t.Fatal("raw projection not idempotent")
		}
	}
}

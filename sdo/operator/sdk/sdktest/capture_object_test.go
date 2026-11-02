package sdktest

import (
	"strings"
	"testing"
)

func TestProjectCapturedObjectReusesSnapshotRedaction(t *testing.T) {
	raw := []byte(`{"apiVersion":"v1","kind":"PodList","items":[{"metadata":{"name":"api","annotations":{"kubectl.kubernetes.io/last-applied-configuration":"private"}},"spec":{"containers":[{"name":"api","env":[{"name":"API_TOKEN","value":"private"},{"name":"MODE","value":"live"}]}]}}]}`)
	body, err := ProjectCapturedObject(raw)
	if err != nil || strings.Contains(string(body), "private") || !strings.Contains(string(body), "live") {
		t.Fatalf("projection: %s %v", body, err)
	}
	for _, raw := range []string{`{"kind":"TokenRequest","status":{"token":"private"}}`, `{"kind":"Table","rows":[{"object":{"kind":"Secret","data":{"key":"private"}}}]}`, `not json`} {
		if b, e := ProjectCapturedObject([]byte(raw)); e == nil || len(b) != 0 {
			t.Fatalf("unknown representation captured: %s", b)
		}
	}
}

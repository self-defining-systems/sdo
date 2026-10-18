package deployment

import (
	"strings"
	"testing"
)

func TestHealthVerdictParsesStdoutSeparatelyFromKubernetesWarnings(t *testing.T) {
	status, err := decodeHealthVerdict([]byte(`{"status":"clear"}`), []byte("W1018 00:00:00 client warning"))
	if err != nil || status != "clear" {
		t.Fatalf("warning contaminated detector verdict: status=%q err=%v", status, err)
	}
	_, err = decodeHealthVerdict([]byte("not JSON"), []byte(strings.Repeat("warning", 2000)))
	if err == nil || !strings.Contains(err.Error(), "not JSON") || len(err.Error()) > 5000 {
		t.Fatalf("invalid verdict omitted useful bounded diagnostics: %v", err)
	}
}

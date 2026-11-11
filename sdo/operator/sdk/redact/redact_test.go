package redact

import (
	"strings"
	"testing"
)

func TestSecretsRemovesSecretLookingValues(t *testing.T) {
	for _, line := range []string{
		`password=hunter2-synthetic`,
		`Authorization: Bearer abcdefgh12345678`,
		`token: "synthetic-token-value"`,
		`attempted key AKIASYNTHETIC0000001 not found`,
		`digest 0123456789abcdef0123456789abcdef01234567`,
		`jwt eyJhbGciOi.eyJzdWIiOi.c2lnbmF0dXJl`,
	} {
		got := Secrets(line)
		if !strings.Contains(got, Marker) {
			t.Errorf("Secrets(%q) = %q, want a redaction", line, got)
		}
	}
	for _, secret := range []string{"hunter2-synthetic", "abcdefgh12345678", "synthetic-token-value", "AKIASYNTHETIC0000001", "0123456789abcdef0123456789abcdef01234567"} {
		for _, line := range []string{"password=" + secret, "Bearer " + secret, "token: " + secret, "key " + secret, "digest " + secret} {
			if got := Secrets(line); strings.Contains(got, secret) && strings.Contains(got, Marker) {
				t.Errorf("Secrets(%q) = %q still holds the value", line, got)
			}
		}
	}
}

func TestSecretsKeepsProse(t *testing.T) {
	line := `permission denied: wrong hmac signature for request to /repository/info`
	if got := Secrets(line); got != line {
		t.Fatalf("Secrets changed prose: %q", got)
	}
}

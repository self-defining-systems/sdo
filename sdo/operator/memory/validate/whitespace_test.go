package validate

import (
	"testing"
)

func TestSecretPathScopeIsMemoryOnly(t *testing.T) {
	for path, want := range map[string]bool{
		"chart/templates/shared-secrets/job.yaml": false,
		".git/config": false,
		".sdo/diagnostics/traffic/secret-token.go":    true,
		".sdo/playbooks/base/.env":                    true,
		".sdo/deployment/chart/templates/secrets/a.y": false,
		".sdo/deployment/.git/config":                 true,
		".sdo/deployment/values/secrets.yaml":         false,
		".sdo/deployment/patches/0001-secret.patch":   false,
		".sdo/playbooks/secret/README.md":             true,
	} {
		if _, got := secretPathPart(path); got != want {
			t.Errorf("secretPathPart(%q) = %v, want %v", path, got, want)
		}
	}
}

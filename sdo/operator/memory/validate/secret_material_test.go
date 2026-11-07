package validate

import "testing"

func TestSecretMaterialInAuthoredFiles(t *testing.T) {
	pem := "-----BEGIN " + "RSA PRIVATE KEY-----\nMIIB\n-----END " + "RSA PRIVATE KEY-----\n"
	cases := []struct {
		name, path, body string
		want             bool
	}{
		{"plain password in values", ".sdo/deployment/values/app.yaml", "db:\n  password: hunter2hunter2\n", true},
		{"quoted token key", ".sdo/deployment/values/app.yaml", "api:\n  \"apiToken\": \"abc123\"\n", true},
		{"pem in patch", ".sdo/deployment/patches/0001-x.patch", "+" + pem, true},
		{"token prefix in manifest", ".sdo/deployment/manifests/a.yaml", "data: ghp_" + "abcdefghijklmnopqrstuvwxyz0123456789\n", true},
		{"aws key id in playbook", ".sdo/playbooks/p/README.md", "use AKIA" + "ABCDEFGHIJKLMNOP\n", true},
		{"empty password", ".sdo/deployment/values/app.yaml", "db:\n  password: \"\"\n", false},
		{"templated password", ".sdo/deployment/values/app.yaml", "db:\n  password: {{ .Values.pw }}\n", false},
		{"secret reference", ".sdo/deployment/values/app.yaml", "db:\n  existingSecret: db-creds\n  secretKeyRef: pw\n", false},
		{"imported chart is not scanned by key", ".sdo/deployment/chart/values.yaml", "password: changeme\n", false},
	}
	for _, c := range cases {
		root := t.TempDir()
		write(t, root, c.path, c.body)
		got := len(secretDataDiagnostics(root, []string{c.path})) > 0
		if got != c.want {
			t.Errorf("%s: flagged=%v want %v", c.name, got, c.want)
		}
	}
}

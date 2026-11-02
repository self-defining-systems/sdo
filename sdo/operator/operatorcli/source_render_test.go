package operatorcli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sdo.dev/operator/internal/testutil"
	"strings"
	"testing"
)

func TestSourceRenderUsesProductionGeneratorAndExcludesSecretData(t *testing.T) {
	root := t.TempDir()
	testutil.InitGit(t, root)
	for name, body := range map[string]string{
		"kustomization.yaml": "configMapGenerator:\n- name: startup\n  files: [init.sh]\ngeneratorOptions: {disableNameSuffixHash: true}\nsecretGenerator:\n- name: credential\n  files: [private/password]\n",
		"init.sh":            "echo synthetic\n",
	} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0644); err != nil {
			t.Fatal(err)
		}
	}
	testutil.Git(t, root, "add", ".")
	testutil.Git(t, root, "commit", "-qm", "synthetic source")
	revision := strings.TrimSpace(testutil.Git(t, root, "rev-parse", "HEAD"))
	request, _ := json.Marshal(map[string]any{"repository": root, "revision": revision, "namespace": "app", "manifest_paths": []string{"kustomization.yaml"}})
	var output bytes.Buffer
	if err := RunSourceRender(context.Background(), bytes.NewReader(request), &output); err != nil {
		t.Fatal(err)
	}
	var objects []map[string]any
	if err := json.Unmarshal(output.Bytes(), &objects); err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 || objects[0]["kind"] != "ConfigMap" || objects[0]["data"].(map[string]any)["init.sh"] != "echo synthetic\n" {
		t.Fatalf("incorrect source render: %s", output.String())
	}
	if strings.Contains(output.String(), "credential") || strings.Contains(output.String(), "password") {
		t.Fatal("Secret generator input/reference entered non-secret inventory output")
	}
}

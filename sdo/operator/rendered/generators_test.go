package rendered

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/sdk"
	"strings"
	"testing"
)

func TestAcceptedFileGeneratorsUseOnlyAcceptedGitAndExcludeSecrets(t *testing.T) {
	root := t.TempDir()
	testutil.InitGit(t, root)
	files := map[string]string{
		"deploy/kustomization.yaml": "resources: [pod.yaml]\nconfigMapGenerator:\n- name: startup\n  files: [init.sh=../scripts/init.sh]\nsecretGenerator:\n- name: credential\n  files: [password=../private/password]\ngeneratorOptions:\n  disableNameSuffixHash: true\n",
		"deploy/pod.yaml":           "apiVersion: v1\nkind: Pod\nmetadata: {name: backend}\nspec:\n  containers:\n  - name: backend\n    image: example/backend:1\n  volumes:\n  - name: startup\n    configMap: {name: startup}\n",
		"scripts/init.sh":           "echo accepted\n",
		"private/password":          "SECRET-CANARY-DO-NOT-RENDER",
	}
	for name, body := range files {
		p := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0644); err != nil {
			t.Fatal(err)
		}
	}
	testutil.Git(t, root, "add", ".")
	testutil.Git(t, root, "commit", "-qm", "synthetic generators")
	revision := strings.TrimSpace(testutil.Git(t, root, "rev-parse", "HEAD"))
	if err := os.WriteFile(filepath.Join(root, "scripts/init.sh"), []byte("unaccepted"), 0644); err != nil {
		t.Fatal(err)
	}
	source := Source{Repository: root, ManifestPaths: []string{"deploy"}}
	objects, err := source.AcceptedManifests(context.Background(), revision, "app")
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 3 {
		t.Fatalf("wanted generated ConfigMap, Pod and Secret reference, got %v", objects)
	}
	cm, err := source.AcceptedManifest(context.Background(), revision, sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "startup"})
	if err != nil {
		t.Fatal(err)
	}
	if cm.Object["data"].(map[string]any)["init.sh"] != "echo accepted\n" {
		t.Fatalf("wrong generated content: %v", cm)
	}
	inventory, err := source.AcceptedInventory(context.Background(), revision, "app")
	if err != nil || len(inventory.Objects) != 2 {
		t.Fatalf("inventory: %+v %v", inventory, err)
	}
	encoded, _ := json.Marshal(objects)
	if strings.Contains(string(encoded), "SECRET-CANARY") {
		t.Fatal("Secret data escaped")
	}
}

func TestGeneratorInputsAreLocalBoundedAndSecretsNeverLoaded(t *testing.T) {
	for _, input := range []string{"https://example.invalid/base", "../../outside", "/absolute"} {
		t.Run(input, func(t *testing.T) {
			_, err := renderKustomization(context.Background(), "deploy/kustomization.yaml", func(_ context.Context, name string) ([]byte, error) {
				if name != "deploy/kustomization.yaml" {
					t.Fatalf("attempt to load forbidden input %s", name)
				}
				return []byte("resources: [" + input + "]\n"), nil
			})
			if err == nil {
				t.Fatal("unsafe resource accepted")
			}
		})
	}
	for _, field := range []string{"generators", "transformers", "helmCharts", "configurations"} {
		t.Run(field, func(t *testing.T) {
			_, err := renderKustomization(context.Background(), "deploy/kustomization.yaml", func(context.Context, string) ([]byte, error) { return []byte(field + ": [untrusted]\n"), nil })
			if err == nil {
				t.Fatal("executable/external generator accepted")
			}
		})
	}
	data, err := renderKustomization(context.Background(), "deploy/kustomization.yaml", func(_ context.Context, name string) ([]byte, error) {
		if name != "deploy/kustomization.yaml" {
			t.Fatalf("Secret file was read: %s", name)
		}
		return []byte("generatorOptions: {disableNameSuffixHash: true}\nsecretGenerator:\n- name: credential\n  files: [../private/password]\nconfigMapGenerator:\n- name: startup\n  literals: [mode=synthetic]\n"), nil
	})
	if err != nil || strings.Contains(string(data), "password") {
		t.Fatalf("secret generator was rendered: %s %v", data, err)
	}
}

func TestAcceptedSecretRemainsOnlyAnIdentityReference(t *testing.T) {
	root := t.TempDir()
	testutil.InitGit(t, root)
	body := "apiVersion: v1\nkind: Secret\nmetadata:\n  name: credential\n  annotations: {last-applied: SECRET-CANARY}\ndata: {password: SECRET-CANARY}\nstringData: {password: SECRET-CANARY}\nextra: SECRET-CANARY\n"
	if err := os.WriteFile(filepath.Join(root, "secret.yaml"), []byte(body), 0644); err != nil {
		t.Fatal(err)
	}
	testutil.Git(t, root, "add", ".")
	testutil.Git(t, root, "commit", "-qm", "synthetic secret reference")
	revision := strings.TrimSpace(testutil.Git(t, root, "rev-parse", "HEAD"))
	source := Source{Repository: root, ManifestPaths: []string{"secret.yaml"}}
	objects, err := source.RawManifests(context.Background(), revision, "app")
	if err != nil || len(objects) != 1 {
		t.Fatalf("reference unavailable: %v", err)
	}
	encoded, _ := json.Marshal(objects)
	if strings.Contains(string(encoded), "SECRET-CANARY") {
		t.Fatal("Secret data or metadata escaped")
	}
}

func TestKustomizeFileGeneratorHashesRewriteConsumersAtAcceptedRevision(t *testing.T) {
	read := func(_ context.Context, name string) ([]byte, error) {
		inputs := map[string]string{
			"kustomization.yaml": "resources: [pod.yaml]\nconfigMapGenerator:\n- name: startup\n  files: [init.sh]\n",
			"init.sh":            "echo synthetic\n",
			"pod.yaml":           "apiVersion: v1\nkind: Pod\nmetadata: {name: backend}\nspec:\n  containers: [{name: backend, image: example/backend:1}]\n  volumes:\n  - name: config\n    configMap: {name: startup}\n",
		}
		return []byte(inputs[name]), nil
	}
	data, err := renderKustomization(context.Background(), "kustomization.yaml", read)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "name: startup\n") || strings.Count(string(data), "name: startup-") != 2 {
		t.Fatalf("generated identity and consumer were not rewritten together: %s", data)
	}
}

func TestSecretGeneratorHashNeverRequiresReadingSecretInputs(t *testing.T) {
	_, err := renderKustomization(context.Background(), "kustomization.yaml", func(_ context.Context, name string) ([]byte, error) {
		if name != "kustomization.yaml" {
			t.Fatalf("Secret input loaded to derive hash: %s", name)
		}
		return []byte("secretGenerator:\n- name: credential\n  files: [private/password]\n"), nil
	})
	if err == nil || !strings.Contains(err.Error(), "stable names") {
		t.Fatalf("hash-dependent Secret was silently misrendered: %v", err)
	}
}

func TestKustomizeConfigMapEnvironmentFilesAreTrackedGeneratorInputs(t *testing.T) {
	data, err := renderKustomization(context.Background(), "deploy/kustomization.yaml", func(_ context.Context, name string) ([]byte, error) {
		if name == "deploy/kustomization.yaml" {
			return []byte("configMapGenerator:\n- name: startup\n  envs: [../config/startup.env]\ngeneratorOptions: {disableNameSuffixHash: true}\n"), nil
		}
		if name != "config/startup.env" {
			t.Fatalf("unexpected generator input %s", name)
		}
		return []byte("MODE=synthetic\nCOUNT=1\n"), nil
	})
	if err != nil || !strings.Contains(string(data), "MODE: synthetic") || !strings.Contains(string(data), `COUNT: "1"`) {
		t.Fatalf("environment file generator unavailable: %s %v", data, err)
	}
}

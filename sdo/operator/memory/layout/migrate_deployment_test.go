package layout

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func deployTree(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for rel, content := range files {
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func readTree(t *testing.T, root, rel string) (string, bool) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
	if os.IsNotExist(err) {
		return "", false
	}
	if err != nil {
		t.Fatal(err)
	}
	return string(data), true
}

const syntheticPatch = "SDO-Patch: 1\nOwner: deployer\nReason: |\n  The synthetic job needs the local pull policy so it starts against the lane registry.\n\n" +
	"diff --git a/chart/job.yaml b/chart/job.yaml\nindex 1111111..2222222 100644\n--- a/chart/job.yaml\n+++ b/chart/job.yaml\n@@ -1,3 +1,4 @@\n kind: Job\n name: migrate\n+pull: IfNotPresent\n image: app:1\n"

// Schema 15 moves the retired views out of memory, rewrites the override's
// paths and drops its retired fields, applies the patch series to the files it
// changes, and removes the patches and the telemetry overlay slot.
func TestMigrateToSchema15MovesViewsAppliesPatchesAndRewritesTheOverride(t *testing.T) {
	plan := `{"source":{"rendererId":"raw","config":{"@type":"type.googleapis.com/sdodev.contracts.v1alpha1.RawRenderConfig","paths":[".sdo/deployment/manifests","k8s"]}},"images":[{"image":"r/app","sourceId":"buildkit","config":{"@type":"type.googleapis.com/sdodev.contracts.v1alpha1.BuildKitImageConfig","contextPath":"app","dockerfilePath":".sdo/deployment/build/app/Dockerfile"}}],"expectedWorkloadIds":["Deployment/app/web"],"strategyId":"raw-manifests-v1"}`
	root := deployTree(t, map[string]string{
		SchemaVersionFile:                            "14\n",
		DeploymentPlanFile:                           plan,
		DeploymentDir + "/manifests/web.yaml":        "kind: Deployment\n",
		DeploymentDir + "/build/app/Dockerfile":      "FROM scratch\n",
		DeploymentDir + "/values/kind.yaml":          "replicas: 1\n",
		DeploymentDir + "/overlays/telemetry/o.yaml": "a: b\n",
		DeploymentDir + "/patches/0001-pull.patch":   syntheticPatch,
		"chart/job.yaml":                             "kind: Job\nname: migrate\nimage: app:1\n",
	})
	changed, err := Migrate(root)
	if err != nil || !changed {
		t.Fatalf("Migrate = %v, %v", changed, err)
	}
	for rel, want := range map[string]string{
		"deploy/sdo/manifests/web.yaml":   "kind: Deployment\n",
		"deploy/sdo/build/app/Dockerfile": "FROM scratch\n",
		"deploy/sdo/values/kind.yaml":     "replicas: 1\n",
		"chart/job.yaml":                  "kind: Job\nname: migrate\npull: IfNotPresent\nimage: app:1\n",
		SchemaVersionFile:                 "15\n",
	} {
		if got, ok := readTree(t, root, rel); !ok || got != want {
			t.Errorf("%s = %q (exists %v), want %q", rel, got, ok, want)
		}
	}
	for _, gone := range []string{DeploymentDir + "/manifests", DeploymentDir + "/build", DeploymentDir + "/values", DeploymentDir + "/overlays", DeploymentDir + "/patches"} {
		if _, err := os.Lstat(filepath.Join(root, filepath.FromSlash(gone))); !os.IsNotExist(err) {
			t.Errorf("%s survived the migration", gone)
		}
	}
	text, _ := readTree(t, root, DeploymentPlanFile)
	var migrated map[string]any
	if err := json.Unmarshal([]byte(text), &migrated); err != nil {
		t.Fatalf("migrated plan %q: %v", text, err)
	}
	for _, field := range retiredPlanFields {
		if _, ok := migrated[field]; ok {
			t.Errorf("migrated plan keeps %s: %s", field, text)
		}
	}
	if !strings.Contains(text, `"deploy/sdo/manifests"`) || !strings.Contains(text, `"deploy/sdo/build/app/Dockerfile"`) || strings.Contains(text, ".sdo/deployment/") || !strings.Contains(text, `"k8s"`) {
		t.Fatalf("migrated plan paths: %s", text)
	}
}

// A plain repository (no views, no patches, a current plan) is only stamped.
func TestMigrateToSchema15LeavesACurrentPlanAlone(t *testing.T) {
	plan := `{"source":{"rendererId":"helm","config":{"@type":"type.googleapis.com/sdodev.contracts.v1alpha1.HelmRenderConfig","chartPath":"chart"}}}` + "\n"
	root := deployTree(t, map[string]string{SchemaVersionFile: "14\n", DeploymentPlanFile: plan})
	if _, err := Migrate(root); err != nil {
		t.Fatal(err)
	}
	if got, _ := readTree(t, root, DeploymentPlanFile); got != plan {
		t.Fatalf("plan = %q", got)
	}
}

// A patch that no longer applies, or a view whose target exists, refuses the
// migration before any file changes, naming what to do.
func TestMigrateToSchema15RefusesBeforeWriting(t *testing.T) {
	for name, files := range map[string]map[string]string{
		"patch does not apply": {
			DeploymentDir + "/patches/0001-pull.patch": syntheticPatch,
			"chart/job.yaml": "kind: Job\nname: other\nimage: app:1\n",
		},
		"target exists": {
			DeploymentDir + "/manifests/web.yaml": "kind: Deployment\n",
			"deploy/sdo/manifests/web.yaml":       "kind: Service\n",
		},
	} {
		t.Run(name, func(t *testing.T) {
			files[SchemaVersionFile] = "14\n"
			root := deployTree(t, files)
			if _, err := Migrate(root); err == nil || !strings.Contains(err.Error(), "migrate again") {
				t.Fatalf("Migrate = %v, want an actionable refusal", err)
			}
			if got, _ := readTree(t, root, SchemaVersionFile); got != "14\n" {
				t.Fatalf("schema stamped after a refusal: %q", got)
			}
			for rel, want := range files {
				if got, _ := readTree(t, root, rel); got != want {
					t.Fatalf("%s changed after a refusal: %q", rel, got)
				}
			}
		})
	}
}

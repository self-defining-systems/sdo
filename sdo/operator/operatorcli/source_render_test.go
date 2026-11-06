package operatorcli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/rendered"
)

// workloadManifest is a workload of kind with an init and a regular container,
// the pod spec nested where the pod-template table says it is.
func workloadManifest(kind, name, image string) string {
	pod := map[string]any{
		"initContainers": []any{map[string]any{"name": "i", "image": "reg.invalid/init:1"}},
		"containers":     []any{map[string]any{"name": "c", "image": "reg.invalid/" + image + ":1"}},
	}
	path, _ := rendered.PodSpecPath(kind)
	var spec any = pod
	for i := len(path) - 1; i >= 0; i-- {
		spec = map[string]any{path[i]: spec}
	}
	object := spec.(map[string]any)
	object["apiVersion"] = "v1"
	object["kind"] = kind
	object["metadata"] = map[string]any{"name": name}
	out, _ := json.Marshal(object)
	return string(out)
}

func renderWorkloads(t *testing.T, files map[string]string) rendered.WorkloadInventory {
	t.Helper()
	dir := t.TempDir()
	testutil.InitGit(t, dir)
	for name, content := range files {
		if err := os.MkdirAll(filepath.Join(dir, "deploy"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "deploy", name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	testutil.Git(t, dir, "add", ".")
	testutil.Git(t, dir, "commit", "-qm", "seed")
	request, _ := json.Marshal(map[string]any{
		"repository": dir, "revision": strings.TrimSpace(testutil.Git(t, dir, "rev-parse", "HEAD")), "namespace": "app",
		"output": "workloads", "deployment_source": map[string]any{
			"renderer_id": "raw",
			"config":      map[string]any{"@type": "type.googleapis.com/sdodev.contracts.v1alpha1.RawRenderConfig", "paths": []string{"deploy"}},
		},
	})
	var out bytes.Buffer
	if err := RunSourceRender(context.Background(), bytes.NewReader(request), &out); err != nil {
		t.Fatal(err)
	}
	var inventory rendered.WorkloadInventory
	if err := json.Unmarshal(out.Bytes(), &inventory); err != nil {
		t.Fatalf("%v: %s", err, out.String())
	}
	return inventory
}

// The plan's workload view is the pod-template table's: every kind it lists is
// a workload with its init and regular images; other kinds are not.
func TestSourceRenderWorkloadsFollowThePodTemplateTable(t *testing.T) {
	files := map[string]string{"service.yaml": "apiVersion: v1\nkind: Service\nmetadata: {name: svc}\nspec: {ports: [{port: 80}]}\n"}
	var want []string
	for _, kind := range rendered.PodKinds() {
		files[strings.ToLower(kind)+".yaml"] = workloadManifest(kind, "w-"+strings.ToLower(kind), strings.ToLower(kind))
		want = append(want, kind+"/app/w-"+strings.ToLower(kind))
	}
	inventory := renderWorkloads(t, files)
	var got []string
	for _, workload := range inventory.Workloads {
		got = append(got, workload.ID)
		if len(workload.Images) != 2 || workload.Images[0] != "reg.invalid/init:1" {
			t.Errorf("%s images %v", workload.ID, workload.Images)
		}
	}
	if strings.Join(sorted(got), ",") != strings.Join(sorted(want), ",") {
		t.Fatalf("workloads %v, want %v", got, want)
	}
	if len(inventory.Images) != len(want)+1 || inventory.Images[0] != "reg.invalid/cronjob:1" {
		t.Fatalf("images %v: want one per kind plus the shared init image, sorted", inventory.Images)
	}
}

func TestSourceRenderRejectsAnUnknownOutput(t *testing.T) {
	request := `{"repository":"/x","revision":"abc","namespace":"app","output":"bogus"}`
	err := RunSourceRender(context.Background(), strings.NewReader(request), &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), `unknown source render output "bogus"`) {
		t.Fatalf("err = %v", err)
	}
}

func sorted(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

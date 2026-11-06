//go:build integration

package operatorcli

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/durationpb"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/planfile"
	"sdo.dev/operator/rendered"
)

const shapeWeb = `apiVersion: apps/v1
kind: Deployment
metadata: {name: web}
spec: {template: {spec: {containers: [{name: c, image: 'example/web:1', ports: [{containerPort: 80}]}]}}}
`

func shapeGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir, "-c", "user.name=t", "-c", "user.email=t@example.invalid"}, args...)...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

func shapeWrite(t *testing.T, dir, name, content string) {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestRefreshNeedsJudgeOnShapeNotSourceBytes: after a health_judge commit, a
// Go-file edit (an image's build input) needs no designer rerun; changing a
// probe/port or renaming a workload does (it reaches the registry load
// step, which a unit test has no cluster for).
func TestRefreshNeedsJudgeOnShapeNotSourceBytes(t *testing.T) {
	dir := t.TempDir()
	shapeGit(t, dir, "init", "-q")
	shapeWrite(t, dir, ".sdo/goal.md", "---\nkind: goal\n---\n# Health objective\nkeep it up\n")
	shapeWrite(t, dir, "k8s/web.yaml", shapeWeb)
	shapeWrite(t, dir, "main.go", "package main // v1\n")
	source, err := rendered.LegacyRawSource([]string{"k8s"})
	if err != nil {
		t.Fatal(err)
	}
	if err := planfile.Write(dir, &contracts.DeploymentPlan{Source: source}); err != nil {
		t.Fatal(err)
	}
	shapeGit(t, dir, "add", "-A")
	shapeGit(t, dir, "commit", "-q", "-m", "judge\n\nSDO-Actor: health_judge\nSDO-Phase: initial\nSDO-Validation: passed")
	config := &contracts.ControllerConfig{AppRoot: dir, Namespace: "app", Duration: durationpb.New(0)}

	shapeWrite(t, dir, "main.go", "package main // v2\n")
	shapeGit(t, dir, "commit", "-qam", "benign")
	if err := refreshControllerHealthOnce(context.Background(), "", config, nil); err != nil {
		t.Fatalf("benign source change required a refresh: %v", err)
	}
	for name, manifest := range map[string]string{
		"port":   strings.Replace(shapeWeb, "containerPort: 80", "containerPort: 81", 1),
		"rename": strings.Replace(shapeWeb, "name: web", "name: api", 1),
	} {
		shapeWrite(t, dir, "k8s/web.yaml", manifest)
		shapeGit(t, dir, "commit", "-qam", name)
		err := refreshControllerHealthOnce(context.Background(), "", config, nil)
		if err == nil || !strings.Contains(err.Error(), "agent registrations") {
			t.Fatalf("%s change: err = %v, want the refresh to start (registry load step)", name, err)
		}
	}
}

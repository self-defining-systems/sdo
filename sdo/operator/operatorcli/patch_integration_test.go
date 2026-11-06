//go:build integration

package operatorcli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/patchset"
)

const patchCLIReason = "The vendored config map carries the wrong retry limit and the application never starts."

const cmYAML = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: app-config\ndata:\n  retries: \"1\"\n"

const rawPlan = `{"source":{"rendererId":"raw","config":{"@type":"type.googleapis.com/sdodev.contracts.v1alpha1.RawRenderConfig","paths":["k8s"]}},"expectedWorkloadIds":["Deployment/default/app"]}`

func cliWorld(t *testing.T) string {
	t.Helper()
	accepted := t.TempDir()
	testutil.InitGit(t, accepted)
	for name, content := range map[string]string{"k8s/cm.yaml": cmYAML, "README": "readme\n"} {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(accepted, name)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(accepted, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	testutil.Git(t, accepted, "add", ".")
	testutil.Git(t, accepted, "commit", "-qm", "seed")
	work := filepath.Join(t.TempDir(), "checkout")
	testutil.Git(t, filepath.Dir(work), "clone", "-q", accepted, work)
	if _, err := patchset.Materialize(context.Background(), work); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(work, ".sdo", "deployment")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "plan.yaml"), []byte(rawPlan), 0o644); err != nil {
		t.Fatal(err)
	}
	return work
}

func runCLI(t *testing.T, run func(context.Context, []string, func(string) string, *bytes.Buffer, *bytes.Buffer) int, args ...string) (int, string, string) {
	t.Helper()
	var out, errOut bytes.Buffer
	code := run(context.Background(), args, func(key string) string {
		if key == "SDO_ACTOR" {
			return "deployer"
		}
		return ""
	}, &out, &errOut)
	return code, out.String(), errOut.String()
}

func patchCLI(t *testing.T, args ...string) (int, string, string) {
	return runCLI(t, func(ctx context.Context, a []string, g func(string) string, o, e *bytes.Buffer) int {
		return RunPatch(ctx, a, g, o, e)
	}, args...)
}

func deployCLI(t *testing.T, args ...string) (int, string, string) {
	return runCLI(t, func(ctx context.Context, a []string, g func(string) string, o, e *bytes.Buffer) int {
		return RunDeployment(ctx, a, g, o, e)
	}, args...)
}

func TestPatchCLICaptureLoop(t *testing.T) {
	work := cliWorld(t)
	if code, out, errOut := patchCLI(t, "check", "--root", work); code != 0 {
		t.Fatalf("clean check = %d\n%s%s", code, out, errOut)
	}
	if err := os.WriteFile(filepath.Join(work, "k8s", "cm.yaml"), []byte(strings.Replace(cmYAML, `"1"`, `"5"`, 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	code, out, _ := patchCLI(t, "check", "--root", work)
	if code != 1 || !strings.Contains(out, "k8s/cm.yaml: patch.uncaptured") {
		t.Fatalf("check with scratch = %d\n%s", code, out)
	}
	// The render is of what Go would deploy: the scratch edit is not in it.
	code, out, errOut := deployCLI(t, "render", "--root", work, "--json")
	if code != 0 || !strings.Contains(out, `"1"`) || strings.Contains(out, `"5"`) || !strings.Contains(errOut, "patch.uncaptured") {
		t.Fatalf("render before capture = %d\nout=%s\nerr=%s", code, out, errOut)
	}
	if code, _, errOut := patchCLI(t, "create", "--root", work); code != 2 || !strings.Contains(errOut, "--reason") {
		t.Fatalf("create without reason = %d, %s", code, errOut)
	}
	if code, _, errOut := patchCLI(t, "create", "--root", work, "--reason", "too short"); code != 1 {
		t.Fatalf("create with a short reason = %d, %s", code, errOut)
	}
	code, out, errOut = patchCLI(t, "create", "--root", work, "--reason", patchCLIReason, "k8s/cm.yaml")
	if code != 0 || !strings.Contains(out, "created .sdo/deployment/patches/0001-") {
		t.Fatalf("create = %d\n%s%s", code, out, errOut)
	}
	if code, out, _ := patchCLI(t, "check", "--root", work); code != 0 || !strings.Contains(out, "1 patch(es) apply") {
		t.Fatalf("check after capture = %d\n%s", code, out)
	}
	code, out, errOut = patchCLI(t, "list", "--root", work, "--json")
	if code != 0 || !strings.Contains(out, `"owner":"deployer"`) || !strings.Contains(out, `"paths":["k8s/cm.yaml"]`) {
		t.Fatalf("list = %d\n%s%s", code, out, errOut)
	}
	code, out, errOut = deployCLI(t, "render", "--root", work, "--json")
	if code != 0 || !strings.Contains(out, `"5"`) || errOut != "" {
		t.Fatalf("render after capture = %d\nout=%s\nerr=%s", code, out, errOut)
	}
	code, out, _ = deployCLI(t, "render", "--root", work)
	if code != 0 || !strings.Contains(out, "ConfigMap default/app-config") || !strings.Contains(out, "1 patch(es)") {
		t.Fatalf("render text = %d\n%s", code, out)
	}
}

func TestPatchCLIReportsBrokenSeriesAndUsageErrors(t *testing.T) {
	work := cliWorld(t)
	bad := "SDO-Patch: 1\nOwner: deployer\nReason: |\n  " + patchCLIReason + "\n\n" +
		"diff --git a/README b/README\nindex " + strings.Repeat("1", 40) + ".." + strings.Repeat("2", 40) + " 100644\n--- a/README\n+++ b/README\n@@ -1 +1 @@\n-nope\n+x\n"
	dir := filepath.Join(work, ".sdo", "deployment", "patches")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "0001-bad.patch"), []byte(bad), 0o644); err != nil {
		t.Fatal(err)
	}
	code, out, _ := patchCLI(t, "check", "--root", work)
	if code != 1 || !strings.Contains(out, "patch.conflict: patch conflict 0001-bad.patch") {
		t.Fatalf("check = %d\n%s", code, out)
	}
	if code, out, _ := deployCLI(t, "render", "--root", work); code != 1 || !strings.Contains(out, "patch.conflict") {
		t.Fatalf("render over a broken series = %d\n%s", code, out)
	}
	if code, _, _ := patchCLI(t, "bogus"); code != 2 {
		t.Fatalf("unknown command = %d", code)
	}
	if code, _, _ := patchCLI(t, "check", "--root", t.TempDir()); code != 2 {
		t.Fatalf("check outside a repository = %d", code)
	}
	if code, _, errOut := runCLI(t, func(ctx context.Context, a []string, g func(string) string, o, e *bytes.Buffer) int {
		return RunPatch(ctx, a, func(string) string { return "" }, o, e)
	}, "create", "--root", work, "--reason", patchCLIReason); code != 2 || !strings.Contains(errOut, "SDO_ACTOR") {
		t.Fatalf("create with no actor = %d, %s", code, errOut)
	}
}

func TestDeploymentRenderReportsTypedRenderErrors(t *testing.T) {
	work := cliWorld(t)
	if err := os.WriteFile(filepath.Join(work, "k8s", "cm.yaml"), []byte("not: [valid"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Scratch only: still renders the base, so make the broken manifest part of .sdo/deployment.
	plan := strings.Replace(rawPlan, `"paths":["k8s"]`, `"paths":[".sdo/deployment/manifests"]`, 1)
	dir := filepath.Join(work, ".sdo", "deployment")
	if err := os.MkdirAll(filepath.Join(dir, "manifests"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "manifests", "x.yaml"), []byte("not: [valid"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "plan.yaml"), []byte(plan), 0o644); err != nil {
		t.Fatal(err)
	}
	code, out, errOut := deployCLI(t, "render", "--root", work)
	if code != 1 || !strings.Contains(out, "render.invalid") {
		t.Fatalf("render = %d\nout=%s\nerr=%s", code, out, errOut)
	}
	if err := os.Remove(filepath.Join(dir, "plan.yaml")); err != nil {
		t.Fatal(err)
	}
	if code, _, errOut := deployCLI(t, "render", "--root", work); code != 2 || !strings.Contains(errOut, "plan.yaml") {
		t.Fatalf("render without a plan = %d, %s", code, errOut)
	}
}

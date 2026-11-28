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
)

const cmYAML = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: app-config\ndata:\n  retries: \"1\"\n"

const rawPlan = `{"source":{"rendererId":"raw","config":{"@type":"type.googleapis.com/sdodev.contracts.v1alpha1.RawRenderConfig","paths":["k8s"]}}}`

// cliWorld is an agent checkout of a small repository with a raw manifest and
// a deployment override in its working .sdo/.
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
	testutil.ConfigureGit(t, work)
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

func deployCLI(t *testing.T, args ...string) (int, string, string) {
	return runCLI(t, func(ctx context.Context, a []string, g func(string) string, o, e *bytes.Buffer) int {
		return RunDeployment(ctx, a, g, o, e)
	}, args...)
}

// deployment render renders what Go deploys: the committed tree. An
// uncommitted manifest is scratch; once committed it renders.
func TestDeploymentRenderRendersTheCommittedTree(t *testing.T) {
	work := cliWorld(t)
	second := "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: second\n"
	if err := os.WriteFile(filepath.Join(work, "k8s", "second.yaml"), []byte(second), 0o644); err != nil {
		t.Fatal(err)
	}
	code, out, errOut := deployCLI(t, "render", "--root", work, "--namespace", "default")
	if code != 0 || !strings.Contains(out, "ConfigMap default/app-config") || strings.Contains(out, "second") {
		t.Fatalf("render = %d %q %q: the uncommitted manifest must not render", code, out, errOut)
	}
	testutil.Git(t, work, "add", "k8s/second.yaml")
	testutil.Git(t, work, "commit", "-qm", "second")
	code, out, errOut = deployCLI(t, "render", "--root", work, "--namespace", "default")
	if code != 0 || !strings.Contains(out, "ConfigMap default/second") {
		t.Fatalf("render after commit = %d %q %q", code, out, errOut)
	}
	// Without an override the source is derived from the files.
	if err := os.Remove(filepath.Join(work, ".sdo", "deployment", "plan.yaml")); err != nil {
		t.Fatal(err)
	}
	code, out, errOut = deployCLI(t, "render", "--root", work, "--namespace", "default")
	if code != 0 || !strings.Contains(out, "ConfigMap default/second") {
		t.Fatalf("render without an override = %d %q %q", code, out, errOut)
	}
}

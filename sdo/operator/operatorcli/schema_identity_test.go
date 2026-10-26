package operatorcli

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/builder"
)

func schemaControllerArgs(root string) []string {
	return []string{"controller", "--operator-root", root, "--toolchain-image", "controller:test", "--app", "/workspace/application", "--namespace", "demo", "--responder-image", "responder:test", "--repository-pvc", "repository", "--repository-mount-path", "/workspace", "--credentials-secret", "credentials", "--worktree-root", "/workspace/worktrees", "--validator-image", "validator:test", "--model", "model", "--broker-mode=native", "--worker-mount-mode=proposal-only"}
}

// Real divergent commits must merge and bind the merged source identity without
// editing a shared generated pin in either branch.
func TestSDKBranchesMergeWithoutIdentityPins(t *testing.T) {
	root := t.TempDir()
	for _, dir := range []string{"sdk", "builder", "memory/validate", "memory/observation"} {
		if err := os.CopyFS(filepath.Join(root, dir), os.DirFS(filepath.Join("..", dir))); err != nil {
			t.Fatal(err)
		}
	}
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-c", "user.name=Schema Test", "-c", "user.email=schema@example.invalid", "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null"}, args...)...)
		cmd.Dir = root
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-b", "base")
	git("add", ".")
	git("commit", "-m", "base")
	base := git("rev-parse", "HEAD")
	identities := make(map[string]string)
	for _, branch := range []string{"first", "second"} {
		git("switch", "-c", branch, base)
		path := filepath.Join(root, "sdk", branch+".go")
		if err := os.WriteFile(path, []byte("package sdk\n// independent SDK change: "+branch+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		git("add", ".")
		git("commit", "-m", branch)
		identity, err := builder.SchemaIdentity(root)
		if err != nil {
			t.Fatal(err)
		}
		identities[branch] = identity
	}
	git("merge", "--no-edit", "first")
	if changed := git("diff", "--name-only", base); changed != "sdk/first.go\nsdk/second.go" {
		t.Fatalf("SDK branches required pin edits: %s", changed)
	}
	identity, err := builder.SchemaIdentity(root)
	if err != nil {
		t.Fatal(err)
	}
	for branch, previous := range identities {
		if previous == identity {
			t.Fatalf("merged identity ignored %s SDK change", branch)
		}
		if _, err := ParseValidateLaunch([]string{"validate", "--app", "/workspace/application", "--operator-root", root, "--expect-schema", previous}); err == nil || !strings.Contains(err.Error(), "stale validator image") {
			t.Fatalf("%s pre-merge identity accepted against merged SDK: %v", branch, err)
		}
	}
	launch, _, err := ParseControllerLaunch(schemaControllerArgs(root))
	if err != nil {
		t.Fatal(err)
	}
	if got := launch.Controller.GetSchemaIdentity(); got != identity {
		t.Fatalf("SDK branches merged without pin edits but controller identity is stale: got %s, want %s", got, identity)
	}
	if _, err := ParseValidateLaunch([]string{"validate", "--app", "/workspace/application", "--operator-root", root, "--expect-schema", launch.Controller.GetSchemaIdentity()}); err != nil {
		t.Fatalf("controller and validator disagree after merge: %v", err)
	}
}

func TestControllerSchemaIdentityFailsClosed(t *testing.T) {
	for _, test := range []struct{ name, root, expected, message string }{
		{"missing sources", t.TempDir(), "", "schema"},
		{"explicit mismatch", "..", "stale", "schema"},
	} {
		t.Run(test.name, func(t *testing.T) {
			args := schemaControllerArgs(test.root)
			if test.expected != "" {
				args = append(args, "--schema-identity", test.expected)
			}
			if _, _, err := ParseControllerLaunch(args); err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("unbound controller schema accepted: %v", err)
			}
		})
	}
}

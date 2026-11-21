package runtime

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/telemetry"
)

func gitIn(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir, "-c", "user.name=t", "-c", "user.email=t@t"}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

// The controller's --memory-upstream-url/--memory-upstream-branch reach the
// broker: accepted history the remote lacks is pushed to the branch, which an
// engineer's push must then fast-forward onto.
func TestTheControllersMemoryUpstreamConfigReplicatesAcceptedHistory(t *testing.T) {
	root := t.TempDir()
	app := filepath.Join(root, "app")
	gitIn(t, root, "init", "-q", "-b", "main", app)
	if err := os.WriteFile(filepath.Join(app, "a.txt"), []byte("one\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitIn(t, app, "add", ".")
	gitIn(t, app, "commit", "-qm", "seed")
	remote := filepath.Join(root, "remote.git")
	gitIn(t, root, "init", "-q", "--bare", remote)
	gitIn(t, app, "push", "-q", remote, "HEAD:refs/heads/sdo-source")
	if err := os.WriteFile(filepath.Join(app, "b.txt"), []byte("two\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitIn(t, app, "add", ".")
	gitIn(t, app, "commit", "-qm", "sdo's own commit")
	local := gitIn(t, app, "rev-parse", "HEAD")

	repository, err := broker.New(app, filepath.Join(root, "work"), func(context.Context, string, []string) error { return nil },
		broker.ObservationPolicy{SettingsDigest: "d", Telemetry: broker.Telemetry{Registry: telemetry.NewRegistry()}})
	if err != nil {
		t.Fatal(err)
	}
	config := &contracts.ControllerConfig{MemoryUpstream: &contracts.MemoryUpstream{Url: remote, Branch: "sdo-source"}}
	if err := applyMemoryUpstream(repository, config); err != nil {
		t.Fatal(err)
	}
	if err := repository.SyncUpstream(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := gitIn(t, remote, "rev-parse", "refs/heads/sdo-source"); got != local {
		t.Fatalf("remote branch = %s, want the accepted head %s", got, local)
	}
}

//go:build integration

package install

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestPreparedInstallUsesAcceptedPVCWithoutBootstrapPrivileges(t *testing.T) {
	root := t.TempDir()
	for _, args := range [][]string{{"init", "-q", root}, {"-C", root, "config", "user.name", "SDO Test"}, {"-C", root, "config", "user.email", "test@example.invalid"}} {
		if output, err := exec.Command("git", args...).CombinedOutput(); err != nil {
			t.Fatalf("git setup %v: %v: %s", args, err, output)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "README.md"), []byte("accepted\n"), 0644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"-C", root, "add", "README.md"}, {"-C", root, "commit", "-qm", "accepted"}} {
		if output, err := exec.Command("git", args...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	head, err := exec.Command("git", "-C", root, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	cfg := testConfig()
	cfg.AcceptedCommit = strings.TrimSpace(string(head))
	client := fake.NewSimpleClientset()
	installer := Installer{Client: client}
	if _, err := installer.InstallPrepared(context.Background(), cfg, root); err != nil {
		t.Fatalf("prepared install failed under namespaced RBAC: %v", err)
	}
	if _, err := client.BatchV1().Jobs(cfg.ControlNamespace).Get(context.Background(), ControllerJobName, metav1.GetOptions{}); err != nil {
		t.Fatalf("controller Job missing: %v", err)
	}
	for _, action := range client.Actions() {
		if action.GetResource().Resource == "namespaces" || action.GetResource().Resource == "roles" || action.GetResource().Resource == "rolebindings" || action.GetResource().Resource == "serviceaccounts" || action.GetResource().Resource == "pods" || action.GetResource().Resource == "persistentvolumeclaims" {
			t.Fatalf("prepared install attempted bootstrap operation: %s %s", action.GetVerb(), action.GetResource().Resource)
		}
	}
	wrong := cfg
	wrong.AcceptedCommit = strings.Repeat("0", 40)
	if _, err := installer.InstallPrepared(context.Background(), wrong, root); err == nil {
		t.Fatal("prepared install accepted a different Git HEAD")
	}
}

func TestAcceptedCheckoutSeederCopiesOnlyAcceptedCommit(t *testing.T) {
	repo := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", repo}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=test", "GIT_AUTHOR_EMAIL=test@invalid", "GIT_COMMITTER_NAME=test", "GIT_COMMITTER_EMAIL=test@invalid")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %s: %v", args, out, err)
		}
		return string(out)
	}
	git("init")
	if err := os.WriteFile(filepath.Join(repo, "version"), []byte("accepted"), 0600); err != nil {
		t.Fatal(err)
	}
	git("add", "version")
	git("commit", "-m", "accepted")
	accepted := strings.TrimSpace(git("rev-parse", "HEAD"))
	if err := os.WriteFile(filepath.Join(repo, "version"), []byte("later"), 0600); err != nil {
		t.Fatal(err)
	}
	git("add", "version")
	git("commit", "-m", "later")
	checkout, cleanup, err := prepareAcceptedCheckout(context.Background(), repo, accepted)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	data, err := os.ReadFile(filepath.Join(checkout, "version"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "accepted" {
		t.Fatalf("seeded %q instead of accepted commit", data)
	}
}

func TestKubeconfigSeederUsesExplicitContextWithoutChangingProcessEnvironment(t *testing.T) {
	repo := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", repo}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=test", "GIT_AUTHOR_EMAIL=test@invalid", "GIT_COMMITTER_NAME=test", "GIT_COMMITTER_EMAIL=test@invalid")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %s: %v", args, out, err)
		}
		return string(out)
	}
	git("init")
	if err := os.WriteFile(filepath.Join(repo, "version"), []byte("accepted"), 0600); err != nil {
		t.Fatal(err)
	}
	git("add", "version")
	git("commit", "-m", "accepted")
	accepted := strings.TrimSpace(git("rev-parse", "HEAD"))
	bin := t.TempDir()
	script := "#!/bin/sh\ntest \"$KUBECONFIG\" = \"$EXPECTED_KUBECONFIG\" || exit 72\ncase \" $* \" in *\" -i \"*) cat >/dev/null;; esac\n"
	if err := os.WriteFile(filepath.Join(bin, "kubectl"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
	t.Setenv("KUBECONFIG", "/tmp/wrong-context")
	t.Setenv("EXPECTED_KUBECONFIG", "/tmp/requested-context")
	cfg := testConfig()
	cfg.AcceptedCommit = accepted
	if err := KubectlTarSeederWithKubeconfig(repo, accepted, "/tmp/requested-context")(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if got := os.Getenv("KUBECONFIG"); got != "/tmp/wrong-context" {
		t.Fatalf("process KUBECONFIG changed to %q", got)
	}
}

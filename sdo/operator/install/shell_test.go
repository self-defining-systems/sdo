package install

import (
	"context"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"os"
	"os/exec"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/shellpath"
	"strings"
	"testing"
	"time"
)

func TestShellOperateJobForwardsSelectedMechanisms(t *testing.T) {
	cfg := testConfig()
	cfg.RegistryPushSecret = "push"
	cfg.CodePath = &contracts.CodePathConfig{Name: "shell", Overrides: &contracts.CodePathOverrides{AgentGuidance: "go"}}
	job, err := OperateJob(cfg, "available", "operation", 1)
	if err != nil {
		t.Fatal(err)
	}
	args := strings.Join(job.Spec.Template.Spec.Containers[0].Args, " ")
	if !strings.Contains(args, "--code-path shell") || !strings.Contains(args, "--code-path-overrides") || !strings.Contains(args, "agent_guidance") {
		t.Fatal(args)
	}
}
func TestShellPreparedInstallUsesOnlyRuntimePrivileges(t *testing.T) {
	root := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		data, err := exec.Command("git", append([]string{"-C", root}, args...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("git: %v %s", err, data)
		}
		return strings.TrimSpace(string(data))
	}
	git("init", "-q")
	git("config", "user.name", "T")
	git("config", "user.email", "t@example.com")
	if err := os.WriteFile(filepath.Join(root, "README.md"), []byte("accepted"), 0644); err != nil {
		t.Fatal(err)
	}
	git("add", ".")
	git("commit", "-qm", "accepted")
	cfg := testConfig()
	cfg.AcceptedCommit = git("rev-parse", "HEAD")
	cfg.CodePath = &contracts.CodePathConfig{Name: "shell"}
	client := fake.NewSimpleClientset(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: cfg.CredentialsSecret, Namespace: cfg.ControlNamespace}})
	installer := Installer{Client: client, PollInterval: time.Millisecond}
	go markSyncPodReady(client, cfg.ControlNamespace)
	if err := installer.Prepare(context.Background(), cfg, func(context.Context, Config) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().Pods(cfg.ControlNamespace).Get(context.Background(), shellpath.RunnerName, metav1.GetOptions{}); err != nil {
		t.Fatal("host preparation did not provision runner", err)
	}
	client.PrependReactor("*", "*", func(action ktesting.Action) (bool, kruntime.Object, error) {
		resource := action.GetResource().Resource
		if (resource == "roles" || resource == "rolebindings" || resource == "serviceaccounts") && action.GetVerb() != "get" && action.GetVerb() != "list" {
			t.Fatalf("prepared runtime attempted %s %s", action.GetVerb(), resource)
		}
		return false, nil, nil
	})
	if _, err := installer.InstallPrepared(context.Background(), cfg, root); err != nil {
		t.Fatal(err)
	}
}

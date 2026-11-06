package operatorcli

import (
	"strings"
	"testing"
)

func TestParseSourceFlags(t *testing.T) {
	good := `{"repository":"git://synthetic.invalid/app.git","ref":"main","poll_interval":"30s"}`
	sources, err := parseSourceFlags("git", good)
	if err != nil || len(sources) != 1 || sources[0].Path != "." {
		t.Fatalf("good = %+v, %v", sources, err)
	}
	if sources, err := parseSourceFlags("", ""); err != nil || sources != nil {
		t.Fatalf("absent = %+v, %v", sources, err)
	}
	for name, tc := range map[string]struct{ kind, config, want string }{
		"kind only":     {"git", "", "together"},
		"config only":   {"", good, "together"},
		"not json":      {"git", "repository=x", "JSON object"},
		"unknown kind":  {"svn", good, "unknown upstream watcher"},
		"unknown field": {"git", `{"repository":"r","ref":"main","branch":"x"}`, "branch"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseSourceFlags(tc.kind, tc.config); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

func operateArgs(extra ...string) []string {
	args := []string{
		"operate", "--app", "/workspace/application", "--operator-root", "/opt/sdo/sdo/operator",
		"--namespace", "app", "--application", "app", "--health-objective", "stay healthy",
		"--operation-id", "op-1", "--controller-image", "controller:test", "--responder-image", "responder:test",
		"--buildkit-image", "buildkit@sha256:" + strings.Repeat("a", 64), "--registry-push-secret", "push",
		"--repository-pvc", "repo", "--repository-mount-path", "/workspace", "--credentials-secret", "creds",
		"--model", "m", "--worktree-root", "/workspace/worktrees",
	}
	return append(args, extra...)
}

func TestOperateSourceFlagsReachLifecycleAndInstall(t *testing.T) {
	config := `{"repository":"git://synthetic.invalid/app.git","ref":"main"}`
	launch, err := ParseOperateLaunch(operateArgs("--source-kind", "git", "--source-config", config))
	if err != nil {
		t.Fatal(err)
	}
	if len(launch.Lifecycle.Sources) != 1 || launch.Lifecycle.Sources[0].Kind != "git" {
		t.Fatalf("lifecycle sources = %+v", launch.Lifecycle.Sources)
	}
	if launch.Install.SourceKind != "git" || launch.Install.SourceConfig != config {
		t.Fatalf("install source = %q %q", launch.Install.SourceKind, launch.Install.SourceConfig)
	}
	plain, err := ParseOperateLaunch(operateArgs())
	if err != nil || len(plain.Lifecycle.Sources) != 0 {
		t.Fatalf("no flags = %+v, %v", plain.Lifecycle.Sources, err)
	}
	if _, err := ParseOperateLaunch(operateArgs("--source-kind", "git")); err == nil {
		t.Fatal("kind without config accepted")
	}
	noGoal := strings.Replace(strings.Join(operateArgs("--source-kind", "git", "--source-config", config), "\x00"), "stay healthy", "", 1)
	if _, err := ParseOperateLaunch(strings.Split(noGoal, "\x00")); err == nil {
		t.Fatal("a source without a health objective accepted: nothing would publish it")
	}
}

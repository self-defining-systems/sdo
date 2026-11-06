package install

import (
	"slices"
	"strings"
	"testing"
)

func TestOperateJobPassesFollowedSource(t *testing.T) {
	cfg := Config{
		Namespace: "app", Application: "app", RepositoryPVC: "repo", CredentialsSecret: "creds",
		ControllerImage: "controller:test", ResponderImage: "responder:test", Model: "m", Replay: "on", NativeCommand: []string{"/usr/local/bin/sdo-operator"}, AcceptedCommit: strings.Repeat("c", 40), TimeoutSeconds: 60, VerificationTimeoutSeconds: 60,
		BuildKitImage: "buildkit@sha256:" + strings.Repeat("a", 64), RegistryPushSecret: "push",
		SourceKind: "git", SourceConfig: `{"repository":"git://synthetic.invalid/app.git","ref":"main"}`,
	}
	job, err := OperateJob(cfg, "stay healthy", "op-1", 3)
	if err != nil {
		t.Fatal(err)
	}
	args := job.Spec.Template.Spec.Containers[0].Args
	i := slices.Index(args, "--source-kind")
	if i < 0 || args[i+1] != "git" || args[i+2] != "--source-config" || args[i+3] != cfg.SourceConfig {
		t.Fatalf("operate args lack the followed source: %v", args)
	}
	cfg.SourceKind, cfg.SourceConfig = "", ""
	job, err = OperateJob(cfg, "stay healthy", "op-1", 3)
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(job.Spec.Template.Spec.Containers[0].Args, "--source-kind") {
		t.Fatal("source flags passed without a source")
	}
	cfg.SourceKind = "git"
	if _, err := OperateJob(cfg, "stay healthy", "op-1", 3); err == nil {
		t.Fatal("a kind without a config must be rejected")
	}
}

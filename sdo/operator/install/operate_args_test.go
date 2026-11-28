package install

import (
	"slices"
	"strings"
	"testing"
)

func TestOperateJobPassesStopAfterArchitecture(t *testing.T) {
	cfg := Config{
		Namespace: "app", Application: "app", RepositoryPVC: "repo", CredentialsSecret: "creds",
		ControllerImage: "controller:test", ResponderImage: "responder:test", Settings: testSettings(), NativeCommand: []string{"/usr/local/bin/sdo-operator"}, AcceptedCommit: strings.Repeat("c", 40),
		BuildKitImage: "buildkit@sha256:" + strings.Repeat("a", 64), RegistryPushSecret: "push",
	}
	job, err := OperateJob(cfg, "stay healthy", "op-1")
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(job.Spec.Template.Spec.Containers[0].Args, "--stop-after-architecture") {
		t.Fatal("--stop-after-architecture passed by default")
	}
	cfg.StopAfterArchitecture = true
	job, _ = OperateJob(cfg, "stay healthy", "op-1")
	if !slices.Contains(job.Spec.Template.Spec.Containers[0].Args, "--stop-after-architecture") {
		t.Fatal("--stop-after-architecture not passed")
	}
}

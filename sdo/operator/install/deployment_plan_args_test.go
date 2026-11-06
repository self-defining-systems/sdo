package install

import (
	"slices"
	"strings"
	"testing"
)

func TestOperateJobPassesProvidedDeploymentPlan(t *testing.T) {
	cfg := Config{
		Namespace: "app", Application: "app", RepositoryPVC: "repo", CredentialsSecret: "creds",
		ControllerImage: "controller:test", ResponderImage: "responder:test", Model: "m", Replay: "on", NativeCommand: []string{"/usr/local/bin/sdo-operator"}, AcceptedCommit: strings.Repeat("c", 40), TimeoutSeconds: 60, VerificationTimeoutSeconds: 60,
		BuildKitImage: "buildkit@sha256:" + strings.Repeat("a", 64), RegistryPushSecret: "push",
		DeploymentPlan: `{"source":{"rendererId":"raw"}}`,
	}
	job, err := OperateJob(cfg, "stay healthy", "op-1", 3)
	if err != nil {
		t.Fatal(err)
	}
	args := job.Spec.Template.Spec.Containers[0].Args
	if i := slices.Index(args, "--deployment-plan"); i < 0 || args[i+1] != cfg.DeploymentPlan {
		t.Fatalf("operate args lack the provided plan: %v", args)
	}
	cfg.DeploymentPlan = ""
	job, err = OperateJob(cfg, "stay healthy", "op-1", 3)
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(job.Spec.Template.Spec.Containers[0].Args, "--deployment-plan") {
		t.Fatal("--deployment-plan passed without a plan")
	}
}

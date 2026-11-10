package install

import (
	"slices"
	"strings"
	"testing"
	"time"
)

// A configured designer session limit reaches the operate Job's workers, and
// the Job's own deadline leaves every attempt room for a session that long.
func TestOperateJobCarriesAndBudgetsTheLifecycleTimeout(t *testing.T) {
	cfg := Config{
		Namespace: "app", Application: "app", RepositoryPVC: "repo", CredentialsSecret: "creds",
		ControllerImage: "controller:test", ResponderImage: "responder:test", Model: "m", Replay: "on", NativeCommand: []string{"/usr/local/bin/sdo-operator"}, AcceptedCommit: strings.Repeat("c", 40), TimeoutSeconds: 60, VerificationTimeoutSeconds: 60,
		BuildKitImage: "buildkit@sha256:" + strings.Repeat("a", 64), RegistryPushSecret: "push",
		LifecycleTimeout: 3 * time.Hour,
	}
	job, err := OperateJob(cfg, "stay healthy", "op-1", 3)
	if err != nil {
		t.Fatal(err)
	}
	args := job.Spec.Template.Spec.Containers[0].Args
	if i := slices.Index(args, "--lifecycle-timeout"); i < 0 || args[i+1] != "3h0m0s" {
		t.Fatalf("operate args lack the lifecycle timeout: %v", args)
	}
	if deadline := time.Duration(*job.Spec.ActiveDeadlineSeconds) * time.Second; deadline < 3*cfg.LifecycleTimeout {
		t.Fatalf("operate Job deadline %s leaves no room for three %s designer sessions", deadline, cfg.LifecycleTimeout)
	}
	cfg.LifecycleTimeout = -time.Second
	if _, err := OperateJob(cfg, "stay healthy", "op-1", 3); err == nil {
		t.Fatal("a negative lifecycle timeout must be rejected")
	}
}

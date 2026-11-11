package install

import (
	"slices"
	"strings"
	"testing"
)

// Watch-only reaches both the operate Job and the controller it installs, and
// is refused without frozen memory (learning must stay off) or beside external
// incidents.
func TestWatchOnlyReachesTheOperateJobAndController(t *testing.T) {
	cfg := Config{
		Namespace: "app", Application: "app", RepositoryPVC: "repo", CredentialsSecret: "creds",
		ControllerImage: "controller:test", ResponderImage: "responder:test", Model: "m", Replay: "on", NativeCommand: []string{"/usr/local/bin/sdo-operator"}, AcceptedCommit: strings.Repeat("c", 40), TimeoutSeconds: 60, VerificationTimeoutSeconds: 60,
		BuildKitImage: "buildkit@sha256:" + strings.Repeat("a", 64), RegistryPushSecret: "push",
		FrozenMemory: true, WatchOnly: true,
	}
	job, err := OperateJob(cfg, "stay healthy", "op-1", 3)
	if err != nil {
		t.Fatal(err)
	}
	if args := job.Spec.Template.Spec.Containers[0].Args; !slices.Contains(args, "--watch-only") || !slices.Contains(args, "--frozen-memory") {
		t.Fatalf("operate args lack watch-only: %v", args)
	}
	if args := controllerJob(cfg).Spec.Template.Spec.Containers[0].Args; !slices.Contains(args, "--watch-only") {
		t.Fatalf("controller args lack watch-only: %v", args)
	}
	unfrozen := cfg
	unfrozen.FrozenMemory = false
	if err := unfrozen.validate(); err == nil || !strings.Contains(err.Error(), "frozen memory") {
		t.Fatalf("watch-only without frozen memory: err = %v", err)
	}
	external := cfg
	external.ExternalIncidents = true
	if err := external.validate(); err == nil || !strings.Contains(err.Error(), "external incidents") {
		t.Fatalf("watch-only with external incidents: err = %v", err)
	}
}

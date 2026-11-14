package worker_registry

import (
	"slices"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// Only the responder writes, so only its sidecar reads the checkout (read-only)
// to name the commit of each write.
func TestResponderProxyReadsTheCheckoutToNameWriteCommits(t *testing.T) {
	job, _ := launchedWithSession(t, &contracts.IncidentRequest{IncidentId: "inc-1"}, "task-1")
	side := sidecar(job)
	index := slices.Index(side.Command, "--worktree")
	if index < 0 || side.Command[index+1] != "/work" {
		t.Fatalf("sidecar command = %v, want --worktree /work", side.Command)
	}
	found := false
	for _, mount := range side.VolumeMounts {
		if mount.MountPath == "/work" {
			found = true
			if !mount.ReadOnly {
				t.Fatal("the proxy must not be able to write the checkout")
			}
		}
	}
	if !found {
		t.Fatalf("sidecar mounts = %+v, want the checkout", side.VolumeMounts)
	}
	reader, _ := launchedWithSession(t, &contracts.ReflectionRequest{}, "task-1")
	if slices.Contains(sidecar(reader).Command, "--worktree") {
		t.Fatal("a read-only scope has no writes to attribute")
	}
}

package operatorcli

import "strings"

// operateArgs is a complete synthetic `operate` command line; tests append the
// flags they exercise.
func operateArgs(extra ...string) []string {
	args := []string{
		"operate", "--app", "/workspace/application", "--operator-root", "/opt/sdo/sdo/operator",
		"--namespace", "app", "--application", "app", "--health-objective", "stay healthy",
		"--operation-id", "op-1", "--controller-image", "controller:test", "--responder-image", "responder:test",
		"--buildkit-image", "buildkit@sha256:" + strings.Repeat("a", 64), "--registry-push-secret", "push",
		"--repository-pvc", "repo", "--repository-mount-path", "/workspace", "--credentials-secret", "creds",
		"--worktree-root", "/workspace/worktrees",
	}
	return append(args, extra...)
}

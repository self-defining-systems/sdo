//go:build integration && controller_e2e

package e2e

// wrapRegistration returns a copy of an agent registration whose command first
// consults /e2e/agent-mode (written by the harness) and otherwise runs the
// registered no-model worker unchanged.
func wrapRegistration(registration map[string]any) map[string]any {
	job, ok := registration["kubernetes_job"].(map[string]any)
	if !ok {
		return nil
	}
	original, _ := job["command"].([]any)
	if len(original) == 0 {
		return nil
	}
	script := `m=$(cat /e2e/agent-mode 2>/dev/null || echo normal)
case "$m" in
crash) echo "scripted agent crash" >&2; exit 3;;
hang) exec sleep 86400;;
esac
exec "$@"`
	command := []any{"bash", "-c", script, "agent"}
	command = append(command, original...)
	wrapped := map[string]any{}
	for k, v := range registration {
		wrapped[k] = v
	}
	newJob := map[string]any{}
	for k, v := range job {
		newJob[k] = v
	}
	newJob["command"] = command
	wrapped["kubernetes_job"] = newJob
	return wrapped
}

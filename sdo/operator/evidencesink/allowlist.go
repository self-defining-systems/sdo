package evidencesink

import (
	"regexp"
	"strings"
)

// ResolvedConfigFile is the controller's copy of the operator configuration it runs under.
const ResolvedConfigFile = "config.yaml"

var commandReportName = regexp.MustCompile(`^[0-9]{4,}-(apply|mitigate)\.json$`)

// Allowed reports whether one path relative to the runtime evidence root (.sdo-runtime) is run evidence that may
// leave the cluster: agent session rollouts, task requests and results, usage, telemetry, command reports,
// checkpoints, model I/O, corpus captures and startup timings. Caches, plugin catalogs and anything named like a
// login, credential or token (other than token_usage.json) never are. Both the evidence sink and the
// runtime-artifacts export apply this one allowlist.
func Allowed(name string) bool {
	parts := strings.Split(name, "/")
	lowerPath := strings.ToLower(name)
	if strings.Contains(lowerPath, "cache") || strings.Contains(lowerPath, "plugin") || strings.Contains(lowerPath, "catalog") {
		return false
	}
	base := strings.ToLower(parts[len(parts)-1])
	if base != "token_usage.json" && (strings.Contains(base, "auth") || strings.Contains(base, "credential") || strings.Contains(base, "token")) {
		return false
	}
	if name == ResolvedConfigFile {
		return true
	}
	if len(parts) == 2 && (parts[0] == "usage" || parts[0] == "telemetry") {
		return strings.HasSuffix(base, ".jsonl") || parts[0] == "telemetry" && strings.HasSuffix(base, ".jsonl.1")
	}
	if len(parts) >= 3 && parts[0] == "codex" && parts[1] == "sessions" {
		return strings.HasPrefix(base, "rollout-") && strings.HasSuffix(base, ".jsonl")
	}
	if len(parts) >= 3 && parts[0] == "claude" && parts[1] == "projects" {
		return strings.HasSuffix(base, ".jsonl")
	}
	if len(parts) == 3 && parts[0] == "setup" {
		return strings.HasSuffix(base, ".jsonl")
	}
	if len(parts) >= 3 && parts[0] == "tasks" {
		return allowedTaskArtifact(parts, base)
	}
	if len(parts) < 3 || parts[0] != "sessions" {
		return false
	}
	if len(parts) == 3 {
		return strings.HasSuffix(base, ".jsonl") || base == "token_usage.json" || base == IncidentIdentityFile || StartupTimingArtifact(base)
	}
	if len(parts) == 5 && parts[2] == "startup-workers" {
		return base == "startup-worker.json"
	}
	if len(parts) == 6 && parts[2] == "startup" && parts[3] == "attempts" {
		return base == "startup-controller.json" || base == "startup-dispatch.json"
	}
	if len(parts) == 4 && parts[2] == "startup" {
		return base == "startup-controller.json" || base == "startup-session.json" || base == "startup-dispatch.json"
	}
	if len(parts) == 6 && parts[2] == "scratch" && parts[3] == "sdo-reports" {
		return commandReportName.MatchString(parts[5])
	}
	if len(parts) == 4 && parts[2] == "corpus" {
		return strings.HasSuffix(base, ".json")
	}
	if len(parts) == 4 && agentRecordingDirectory(parts[2]) {
		return agentRecordingFile(base)
	}
	if len(parts) == 5 && parts[2] == "checkpoints" {
		return base == "request.json" || base == "result.json" || base == "file_edits.json"
	}
	if len(parts) == 5 && parts[2] == "model_io" {
		return strings.HasSuffix(base, ".json")
	}
	if len(parts) >= 5 && parts[2] == "codex" && parts[3] == "sessions" {
		return strings.HasPrefix(base, "rollout-") && strings.HasSuffix(base, ".jsonl")
	}
	return len(parts) >= 5 && parts[2] == "claude" && parts[3] == "projects" && strings.HasSuffix(base, ".jsonl")
}

// allowedTaskArtifact admits one lifecycle task's evidence directory (tasks/<key>/): the recorded task, its
// request and outcome, the worker's JSONL logs, and the agent CLI's session rollouts.
func allowedTaskArtifact(parts []string, base string) bool {
	switch {
	case len(parts) == 4 && parts[2] == "api-history":
		return agentRecordingFile(base)
	case len(parts) == 3:
		return base == "task.json" || base == "request.json" || base == "result.json" || base == "error.json" || strings.HasSuffix(base, ".jsonl")
	case parts[2] == "codex" && len(parts) >= 5 && parts[3] == "sessions":
		return strings.HasPrefix(base, "rollout-") && strings.HasSuffix(base, ".jsonl")
	case parts[2] == "claude" && len(parts) >= 5 && parts[3] == "projects":
		return strings.HasSuffix(base, ".jsonl")
	}
	return false
}

// agentRecordingDirectory names a directory of the API proxy's recording of an agent's cluster calls: the
// agent's own (api-history) and the reflection agent's (api-history-reflection, beside the incident's).
func agentRecordingDirectory(name string) bool {
	return name == "api-history" || name == "api-history-reflection"
}

// agentRecordingFile admits the redacted event log and its manifest. Projected response blobs, the identity key
// and the mutation fence stay in the cluster.
func agentRecordingFile(base string) bool { return base == "events.jsonl" || base == "manifest.json" }

// StartupTimingArtifact reports whether base names one of the startup timing documents a reader validates.
func StartupTimingArtifact(base string) bool {
	switch base {
	case "startup-controller.json", "startup-worker.json", "startup-daemon.json", "startup-session.json", "startup-dispatch.json":
		return true
	}
	return false
}

// descend reports whether a runtime directory can hold allowed evidence, so a scan never walks build caches,
// toolchains or backups, and only the redacted API recording (never its blobs).
func descend(relative string) bool {
	parts := strings.Split(relative, "/")
	lower := strings.ToLower(parts[len(parts)-1])
	if strings.Contains(lower, "cache") || strings.Contains(lower, "plugin") || strings.Contains(lower, "catalog") {
		return false
	}
	switch parts[0] {
	case "sessions":
		if len(parts) >= 4 && parts[2] == "scratch" {
			return parts[3] == "sdo-reports"
		}
		if len(parts) >= 3 {
			switch parts[2] {
			case "scratch", "startup", "startup-workers", "corpus", "checkpoints", "model_io", "codex", "claude":
				return true
			case "api-history", "api-history-reflection":
				return len(parts) == 3
			}
			return false
		}
		return true
	case "tasks":
		return len(parts) < 3 || parts[2] == "codex" || parts[2] == "claude" || (parts[2] == "api-history" && len(parts) == 3)
	case "usage", "telemetry":
		return len(parts) == 1
	case "setup", "codex", "claude":
		return true
	}
	return false
}

//go:build integration

package operatorcli

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"sdo.dev/operator/worker_registry"
)

func writeFixture(t *testing.T, root, name string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("{}\n"), 0600); err != nil {
		t.Fatal(err)
	}
}

// The export archives each lifecycle task's directory (request, outcome, usage and the agent CLI's rollout) and
// never the provider login or a cache beside them.
func TestRuntimeArchiveCarriesLifecycleTaskEvidence(t *testing.T) {
	runtime := t.TempDir()
	key := worker_registry.TaskEvidenceKey("judge-1")
	task := worker_registry.TaskEvidenceRoot[len(".sdo-runtime/"):] + "/" + key
	kept := []string{"task.json", "request.json", "result.json", "error.json", "setup-usage.jsonl", "agent-resolved.jsonl", "codex/sessions/2026/11/06/rollout-a.jsonl", "claude/projects/p/s.jsonl"}
	dropped := []string{"codex/auth.json", "claude/.credentials.json", "codex/skills/x/SKILL.md", "codex/cache/rollout-b.jsonl", "scratch.txt"}
	for _, name := range append(append([]string{}, kept...), dropped...) {
		writeFixture(t, runtime, task+"/"+name)
	}
	for _, live := range []bool{false, true} {
		command := archiveCommand(live)
		command[4] = runtime
		var archive bytes.Buffer
		cmd := exec.Command(command[0], command[1:]...)
		cmd.Stdout = &archive
		if err := cmd.Run(); err != nil {
			t.Fatalf("archive script (live=%v): %v", live, err)
		}
		destination := t.TempDir()
		if err := extractRuntimeArtifacts(&archive, destination); err != nil {
			t.Fatal(err)
		}
		for _, name := range kept {
			if _, err := os.Stat(filepath.Join(destination, task, name)); err != nil {
				t.Errorf("live=%v: %s not exported: %v", live, name, err)
			}
		}
		for _, name := range dropped {
			if _, err := os.Stat(filepath.Join(destination, task, name)); err == nil {
				t.Errorf("live=%v: %s must not be exported", live, name)
			}
		}
	}
}

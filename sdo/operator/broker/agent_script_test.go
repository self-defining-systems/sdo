package broker

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The external CLI replaces only the model. Python's real judge worker parses
// the turn; the shared memory CLI and broker both validate the authored bytes.
func TestAgentScriptJudgeMemoryCheckAndBroker(t *testing.T) {
	ctx := context.Background()
	if _, err := exec.LookPath("bwrap"); err != nil {
		t.Skip("agent scripts require bubblewrap")
	}
	repoPath, _ := filepath.Abs("../../..")
	if _, err := os.Stat(filepath.Join(repoPath, ".venv/bin/python")); err != nil {
		t.Skip("agent scripts require uv sync --extra test")
	}
	b, root := judgeBroker(t)
	proposal, err := b.CreateProposalForPhase(ctx, "script-judge", InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	fakeJudgeEdits(t, proposal.Path)
	writeJudgeFile(t, proposal.Path, ".sdo/diagnostics/detectors/health/health-objective/manifest.yaml", detectorYAML("health", "health-objective", 1, "base"))
	detector, err := os.ReadFile("../memory/validate/testdata/good/.sdo/diagnostics/detectors/health/health/detector.go")
	if err != nil {
		t.Fatal(err)
	}
	writeJudgeFile(t, proposal.Path, ".sdo/diagnostics/detectors/health/health-objective/detector.go", strings.ReplaceAll(string(detector), `ID: "health"`, `ID: "health-objective"`))
	writeJudgeFile(t, proposal.Path, ".sdo/diagnostics/traffic/generators/generators.go", `package generators
import "sdo.dev/operator/sdk/traffic"
func Scenarios() traffic.Catalog { return traffic.Catalog{{ID: "api", Description: "synthetic api read", Target: traffic.Target{Service:"api",Port:80}, DependsOn: []string{"api"}, SideEffect: traffic.SideEffectRead, Steps: []traffic.Step{{Name:"read", Endpoint:traffic.GET("/",nil)}}}} }
`)
	var steps []map[string]any
	err = filepath.WalkDir(filepath.Join(proposal.Path, ".sdo"), func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		relative, _ := filepath.Rel(proposal.Path, path)
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		steps = append(steps, map[string]any{"kind": "write", "path": filepath.ToSlash(relative), "text": string(data)})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	git(t, proposal.Path, "reset", "--hard", proposal.BaseCommit)
	git(t, proposal.Path, "clean", "-fd")
	repo, _ := filepath.Abs("../../..")
	binary := filepath.Join(t.TempDir(), "sdo-operator")
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, "../cmd/sdo-operator")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v: %s", err, output)
	}
	steps = append(steps, map[string]any{"kind": "tool", "argv": []string{"sdo-operator", "memory", "check"}})
	plan := map[string]any{"version": 1, "turns": []any{map[string]any{"role": "health-judge", "steps": steps, "result": map[string]any{
		"round": 1, "objective_digest": "f65dbb891dca43a5e77c53610a6f57bdc8ad2269f726cecd62e31142a0c9f5b1", "source_commit": proposal.BaseCommit,
		"covered_resources": []any{}, "failure_patterns": []string{"synthetic failure"}, "detector_ids": []string{"health-objective"},
	}}}}
	// The fixture runner fills the objective digest from its synthetic request.
	data, _ := json.Marshal(plan)
	script := filepath.Join(t.TempDir(), "script.json")
	if err := os.WriteFile(script, data, 0o600); err != nil {
		t.Fatal(err)
	}
	python := filepath.Join(repo, ".venv/bin/python")
	command := exec.CommandContext(ctx, python, "-m", "benchmarks.sregym.agent_script.judge_example", proposal.Path, script)
	command.Env = append(os.Environ(), "PYTHONPATH="+repo, "PATH="+filepath.Dir(binary)+":"+os.Getenv("PATH"), "SDO_ACTOR=health_judge", "SDO_PHASE=initial", "SDO_BASE_COMMIT="+proposal.BaseCommit, "SDO_OPERATOR_ROOT="+filepath.Join(repo, "sdo/operator"))
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("script judge worker: %v: %s", err, output)
	}
	receipt, err := b.Publish(ctx, "script-judge", HealthJudge, InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Commit == "" {
		t.Fatal("missing accepted judge commit")
	}
}

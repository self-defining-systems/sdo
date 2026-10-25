// Produce accepted setup commits through the real broker and detector compiler.
package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"

	"sdo.dev/operator/broker"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/rendered"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func main() {
	ctx := context.Background()
	root := os.Args[1]
	operator, err := os.Getwd()
	must(err)
	authority, err := broker.New(root, root+"-proposals", func(ctx context.Context, candidate string, _ []string) error {
		diagnostics, err := validate.CompileCandidate(ctx, validate.Request{Root: candidate, OperatorRoot: operator})
		if err != nil {
			return err
		}
		return validate.Err(diagnostics)
	})
	must(err)
	proposal, err := authority.CreateProposalForPhase(ctx, "health-setup", broker.InitialPhase)
	must(err)
	health := ".sdo/diagnostics/detectors/health/"
	must(os.MkdirAll(filepath.Join(proposal.Path, health, "health-objective"), 0755))
	for _, name := range []string{"manifest.yaml", "detector.go"} {
		body, err := os.ReadFile(filepath.Join(proposal.Path, health, "toy-endpoints", name))
		must(err)
		must(os.WriteFile(filepath.Join(proposal.Path, health, "health-objective", name), []byte(strings.ReplaceAll(string(body), "toy-endpoints", "health-objective")), 0644))
	}
	index := filepath.Join(proposal.Path, ".sdo/playbooks/README.md")
	body, err := os.ReadFile(index)
	must(err)
	must(os.WriteFile(index, append(body, []byte("\n<!-- initial health setup -->\n")...), 0644))
	receipt, err := authority.Publish(ctx, "health-setup", broker.HealthJudge, broker.InitialPhase)
	must(err)
	snapshot, err := (rendered.Source{Repository: root, ManifestPaths: []string{"service.yaml"}}).BuildLock(ctx, receipt.Commit, "sdo-toy")
	must(err)
	head, err := authority.RecordRenderedLock(ctx, receipt.Commit, snapshot)
	must(err)
	must(json.NewEncoder(os.Stdout).Encode(map[string]any{"head": head, "health_paths": receipt.ChangedPaths}))
}

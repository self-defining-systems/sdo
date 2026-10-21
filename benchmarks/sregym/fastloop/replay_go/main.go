package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/builder"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/operatorcli"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	checkpoint := flag.String("checkpoint", "", "reflection checkpoint directory")
	source := flag.String("source", "", "local accepted repository")
	operatorRoot := flag.String("operator-root", "", "current operator module")
	flag.Parse()
	if *checkpoint == "" || *source == "" || *operatorRoot == "" || flag.NArg() != 0 {
		return errors.New("replay broker requires checkpoint, source, and operator-root")
	}
	requestWire, err := os.ReadFile(filepath.Join(*checkpoint, "request.json"))
	if err != nil {
		return err
	}
	resultWire, err := os.ReadFile(filepath.Join(*checkpoint, "result.json"))
	if err != nil {
		return err
	}
	if len(resultWire) > 700<<10 {
		return errors.New("recorded reflection result exceeds worker transport limit")
	}
	task := new(contracts.AgentTask)
	result := new(contracts.AgentResult)
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(requestWire, task); err != nil {
		return fmt.Errorf("decode task: %w", err)
	}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(resultWire, result); err != nil {
		return fmt.Errorf("decode result: %w", err)
	}
	if task.GetTaskType() != "outcome-reflection" || result.GetTaskId() != task.GetTaskId() || result.GetProposalId() != task.GetWorkspace().GetProposalId() || result.GetStatus() != "completed" {
		return errors.New("recorded reflection result does not match dispatch")
	}
	request := new(contracts.ReflectionRequest)
	response := new(contracts.ReflectionResult)
	if err := task.GetRequest().UnmarshalTo(request); err != nil {
		return fmt.Errorf("decode reflection request: %w", err)
	}
	if err := result.GetResponse().UnmarshalTo(response); err != nil {
		return fmt.Errorf("decode reflection result: %w", err)
	}
	if request.GetOutcomeCommit() == "" || request.GetOutcomeCommit() != task.GetWorkspace().GetBaseCommit() || request.GetMemorySnapshot().GetAcceptedCommit() != request.GetOutcomeCommit() {
		return errors.New("reflection is not bound to its accepted outcome commit")
	}
	if response.GetLearningDecision() != "updated" {
		if response.GetLearningDecision() != "no_change" && response.GetLearningDecision() != "sdk_gap" {
			return errors.New("reflection has no valid learning decision")
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]string{"status": response.GetLearningDecision()})
	}
	if response.GetArtifactProposal() == nil {
		return errors.New("updated reflection omitted typed artifacts")
	}
	root, err := os.MkdirTemp("", "sdo-fastloop-broker-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(root)
	accepted := filepath.Join(root, "accepted")
	if err := git(ctx, "", "clone", "--quiet", "--no-hardlinks", "--no-checkout", "--", *source, accepted); err != nil {
		return err
	}
	if err := git(ctx, accepted, "reset", "--quiet", "--hard", request.GetOutcomeCommit()); err != nil {
		return err
	}
	if err := git(ctx, accepted, "config", "user.name", "Fastloop Replay"); err != nil {
		return err
	}
	if err := git(ctx, accepted, "config", "user.email", "fastloop-replay@example.invalid"); err != nil {
		return err
	}
	validator := func(ctx context.Context, candidate string, _ []string) error {
		return operatorcli.RunValidate(ctx, builder.Config{ApplicationRoot: candidate, OperatorRoot: *operatorRoot})
	}
	repository, err := broker.New(accepted, filepath.Join(root, "proposals"), validator)
	if err != nil {
		return err
	}
	incidentID := request.GetOutcome().GetIncidentId()
	proposal, err := repository.CreateProposalForPhase(ctx, incidentID, broker.ReflectionPhase)
	if err != nil {
		return err
	}
	if proposal.BaseCommit != request.GetOutcomeCommit() {
		return errors.New("broker replay base differs from recorded outcome")
	}
	if err := memory.MaterializeArtifacts(proposal.Path, response.GetArtifactProposal(), memory.ResponderArtifacts, filepath.Join(*checkpoint, "captures")); err != nil {
		return fmt.Errorf("materialize recorded result: %w", err)
	}
	receipt, err := repository.Publish(ctx, incidentID, broker.Responder, broker.ReflectionPhase)
	if err != nil {
		return fmt.Errorf("validate recorded result: %w", err)
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]string{"status": "accepted", "commit": receipt.Commit})
}

func git(ctx context.Context, root string, args ...string) error {
	argv := append([]string{}, args...)
	if root != "" {
		argv = append([]string{"-C", root}, args...)
	}
	command := exec.CommandContext(ctx, "git", argv...)
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return nil
}

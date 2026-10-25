package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/builder"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/validate"
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
	if task.GetTaskType() != "outcome-reflection-direct-v1" || result.GetTaskId() != task.GetTaskId() || result.GetProposalId() != task.GetWorkspace().GetProposalId() || result.GetStatus() != "completed" {
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
	if response.GetArtifactProposal() != nil {
		return errors.New("proposal-only reflection recordings are unsupported; record direct file edits")
	}
	editsWire, err := os.ReadFile(filepath.Join(*checkpoint, "file_edits.json"))
	if os.IsNotExist(err) && response.GetLearningDecision() == "no_change" && response.GetNoChangeReason() != "" && len(response.GetProposedArtifactIds()) == 0 {
		return json.NewEncoder(os.Stdout).Encode(map[string]string{"status": "no_change"})
	}
	if err != nil {
		return fmt.Errorf("reflection recording requires direct file edits: %w", err)
	}
	var recorded recordedEdits
	if len(editsWire) > 48<<20 {
		return errors.New("recorded file edits exceed 48 MiB")
	}
	decoder := json.NewDecoder(strings.NewReader(string(editsWire)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&recorded); err != nil {
		return err
	}
	if recorded.Version != 1 {
		return errors.New("unsupported recorded file edits")
	}
	if len(recorded.Edits) == 0 && response.GetLearningDecision() != "updated" {
		decision := &contracts.ReflectionDecision{LearningDecision: response.GetLearningDecision(), NoChangeReason: response.GetNoChangeReason(), PlaybookIds: response.GetProposedArtifactIds()}
		if err := validate.Err(validate.ReflectionDiagnostics(decision, nil)); err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]string{"status": response.GetLearningDecision()})
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
	if err := applyEdits(proposal.Path, recorded.Edits); err != nil {
		return err
	}
	paths, err := repository.ChangedPaths(ctx, incidentID, broker.ReflectionPhase)
	if err != nil {
		return err
	}
	decision := &contracts.ReflectionDecision{LearningDecision: response.GetLearningDecision(), NoChangeReason: response.GetNoChangeReason(), PlaybookIds: response.GetProposedArtifactIds(), SdkGaps: response.GetSdkGaps()}
	if err := validate.Err(validate.ReflectionDiagnostics(decision, paths)); err != nil {
		return json.NewEncoder(os.Stdout).Encode(map[string]string{"status": "rejected", "error": err.Error()})
	}
	if len(paths) == 0 {
		return json.NewEncoder(os.Stdout).Encode(map[string]string{"status": response.GetLearningDecision()})
	}
	captureDir, err := memory.IncidentCorpusDir(root, incidentID)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(captureDir, 0700); err != nil {
		return err
	}
	captureRoot := filepath.Join(*checkpoint, "captures")
	err = filepath.WalkDir(captureRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(captureRoot, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink capture %q", relative)
		}
		if entry.IsDir() {
			if relative == "." || relative == "healthy" {
				return nil
			}
			return fmt.Errorf("unexpected capture directory %q", relative)
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("non-regular capture %q", relative)
		}
		if strings.Contains(relative, "/") && relative != "healthy/healthy.json" && relative != "healthy/reference" {
			return fmt.Errorf("unexpected nested capture %q", relative)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		destination := filepath.Join(captureDir, filepath.FromSlash(relative))
		if err := os.MkdirAll(filepath.Dir(destination), 0700); err != nil {
			return err
		}
		return os.WriteFile(destination, data, 0600)
	})
	if err != nil {
		return err
	}

	receipt, err := repository.PublishReflection(validate.WithCaptureSource(ctx, captureDir, incidentID), incidentID, &contracts.ReflectionDecision{LearningDecision: response.GetLearningDecision(), NoChangeReason: response.GetNoChangeReason(), PlaybookIds: response.GetProposedArtifactIds(), SdkGaps: response.GetSdkGaps()})
	if err != nil {
		return json.NewEncoder(os.Stdout).Encode(map[string]string{"status": "rejected", "error": fmt.Sprintf("validate recorded result: %v", err)})
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

type recordedEdit struct {
	Path          string  `json:"path"`
	ContentBase64 *string `json:"content_base64"`
	Mode          uint32  `json:"mode"`
}
type recordedEdits struct {
	Version int            `json:"version"`
	Edits   []recordedEdit `json:"edits"`
}

func applyEdits(root string, edits []recordedEdit) error {
	seen := map[string]bool{}
	for _, edit := range edits {
		if edit.Path == "" || filepath.IsAbs(edit.Path) || filepath.ToSlash(filepath.Clean(edit.Path)) != edit.Path || strings.Contains(edit.Path, "\\") || edit.Path == "." {
			return fmt.Errorf("invalid recorded file path %q", edit.Path)
		}
		for _, part := range strings.Split(edit.Path, "/") {
			if part == ".." || part == ".git" {
				return fmt.Errorf("invalid recorded file path %q", edit.Path)
			}
		}
		if seen[edit.Path] {
			return fmt.Errorf("duplicate recorded file path %q", edit.Path)
		}
		seen[edit.Path] = true
		if edit.Mode != 0644 && edit.Mode != 0755 {
			return errors.New("invalid recorded file mode")
		}
		target := filepath.Join(root, filepath.FromSlash(edit.Path))
		for path := target; path != root; path = filepath.Dir(path) {
			if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf("recorded path traverses symlink %q", edit.Path)
			} else if err != nil && !os.IsNotExist(err) {
				return err
			}
		}
		if edit.ContentBase64 == nil {
			if err := os.Remove(target); err != nil && !os.IsNotExist(err) {
				return err
			}
			continue
		}
		content, err := io.ReadAll(base64.NewDecoder(base64.StdEncoding.Strict(), strings.NewReader(*edit.ContentBase64)))
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
			return err
		}
		if err := os.WriteFile(target, content, os.FileMode(edit.Mode)); err != nil {
			return err
		}
		if err := os.Chmod(target, os.FileMode(edit.Mode)); err != nil {
			return err
		}
	}
	return nil
}

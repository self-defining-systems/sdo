// Package broker owns the only path from agent proposals to accepted Git state.
// Agent clones have independent Git metadata and no publishing remote.
package broker

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/memory/validate"
)

// Actor and Phase are defined by the validator, which owns the ownership
// policy over them.
type (
	Actor = validate.Actor
	Phase = validate.Phase
)

const (
	Human       = validate.Human
	Deployer    = validate.Deployer
	HealthJudge = validate.HealthJudge
	Responder   = validate.Responder
	Upkeep      = validate.Upkeep

	ProposalPhase   = validate.ProposalPhase
	ReflectionPhase = validate.ReflectionPhase
	InitialPhase    = validate.InitialPhase
	OutcomePhase    = validate.OutcomePhase
)

// Validator validates a complete candidate tree, including manifests and
// detector compilation. The broker invokes it only in its private clone.
type Validator func(context.Context, string, []string) error

type Proposal struct {
	IncidentID string
	Path       string
	BaseCommit string
}

type Receipt struct {
	IncidentID   string
	Phase        Phase
	Commit       string
	ChangedPaths []string
}

// SourceEvidence is produced by a trusted deployment strategy, never by an
// agent. Its ledger record is staged before source acceptance and reconciled
// against the attributed Git commit after an interrupted publish.
type SourceEvidence struct {
	ProposalTreeDigest string   `json:"proposal_tree_digest"`
	ObservedRevision   string   `json:"observed_revision"`
	StrategyID         string   `json:"strategy_id"`
	Images             []string `json:"images"`
	ResourceIDs        []string `json:"resource_ids"`
	Ready              bool     `json:"ready"`
	AcceptedCommit     string   `json:"accepted_commit,omitempty"`
}

type Broker struct {
	target        string
	workspaceRoot string
	validator     Validator
}

// New requires the workspace root to be outside the accepted repository.
// The worker must receive only Proposal.Path, never target or workspaceRoot.
func New(target, workspaceRoot string, validator Validator) (*Broker, error) {
	if target == "" || workspaceRoot == "" {
		return nil, errors.New("target and workspace root are required")
	}
	absoluteTarget, err := filepath.Abs(target)
	if err != nil {
		return nil, err
	}
	absoluteTarget, err = filepath.EvalSymlinks(absoluteTarget)
	if err != nil {
		return nil, fmt.Errorf("accepted repository path: %w", err)
	}
	absoluteWorkspaces, err := filepath.Abs(workspaceRoot)
	if err != nil {
		return nil, err
	}
	if within(absoluteTarget, absoluteWorkspaces) || within(absoluteWorkspaces, absoluteTarget) {
		return nil, errors.New("workspace and accepted repository must be separate")
	}
	if err := os.MkdirAll(absoluteWorkspaces, 0o700); err != nil {
		return nil, err
	}
	absoluteWorkspaces, err = filepath.EvalSymlinks(absoluteWorkspaces)
	if err != nil {
		return nil, err
	}
	if within(absoluteTarget, absoluteWorkspaces) || within(absoluteWorkspaces, absoluteTarget) {
		return nil, errors.New("workspace and accepted repository must be separate")
	}
	repositoryRoot, err := gitText(context.Background(), absoluteTarget, "rev-parse", "--show-toplevel")
	if err != nil {
		return nil, fmt.Errorf("accepted repository: %w", err)
	}
	if repositoryRoot != absoluteTarget {
		return nil, errors.New("accepted repository path must be its Git root")
	}
	return &Broker{target: absoluteTarget, workspaceRoot: absoluteWorkspaces, validator: validator}, nil
}

func (b *Broker) CreateProposal(ctx context.Context, incidentID string) (Proposal, error) {
	return b.CreateProposalForPhase(ctx, incidentID, ProposalPhase)
}

// CreateProposalForPhase starts a fresh isolated checkout for each lifecycle
// phase. Reflection therefore starts from the accepted outcome commit.
func (b *Broker) CreateProposalForPhase(ctx context.Context, incidentID string, phase Phase) (Proposal, error) {
	if strings.TrimSpace(incidentID) == "" {
		return Proposal{}, errors.New("incident ID is required")
	}
	if phase != ProposalPhase && phase != ReflectionPhase && phase != InitialPhase {
		return Proposal{}, fmt.Errorf("invalid proposal phase %q", phase)
	}
	var proposal Proposal
	err := b.locked(func() error {
		var err error
		proposal, err = b.proposal(incidentID, phase)
		if err == nil {
			return nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := b.requireClean(ctx); err != nil {
			return err
		}
		base, err := gitText(ctx, b.target, "rev-parse", "HEAD")
		if err != nil {
			return err
		}
		parent := b.proposalRoot(incidentID, phase)
		if err := os.RemoveAll(parent); err != nil {
			return fmt.Errorf("remove incomplete proposal: %w", err)
		}
		if err := os.MkdirAll(parent, 0o700); err != nil {
			return err
		}
		checkout := filepath.Join(parent, "checkout")
		if _, err := runGit(ctx, "", "clone", "--no-local", "--no-hardlinks", "--quiet", "--no-checkout", b.target, checkout); err != nil {
			return err
		}
		if _, err := runGit(ctx, checkout, "checkout", "--quiet", "--detach", base); err != nil {
			return err
		}
		if _, err := runGit(ctx, checkout, "remote", "remove", "origin"); err != nil {
			return err
		}
		proposal = Proposal{IncidentID: incidentID, Path: checkout, BaseCommit: base}
		if err := validate.WritePublicationContext(ctx, checkout, validate.PublicationContext{BaseCommit: base, IncidentID: incidentID}); err != nil {
			return err
		}
		data, err := json.Marshal(proposal)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(parent, "proposal.json"), data, 0o600)
	})
	return proposal, err
}

func (b *Broker) Publish(ctx context.Context, incidentID string, actor Actor, phase Phase) (Receipt, error) {
	return b.publish(ctx, incidentID, actor, phase, "", nil, nil)
}

// PublishVerifiedSource accepts an initial source proposal only when its full
// Git tree still matches the tree deployed and observed by the trusted adapter.
// The check and accepted commit occur under the broker's publishing lock.
func (b *Broker) PublishVerifiedSource(ctx context.Context, incidentID string, evidence SourceEvidence) (Receipt, error) {
	if err := validateSourceEvidence(evidence); err != nil {
		return Receipt{}, err
	}
	return b.publish(ctx, incidentID, Deployer, InitialPhase, evidence.ProposalTreeDigest, &evidence, nil)
}

func (b *Broker) publish(ctx context.Context, incidentID string, actor Actor, phase Phase, expectedSourceTree string, evidence *SourceEvidence, decision *contracts.ReflectionDecision) (Receipt, error) {
	if err := validate.Err(validate.PhaseDiagnostics(phase)); err != nil {
		return Receipt{}, err
	}
	if phase == OutcomePhase {
		return Receipt{}, errors.New("outcomes must use RecordOutcome")
	}
	var receipt Receipt
	err := b.locked(func() error {
		if prior, ok, err := b.findAccepted(ctx, incidentID, phase); err != nil {
			return err
		} else if ok {
			if expectedSourceTree != "" {
				stored, found, err := b.sourceEvidenceLocked(ctx, incidentID, prior)
				if err != nil {
					return err
				}
				if !found {
					return errors.New("accepted source has no trusted deployment evidence")
				}
				claimed := *evidence
				claimed.AcceptedCommit = prior.Commit
				if !reflect.DeepEqual(stored, claimed) {
					return errors.New("accepted source evidence differs from verified deployment")
				}
			}
			receipt = prior
			return nil
		}
		if err := b.requireClean(ctx); err != nil {
			return err
		}
		proposal, err := b.proposal(incidentID, phase)
		if err != nil {
			return err
		}
		current, err := gitText(ctx, b.target, "rev-parse", "HEAD")
		if err != nil {
			return err
		}
		if current != proposal.BaseCommit {
			return errors.New("proposal base is stale; re-create the proposal from accepted state")
		}
		paths, err := validate.ChangedPaths(ctx, proposal.Path, proposal.BaseCommit)
		if err != nil {
			return err
		}
		publication := &validate.PublicationContext{BaseCommit: proposal.BaseCommit, IncidentID: incidentID, VerifiedSourceTree: expectedSourceTree}
		if phase == ReflectionPhase && decision == nil {
			decision, err = validate.LoadReflectionDecision(ctx, proposal.Path, "")
			if err != nil {
				return err
			}
		}
		// Validate before copying so invalid filesystem shapes also receive the
		// shared diagnostics. Recheck the frozen private clone with the real runner.
		preflight, err := checkCandidate(ctx, proposal.Path, proposal.BaseCommit, actor, phase, paths, func(context.Context, string, []string) error { return nil }, publication, decision)
		if err != nil {
			return err
		}
		if err := validate.Err(preflight); err != nil {
			return err
		}
		if len(paths) == 0 {
			receipt = Receipt{IncidentID: incidentID, Phase: phase}
			return nil
		}
		if expectedSourceTree != "" {
			actualTree, err := ProposalTreeDigest(ctx, proposal.Path)
			if err != nil {
				return err
			}
			if actualTree != expectedSourceTree {
				return errors.New("source proposal tree changed after deployment verification")
			}
			if err := b.writeSourceEvidence(incidentID, *evidence); err != nil {
				return err
			}
		}
		if b.validator == nil {
			return errors.New("proposal validation is not configured")
		}
		stage, cleanup, err := b.privateClone(ctx)
		if err != nil {
			return err
		}
		defer cleanup()
		for _, path := range paths {
			if err := copyChange(proposal.Path, stage, path); err != nil {
				return err
			}
		}
		diagnostics, err := checkCandidate(ctx, stage, proposal.BaseCommit, actor, phase, paths, b.validator, publication, decision)
		if err != nil {
			return fmt.Errorf("proposal validation: %w", err)
		}
		if err := validate.Err(diagnostics); err != nil {
			return fmt.Errorf("candidate validation: %w", err)
		}
		if expectedSourceTree != "" {
			stagedTree, err := ProposalTreeDigest(ctx, stage)
			if err != nil {
				return err
			}
			if stagedTree != expectedSourceTree {
				return errors.New("staged source tree differs from verified deployment")
			}
		}
		// The shared check may install generic health registrations. Publish
		// every checked file, including those explicit Go bootstrap outputs.
		paths, err = validate.ChangedPaths(ctx, stage, proposal.BaseCommit)
		if err != nil {
			return err
		}
		receipt, err = b.accept(ctx, stage, incidentID, actor, phase, paths)
		if err != nil {
			return err
		}
		if evidence != nil {
			accepted := *evidence
			accepted.AcceptedCommit = receipt.Commit
			if err := b.writeSourceEvidence(incidentID, accepted); err != nil {
				return fmt.Errorf("source accepted but evidence finalization needs reconciliation: %w", err)
			}
		}
		return nil
	})
	return receipt, err
}

// ChangedPaths reports the actual proposal tree diff, including local agent
// commits, staged changes, and untracked files. It never trusts worker claims.
func (b *Broker) ChangedPaths(ctx context.Context, incidentID string, phase Phase) ([]string, error) {
	proposal, err := b.proposal(incidentID, phase)
	if err != nil {
		return nil, err
	}
	return validate.ChangedPaths(ctx, proposal.Path, proposal.BaseCommit)
}

// ProposalInfo returns broker-owned identity for a prepared checkout.
func (b *Broker) ProposalInfo(incidentID string, phase Phase) (Proposal, error) {
	return b.proposal(incidentID, phase)
}

// AcceptedCommit finds a broker-authored commit after a process restart.
func (b *Broker) AcceptedCommit(ctx context.Context, incidentID string, phase Phase) (Receipt, bool, error) {
	return b.findAccepted(ctx, incidentID, phase)
}

// DiagnosticsFingerprint is the Git tree identity of the accepted detector
// package at a specific commit. It supports incident-correlated rollouts.
func (b *Broker) DiagnosticsFingerprint(ctx context.Context, commit string) (string, error) {
	if commit == "" {
		return "", errors.New("commit is required")
	}
	return gitText(ctx, b.target, "rev-parse", commit+":"+layout.DiagnosticsDir)
}

// ReleaseProposal removes a proposal checkout after acknowledgement or a
// superseded incident. Accepted commits remain in the operational repository.
func (b *Broker) ReleaseProposal(incidentID string, phase Phase) error {
	if strings.TrimSpace(incidentID) == "" {
		return errors.New("incident ID is required")
	}
	return b.locked(func() error { return os.RemoveAll(b.proposalRoot(incidentID, phase)) })
}

// RecordOutcome writes an authoritative immutable outcome in a private broker
// clone and commits it. Agents cannot supply an outcome through Publish.
func (b *Broker) RecordOutcome(ctx context.Context, incidentID string, record *contracts.OutcomeRecord) (Receipt, error) {
	var receipt Receipt
	err := b.locked(func() error {
		if err := b.requireClean(ctx); err != nil {
			return err
		}
		stage, cleanup, err := b.privateClone(ctx)
		if err != nil {
			return err
		}
		defer cleanup()
		store := memory.NewJSONStore(stage)
		if err := store.ImportLegacy(ctx); err != nil {
			return err
		}
		outcome, err := store.Put(ctx, record)
		if err != nil {
			return err
		}
		if outcome.IncidentID != incidentID {
			return errors.New("outcome incident ID does not match broker request")
		}
		base, err := gitText(ctx, stage, "rev-parse", "HEAD")
		if err != nil {
			return err
		}
		paths, err := validate.ChangedPaths(ctx, stage, base)
		if err != nil {
			return err
		}
		if len(paths) == 0 {
			if prior, ok, err := b.findAccepted(ctx, incidentID, OutcomePhase); err != nil {
				return err
			} else if ok {
				receipt = prior
				return nil
			}
			return errors.New("outcome already exists without an attributed broker commit")
		}
		receipt, err = b.accept(ctx, stage, incidentID, "controller", OutcomePhase, paths)
		return err
	})
	return receipt, err
}

func (b *Broker) proposal(incidentID string, phase Phase) (Proposal, error) {
	path := filepath.Join(b.proposalRoot(incidentID, phase), "proposal.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return Proposal{}, err
	}
	var proposal Proposal
	if err := json.Unmarshal(data, &proposal); err != nil {
		return Proposal{}, fmt.Errorf("proposal metadata: %w", err)
	}
	if proposal.IncidentID != incidentID || proposal.Path != filepath.Join(b.proposalRoot(incidentID, phase), "checkout") || proposal.BaseCommit == "" {
		return Proposal{}, errors.New("invalid proposal metadata")
	}
	return proposal, nil
}

func (b *Broker) proposalRoot(incidentID string, phase Phase) string {
	sum := sha256.Sum256([]byte(incidentID + "\x00" + string(phase)))
	return filepath.Join(b.workspaceRoot, "proposals", fmt.Sprintf("%x", sum))
}

func (b *Broker) privateClone(ctx context.Context) (string, func(), error) {
	parent := filepath.Join(b.workspaceRoot, "staging")
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return "", nil, err
	}
	dir, err := os.MkdirTemp(parent, "broker-")
	if err != nil {
		return "", nil, err
	}
	cleanup := func() { os.RemoveAll(dir) }
	stage := filepath.Join(dir, "checkout")
	if _, err := runGit(ctx, "", "clone", "--no-local", "--no-hardlinks", "--quiet", b.target, stage); err != nil {
		cleanup()
		return "", nil, err
	}
	if _, err := runGit(ctx, stage, "remote", "remove", "origin"); err != nil {
		cleanup()
		return "", nil, err
	}
	return stage, cleanup, nil
}

func (b *Broker) requireClean(ctx context.Context) error {
	status, err := gitText(ctx, b.target, "status", "--porcelain")
	if err != nil {
		return err
	}
	if status != "" {
		return errors.New("accepted repository must be clean")
	}
	return nil
}

func (b *Broker) locked(fn func() error) error {
	if err := os.MkdirAll(b.workspaceRoot, 0o700); err != nil {
		return err
	}
	file, err := os.OpenFile(filepath.Join(b.workspaceRoot, "broker.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	return fn()
}

func (b *Broker) accept(ctx context.Context, stage, incidentID string, actor Actor, phase Phase, paths []string) (Receipt, error) {
	if _, err := runGit(ctx, stage, "add", "--all", "--", "."); err != nil {
		return Receipt{}, err
	}
	message := fmt.Sprintf("sdo(%s): validated %s\n\nSDO-Incident: %s\nSDO-Actor: %s\nSDO-Phase: %s\nSDO-Validation: passed", incidentID, phase, incidentID, actor, phase)
	if _, err := runGitEnv(ctx, stage, brokerGitEnv(), "commit", "--quiet", "-m", message); err != nil {
		return Receipt{}, err
	}
	commit, err := gitText(ctx, stage, "rev-parse", "HEAD")
	if err != nil {
		return Receipt{}, err
	}
	if _, err := runGit(ctx, b.target, "fetch", "--no-tags", "--quiet", stage, commit); err != nil {
		return Receipt{}, err
	}
	if _, err := runGit(ctx, b.target, "merge", "--ff-only", "--quiet", commit); err != nil {
		return Receipt{}, fmt.Errorf("accepted branch changed during publish: %w", err)
	}
	return Receipt{IncidentID: incidentID, Phase: phase, Commit: commit, ChangedPaths: paths}, nil
}

func (b *Broker) findAccepted(ctx context.Context, incidentID string, phase Phase) (Receipt, bool, error) {
	data, err := runGit(ctx, b.target, "log", "--format=%H%x1f%B%x1e")
	if err != nil {
		return Receipt{}, false, err
	}
	for _, item := range strings.Split(string(data), "\x1e") {
		parts := strings.SplitN(item, "\x1f", 2)
		if len(parts) != 2 {
			continue
		}
		message := "\n" + parts[1] + "\n"
		if strings.Contains(message, "\nSDO-Incident: "+incidentID+"\n") && strings.Contains(message, "\nSDO-Phase: "+string(phase)+"\n") {
			commit := strings.TrimSpace(parts[0])
			changed, err := runGit(ctx, b.target, "diff-tree", "--root", "--no-commit-id", "--name-only", "-r", "-z", commit)
			if err != nil {
				return Receipt{}, false, err
			}
			paths := make([]string, 0)
			for _, path := range strings.Split(string(changed), "\x00") {
				if path != "" {
					paths = append(paths, path)
				}
			}
			return Receipt{IncidentID: incidentID, Phase: phase, Commit: commit, ChangedPaths: paths}, true, nil
		}
	}
	return Receipt{}, false, nil
}

func brokerGitEnv() []string {
	return []string{"GIT_AUTHOR_NAME=SDO Commit Broker", "GIT_AUTHOR_EMAIL=sdo-commit-broker@localhost", "GIT_COMMITTER_NAME=SDO Commit Broker", "GIT_COMMITTER_EMAIL=sdo-commit-broker@localhost"}
}

func runGit(ctx context.Context, cwd string, args ...string) ([]byte, error) {
	return runGitEnv(ctx, cwd, nil, args...)
}

func runGitEnv(ctx context.Context, cwd string, env []string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	if cwd != "" {
		cmd.Dir = cwd
	}
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return out, nil
}

func gitText(ctx context.Context, cwd string, args ...string) (string, error) {
	out, err := runGit(ctx, cwd, args...)
	return strings.TrimSpace(string(out)), err
}

// ProposalTreeDigest computes the complete candidate Git tree without changing
// the checkout's index. Both deployment and publish use this exact convention.
func ProposalTreeDigest(ctx context.Context, checkout string) (string, error) {
	root, err := filepath.Abs(checkout)
	if err != nil {
		return "", err
	}
	indexDir, err := os.MkdirTemp("", "sdo-candidate-index-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(indexDir)
	index := filepath.Join(indexDir, "index")
	run := func(args ...string) (string, error) {
		output, err := runGitEnv(ctx, root, []string{"GIT_INDEX_FILE=" + index}, args...)
		return strings.TrimSpace(string(output)), err
	}
	if _, err := run("read-tree", "HEAD"); err != nil {
		return "", err
	}
	if _, err := run("add", "-A"); err != nil {
		return "", err
	}
	tree, err := run("write-tree")
	if err != nil {
		return "", err
	}
	if len(tree) != 40 && len(tree) != 64 {
		return "", errors.New("candidate Git tree has invalid identity")
	}
	return tree, nil
}

func copyChange(sourceRoot, targetRoot, relative string) error {
	source := filepath.Join(sourceRoot, relative)
	target := filepath.Join(targetRoot, relative)
	if err := safeParents(sourceRoot, source); err != nil {
		return err
	}
	if err := safeParents(targetRoot, target); err != nil {
		return err
	}
	info, err := os.Lstat(source)
	if os.IsNotExist(err) {
		if err := os.Remove(target); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("proposal path must be a regular file: %s", relative)
	}
	data, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	if err := os.Remove(target); err != nil && !os.IsNotExist(err) {
		return err
	}
	return os.WriteFile(target, data, info.Mode().Perm())
}

func safeParents(root, path string) error {
	parent := filepath.Dir(path)
	for current := parent; current != root; current = filepath.Dir(current) {
		if !within(root, current) {
			return errors.New("proposal path escaped checkout")
		}
		info, err := os.Lstat(current)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return fmt.Errorf("non-directory in proposal path: %s", current)
		}
	}
	return nil
}

func within(root, path string) bool {
	relative, err := filepath.Rel(root, path)
	return err == nil && (relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(os.PathSeparator))))
}

package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/validate"
)

const MemoryConflictRule = "memory.conflict"

// MemoryConflict rejects a replay without changing accepted state. The wire
// diagnostic carries accepted provenance and independent validator errors.
type MemoryConflict struct {
	Conflict *contracts.MemoryConflict
	cause    error
}

func (e *MemoryConflict) Error() string {
	parts := []string{"memory conflict at accepted commit " + e.Conflict.GetAcceptedCommit(), "paths: " + strings.Join(e.Conflict.GetConflictingPaths(), ", ")}
	for _, change := range e.Conflict.GetAcceptedChanges() {
		parts = append(parts, fmt.Sprintf("accepted %s incident %s changed %s", change.GetCommit(), change.GetIncidentId(), strings.Join(change.GetPaths(), ", ")))
	}
	for _, diagnostic := range e.Conflict.GetValidatorErrors() {
		parts = append(parts, fmt.Sprintf("[%s] %s", diagnostic.GetRule(), diagnostic.GetMessage()))
	}
	return strings.Join(parts, "; ")
}
func (e *MemoryConflict) Unwrap() error { return e.cause }

// NonRetryable is false even when the cause is a validation failure: a newer
// accepted head made the merged tree fail, and the proposer can merge both
// incidents' intent and resubmit.
func (e *MemoryConflict) NonRetryable() bool { return false }

func (e *MemoryConflict) Diagnostic() *contracts.MemoryDiagnostic {
	return &contracts.MemoryDiagnostic{Rule: MemoryConflictRule, Message: e.Error(), Hint: "Merge both incidents' intent and preserve the other incident's accepted learning in the current accepted checkout.", MemoryConflict: e.Conflict}
}

// proposalPatch freezes only the authorized proposal diff relative to its
// original base. Agent commits, staged/untracked files, modes, binaries and
// deletions are all captured by Git. The original proposal is never mutated.
func (b *Broker) proposalPatch(ctx context.Context, proposal Proposal, candidate string, paths []string) (string, func(), error) {
	frozen, cleanup, err := b.privateClone(ctx)
	if err != nil {
		return "", nil, err
	}
	patch, err := func() ([]byte, error) {
		if _, err := runGit(ctx, frozen, "checkout", "--quiet", "--detach", proposal.BaseCommit); err != nil {
			return nil, err
		}
		for _, path := range paths {
			if err := copyChange(candidate, frozen, path); err != nil {
				return nil, err
			}
		}
		if err := addIgnoredOperationalMemory(ctx, frozen, nil); err != nil {
			return nil, err
		}
		if _, err := runGit(ctx, frozen, "add", "--all", "--", "."); err != nil {
			return nil, err
		}
		return runGit(ctx, frozen, "diff", "--cached", "--binary", "--full-index", proposal.BaseCommit)
	}()
	if err != nil || len(patch) == 0 {
		cleanup()
		return "", nil, err
	}
	patchFile := filepath.Join(filepath.Dir(frozen), "proposal.patch")
	if err := os.WriteFile(patchFile, patch, 0600); err != nil {
		cleanup()
		return "", nil, err
	}
	return patchFile, cleanup, nil
}

// replayCandidate three-way applies the frozen proposal diff onto the current
// accepted checkout.
func (b *Broker) replayCandidate(ctx context.Context, proposal Proposal, candidate, stage, current string, paths []string) error {
	if _, err := runGit(ctx, stage, "merge-base", "--is-ancestor", proposal.BaseCommit, current); err != nil {
		return fmt.Errorf("proposal base is not an ancestor of accepted state: %w", err)
	}
	patchFile, cleanup, err := b.proposalPatch(ctx, proposal, candidate, paths)
	if err != nil || patchFile == "" {
		return err
	}
	defer cleanup()
	if _, err := runGit(ctx, stage, "apply", "--3way", "--index", "--", patchFile); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		raw, listErr := runGit(ctx, stage, "diff", "--name-only", "--diff-filter=U", "-z")
		if listErr != nil {
			return listErr
		}
		conflicts := nulPaths(raw)
		if len(conflicts) == 0 {
			conflicts = slices.Clone(paths)
		}
		return b.memoryConflict(ctx, proposal.BaseCommit, current, conflicts, nil)
	}
	return nil
}

// RebaseProposal carries a rejected proposal onto the current accepted head
// in place, so a correction starts from its own work rather than a clean
// checkout. Hunks that merge cleanly are kept; colliding hunks are left as
// conflict markers that the shared check rejects until they are resolved.
// The checkout path is stable. A missing proposal is not an error: the next
// inspection starts a fresh one.
func (b *Broker) RebaseProposal(ctx context.Context, incidentID string, phase Phase) (Proposal, error) {
	if err := checkIncidentID(incidentID); err != nil {
		return Proposal{}, err
	}
	var rebased Proposal
	err := b.locked(func() error {
		root := b.proposalRoot(incidentID, phase)
		checkout, rejected, next := filepath.Join(root, "checkout"), filepath.Join(root, "checkout.rejected"), filepath.Join(root, "checkout.rebase")
		// Finish an interrupted swap: metadata is written last, so the rejected
		// checkout still matches it until then.
		if _, err := os.Stat(checkout); os.IsNotExist(err) {
			if _, err := os.Stat(rejected); err == nil {
				if err := os.Rename(rejected, checkout); err != nil {
					return err
				}
			}
		}
		if err := os.RemoveAll(next); err != nil {
			return err
		}
		if err := os.RemoveAll(rejected); err != nil {
			return err
		}
		proposal, err := b.proposal(incidentID, phase)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		current, err := gitText(ctx, b.target, "rev-parse", "HEAD")
		if err != nil {
			return err
		}
		rebased = proposal
		if current == proposal.BaseCommit {
			return nil
		}
		if _, err := runGit(ctx, b.target, "merge-base", "--is-ancestor", proposal.BaseCommit, current); err != nil {
			return fmt.Errorf("proposal base is not an ancestor of accepted state: %w", err)
		}
		paths, err := validate.ChangedPaths(ctx, proposal.Path, proposal.BaseCommit)
		if err != nil {
			return err
		}
		patchFile, cleanup, err := b.proposalPatch(ctx, proposal, proposal.Path, paths)
		if err != nil {
			return err
		}
		if cleanup != nil {
			defer cleanup()
		}
		if err := cloneGit(ctx, b.target, next, "--no-checkout"); err != nil {
			return err
		}
		if _, err := runGit(ctx, next, "checkout", "--quiet", "--detach", current); err != nil {
			return err
		}
		if _, err := runGit(ctx, next, "remote", "remove", "origin"); err != nil {
			return err
		}
		if patchFile != "" {
			// A failed three-way apply leaves conflict markers in the colliding
			// files and applies every other hunk; the index then returns to the
			// accepted head so the checkout is an ordinary edited worktree.
			if _, err := runGit(ctx, next, "-c", "merge.conflictStyle=diff3", "apply", "--3way", "--index", "--", patchFile); err != nil && ctx.Err() != nil {
				return ctx.Err()
			}
			if _, err := runGit(ctx, next, "reset", "--quiet"); err != nil {
				return err
			}
		}
		// The agent's intended decision stays with its kept edits.
		if decision, err := os.ReadFile(filepath.Join(proposal.Path, ".git", validate.DecisionFile)); err == nil {
			if err := os.WriteFile(filepath.Join(next, ".git", validate.DecisionFile), decision, 0o600); err != nil {
				return err
			}
		} else if !os.IsNotExist(err) {
			return err
		}
		rebased = Proposal{IncidentID: incidentID, Path: checkout, BaseCommit: current}
		if err := validate.WritePublicationContext(ctx, next, validate.PublicationContext{BaseCommit: current, IncidentID: incidentID, ObservationCeiling: b.observationCeiling, ApplicationNamespace: b.applicationNamespace}); err != nil {
			return err
		}
		data, err := json.Marshal(rebased)
		if err != nil {
			return err
		}
		if err := os.Rename(checkout, rejected); err != nil {
			return err
		}
		if err := os.Rename(next, checkout); err != nil {
			return err
		}
		metadata := filepath.Join(root, "proposal.json")
		if err := os.WriteFile(metadata+".tmp", data, 0o600); err != nil {
			return err
		}
		if err := os.Rename(metadata+".tmp", metadata); err != nil {
			return err
		}
		return os.RemoveAll(rejected)
	})
	return rebased, err
}

func nulPaths(raw []byte) []string {
	var paths []string
	for _, path := range strings.Split(string(raw), "\x00") {
		if path != "" {
			paths = append(paths, path)
		}
	}
	return paths
}

func (b *Broker) memoryConflict(ctx context.Context, base, current string, paths []string, cause error) error {
	conflict := &contracts.MemoryConflict{BaseCommit: base, AcceptedCommit: current, ConflictingPaths: slices.Clone(paths)}
	var failure *validate.Failure
	if errors.As(cause, &failure) {
		conflict.ValidatorErrors = validate.ProtoDiagnostics(failure.Diagnostics)
	}
	commits, err := gitText(ctx, b.target, "rev-list", "--reverse", base+".."+current)
	if err != nil {
		return err
	}
	for _, commit := range strings.Fields(commits) {
		raw, err := runGit(ctx, b.target, "diff-tree", "--no-commit-id", "--name-only", "-r", "-z", commit)
		if err != nil {
			return err
		}
		changed := nulPaths(raw)
		// For a semantic conflict a changed dependency may be outside the proposal
		// paths. Retain every intervening publication for that diagnostic.
		if cause == nil {
			changed = slices.DeleteFunc(changed, func(path string) bool { return !slices.Contains(paths, path) })
		}
		if len(changed) == 0 {
			continue
		}
		message, err := gitText(ctx, b.target, "show", "-s", "--format=%B", commit)
		if err != nil {
			return err
		}
		incident := ""
		for _, line := range strings.Split(message, "\n") {
			if strings.HasPrefix(line, "SDO-Incident: ") {
				incident = strings.TrimPrefix(line, "SDO-Incident: ")
			}
		}
		conflict.AcceptedChanges = append(conflict.AcceptedChanges, &contracts.AcceptedMemoryChange{Commit: commit, IncidentId: incident, Paths: changed})
	}
	return &MemoryConflict{Conflict: conflict, cause: cause}
}

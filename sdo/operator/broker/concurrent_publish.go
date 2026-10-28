package broker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/layout"
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

// replayCandidate freezes only the authorized proposal diff relative to its
// original base, then three-way applies it onto the current accepted checkout.
// Agent commits, staged/untracked files, modes, binaries and deletions are all
// captured by Git. The generated playbook index is left out of the patch and
// regenerated on the merged tree. The original proposal is never mutated by
// the broker.
func (b *Broker) replayCandidate(ctx context.Context, proposal Proposal, candidate, stage, current string, paths []string) error {
	if _, err := runGit(ctx, stage, "merge-base", "--is-ancestor", proposal.BaseCommit, current); err != nil {
		return fmt.Errorf("proposal base is not an ancestor of accepted state: %w", err)
	}
	frozen, cleanup, err := b.privateClone(ctx)
	if err != nil {
		return err
	}
	defer cleanup()
	if _, err := runGit(ctx, frozen, "checkout", "--quiet", "--detach", proposal.BaseCommit); err != nil {
		return err
	}
	for _, path := range paths {
		if path == layout.PlaybookIndexFile {
			continue
		}
		if err := copyChange(candidate, frozen, path); err != nil {
			return err
		}
	}
	if err := addIgnoredOperationalMemory(ctx, frozen, nil); err != nil {
		return err
	}
	if _, err := runGit(ctx, frozen, "add", "--all", "--", "."); err != nil {
		return err
	}
	patch, err := runGit(ctx, frozen, "diff", "--cached", "--binary", "--full-index", proposal.BaseCommit)
	if err != nil {
		return err
	}
	if len(patch) == 0 {
		return nil
	}
	patchFile := filepath.Join(filepath.Dir(frozen), "proposal.patch")
	if err := os.WriteFile(patchFile, patch, 0600); err != nil {
		return err
	}
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
	if slices.ContainsFunc(paths, func(path string) bool { return strings.HasPrefix(path, layout.PlaybooksDir+"/") }) {
		if _, err := layout.WritePlaybookIndex(stage); err != nil {
			return fmt.Errorf("regenerate playbook index: %w", err)
		}
	}
	return nil
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

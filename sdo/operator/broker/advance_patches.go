package broker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/patchset"
	"sdo.dev/operator/rendered"
)

// mirrorTrailer records, on an accepted advance commit, whether the accepted
// source outside .sdo/ is an exact mirror of the upstream revision it follows.
const mirrorTrailer = "SDO-Mirror"

// Mirror states recorded in the SDO-Mirror trailer.
const (
	// MirrorExact: before and after the advance, every path outside .sdo/ is
	// the upstream revision's. Local source differences live only in patches.
	MirrorExact = "exact"
	// MirrorLegacy: the accepted source already differed from its upstream
	// outside .sdo/ (edits baked in by an earlier schema); the advance merged
	// three-way and the invariant is not required.
	MirrorLegacy = "legacy"
)

// PatchHoldError reports that the candidate of an advance cannot produce an
// effective tree because the deployment patch series does not apply to it. It
// is a verdict on the upstream revision, not an infrastructure failure: the
// advance holds it before any cluster effect. Patch is also reachable with
// errors.As.
type PatchHoldError struct {
	// Candidate is the advance commit the series was applied to.
	Candidate string
	Patch     *patchset.PatchError
	// Owner is the actor the failing patch belongs to ("" when unreadable).
	Owner string
	// UpstreamCommits are the upstream commits (short ID and subject, newest
	// first, capped) that touched the patch's paths since the merge base.
	UpstreamCommits []string
}

func (e *PatchHoldError) Error() string {
	return fmt.Sprintf("deployment patches do not apply to advance candidate %s: %v", e.Candidate, e.Patch)
}

func (e *PatchHoldError) Unwrap() error { return e.Patch }

const maxHoldCommits = 5

// effectiveError turns a failure of rendered.Effective on tree (in the object
// store at repository) into a typed hold or a plain error to retry. A patch
// that is no longer valid, or a non-regular patch entry, is a hold.
func effectiveError(ctx context.Context, repository, candidate, tree, base, upstream string, err error) error {
	var patchErr *patchset.PatchError
	if !errors.As(err, &patchErr) {
		if !errors.Is(err, rendered.ErrSourceInvalid) {
			return fmt.Errorf("effective tree of advance candidate %s: %w", candidate, err)
		}
		patchErr = &patchset.PatchError{Kind: patchset.KindForbidden, Detail: err.Error()}
	}
	hold := &PatchHoldError{Candidate: candidate, Patch: patchErr}
	if patchErr.Patch != "" {
		hold.Owner = patchOwner(ctx, repository, tree, patchErr.Patch)
	}
	if len(patchErr.Paths) > 0 && base != "" && upstream != "" {
		args := append([]string{"log", "--no-color", fmt.Sprintf("--max-count=%d", maxHoldCommits), "--format=%h %s", base + ".." + upstream, "--"}, patchErr.Paths...)
		if out, logErr := gitText(ctx, repository, args...); logErr == nil && out != "" {
			hold.UpstreamCommits = strings.Split(out, "\n")
		}
	}
	return hold
}

// patchOwner reads the Owner header of the named patch in tree; "" when the
// file cannot be read or parsed (the hold still stands).
func patchOwner(ctx context.Context, repository, tree, name string) string {
	data, err := runGit(ctx, repository, "cat-file", "blob", tree+":"+patchset.Directory+"/"+name)
	if err != nil {
		return ""
	}
	patch, err := patchset.Parse(name, data)
	if err != nil {
		return ""
	}
	return string(patch.Header.Owner)
}

// checkEffective requires the patch series of tree to apply, the way every
// render, build and verification reads the source. Close happens before
// return: only the verdict is needed.
func checkEffective(ctx context.Context, repository, tree string) error {
	effective, err := rendered.Effective(ctx, rendered.ObjectStore{Dir: repository}, rendered.BaseTree(tree))
	if err != nil {
		return err
	}
	effective.Close()
	return nil
}

// nonMemoryTree is the tree of commit without .sdo/: the source as upstream
// would know it.
func nonMemoryTree(ctx context.Context, repository, commit string) (string, error) {
	dir, err := os.MkdirTemp("", "sdo-mirror-index-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(dir)
	env := []string{"GIT_INDEX_FILE=" + filepath.Join(dir, "index")}
	if _, err := runGitEnv(ctx, repository, env, "read-tree", commit); err != nil {
		return "", err
	}
	if _, err := runGitEnv(ctx, repository, env, "rm", "-r", "--cached", "--ignore-unmatch", "-q", "--", layout.Dir); err != nil {
		return "", err
	}
	out, err := runGitEnv(ctx, repository, env, "write-tree")
	return strings.TrimSpace(string(out)), err
}

// mirrorState decides, in the private clone stage after the merge, whether the
// advance keeps the mirror invariant. When the accepted HEAD mirrored base
// outside .sdo/ (no baked-in source edits), the merge must produce exactly the
// upstream revision's non-memory tree; anything else is a broker error. A
// repository whose source already differed is legacy: the three-way merge
// stands and the state is only recorded.
func mirrorState(ctx context.Context, stage, head, base, upstream string) (string, error) {
	mirrored, err := nonMemoryTree(ctx, stage, base)
	if err != nil {
		return "", err
	}
	accepted, err := nonMemoryTree(ctx, stage, head)
	if err != nil {
		return "", err
	}
	if mirrored != accepted {
		return MirrorLegacy, nil
	}
	want, err := nonMemoryTree(ctx, stage, upstream)
	if err != nil {
		return "", err
	}
	got, err := advanceSourceTreeDigest(ctx, stage)
	if err != nil {
		return "", err
	}
	if got != want {
		return "", fmt.Errorf("advance to upstream %s breaks the mirror invariant: merged source tree %s is not the upstream's %s", upstream, got, want)
	}
	return MirrorExact, nil
}

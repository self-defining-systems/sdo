package memory

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"sdo.dev/operator/memory/layout"
)

// AdoptionCommit identifies the latest explicit human adoption boundary.
// Its provenance is deliberately distinct from broker-validated publication.
func AdoptionCommit(ctx context.Context, repository, revision string) (string, error) {
	if repository == "" {
		return "", nil
	}
	if _, err := os.Stat(filepath.Join(repository, ".git")); errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if revision == "" {
		revision = "HEAD"
	}
	raw, err := exec.CommandContext(ctx, "git", "-C", repository, "log", revision, "--format=%H%x00%B%x00").Output()
	if err != nil {
		return "", err
	}
	entries := strings.Split(string(raw), "\x00")
	for i := 0; i+1 < len(entries); i += 2 {
		body := "\n" + entries[i+1] + "\n"
		if strings.Contains(body, "\nSDO-Origin: adopted-existing\n") && strings.Contains(body, "\nSDO-Validation: skipped\n") && strings.Contains(body, "\nSDO-Actor: human\n") {
			return strings.TrimSpace(entries[i]), nil
		}
	}
	return "", nil
}

// AdoptedBuild permits compilation without corpus/replay acceptance only while
// the entire accepted executable memory is identical to the adopted base.
func AdoptedBuild(ctx context.Context, repository, revision string) (bool, error) {
	adoption, err := AdoptionCommit(ctx, repository, revision)
	if err != nil || adoption == "" {
		return false, err
	}
	dirty, err := exec.CommandContext(ctx, "git", "-C", repository, "status", "--porcelain", "--", layout.DiagnosticsDir, layout.PlaybooksDir, layout.LibrariesDir, layout.CorpusDir).Output()
	if err != nil {
		return false, err
	}
	if len(dirty) != 0 {
		return false, nil
	}
	others, err := exec.CommandContext(ctx, "git", "-C", repository, "ls-files", "--others", "--", layout.DiagnosticsDir, layout.PlaybooksDir, layout.LibrariesDir, layout.CorpusDir).Output()
	if err != nil {
		return false, err
	}
	if len(others) != 0 {
		return false, nil
	}
	before, err := ControllerFingerprint(ctx, repository, adoption)
	if err != nil {
		return false, err
	}
	after, err := ControllerFingerprint(ctx, repository, revision)
	if err != nil {
		return false, err
	}
	return before == after, nil
}

// MarkAdoptedExisting commits only the supplied memory, without claiming any
// validation, outcome, or capture. Callers must compile it before this operation.
func MarkAdoptedExisting(ctx context.Context, repository string) (string, error) {
	git := func(args ...string) (string, error) {
		raw, err := exec.CommandContext(ctx, "git", append([]string{"-C", repository}, args...)...).CombinedOutput()
		if err != nil {
			return "", fmt.Errorf("adopt existing memory git: %w: %s", err, raw)
		}
		return strings.TrimSpace(string(raw)), nil
	}
	changed, err := git("status", "--porcelain", "--", ".", ":(exclude).sdo")
	if err != nil {
		return "", err
	}
	if changed != "" {
		return "", errors.New("adoption requires source outside .sdo to match HEAD")
	}
	prior, err := AdoptionCommit(ctx, repository, "HEAD")
	if err != nil {
		return "", err
	}
	if prior != "" {
		dirty, err := git("status", "--porcelain", "--", layout.Dir)
		if err != nil {
			return "", err
		}
		if dirty != "" {
			return "", errors.New("initial adoption is already recorded; later changes require the broker")
		}
		return git("rev-parse", "HEAD")
	}
	if _, err := git("add", "--force", "--all", "--", layout.Dir); err != nil {
		return "", err
	}
	message := "sdo: adopt existing operational memory\n\nSDO-Actor: human\nSDO-Phase: adoption\nSDO-Origin: adopted-existing\nSDO-Validation: skipped"
	if _, err := git("commit", "--allow-empty", "-m", message); err != nil {
		return "", err
	}
	return git("rev-parse", "HEAD")
}

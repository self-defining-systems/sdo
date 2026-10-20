package memory

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// DiagnosticsProjection gives the compiler an accepted detector bundle without
// exposing the repository's physical artifact path to the compiler.
type DiagnosticsProjection struct {
	Directory        string
	Fingerprint      string
	AcceptedRevision string
}

var ErrDiagnosticsNotBootstrapped = errors.New("diagnostics have not been bootstrapped")

func AcceptedDiagnostics(ctx context.Context, repository string) (DiagnosticsProjection, error) {
	directory, err := CandidateDiagnostics(repository)
	if err != nil {
		return DiagnosticsProjection{}, err
	}
	root := filepath.Dir(filepath.Dir(directory))
	git := func(args ...string) (string, error) {
		command := exec.CommandContext(ctx, "git", append([]string{"-C", root}, args...)...)
		output, err := command.CombinedOutput()
		if err != nil {
			return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
		}
		return strings.TrimSpace(string(output)), nil
	}
	revision, err := git("rev-parse", "HEAD")
	if err != nil {
		return DiagnosticsProjection{}, err
	}
	status, err := git("status", "--porcelain", "--", ".sdo/diagnostics", ".sdo/playbooks", ".sdo/lib", ".sdo/corpus")
	if err != nil {
		return DiagnosticsProjection{}, err
	}
	if status != "" {
		return DiagnosticsProjection{}, errors.New("diagnostics and program memory must match the accepted commit before controller launch")
	}
	fingerprint, err := git("rev-parse", "HEAD:.sdo/diagnostics")
	if err != nil {
		return DiagnosticsProjection{}, err
	}
	return DiagnosticsProjection{Directory: directory, Fingerprint: fingerprint, AcceptedRevision: revision}, nil
}

// CandidateDiagnostics resolves the physical detector bundle for a broker
// candidate. Candidates are staged but not yet committed, so no HEAD check is
// appropriate here; the validator binds the staged tree separately.
func CandidateDiagnostics(repository string) (string, error) {
	if repository == "" {
		return "", errors.New("candidate repository is required")
	}
	root, err := filepath.Abs(repository)
	if err != nil {
		return "", err
	}
	for _, part := range []string{filepath.Join(root, ".sdo"), filepath.Join(root, ".sdo", "diagnostics")} {
		info, err := os.Lstat(part)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return "", ErrDiagnosticsNotBootstrapped
			}
			return "", fmt.Errorf("inspect diagnostics storage: %w", err)
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return "", errors.New("diagnostics storage must be a real directory")
		}
	}
	manifest := filepath.Join(root, ".sdo", "diagnostics", "manifest.yaml")
	info, err := os.Lstat(manifest)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", ErrDiagnosticsNotBootstrapped
		}
		return "", fmt.Errorf("inspect diagnostics manifest: %w", err)
	}
	if !info.Mode().IsRegular() {
		return "", errors.New("diagnostics manifest must be a regular file")
	}
	return filepath.Join(root, ".sdo", "diagnostics"), nil
}

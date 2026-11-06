package validate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"sdo.dev/operator/memory/layout"
)

// preexistingHint tells the proposing agent the fault is not its work.
const preexistingHint = "pre-existing in accepted memory: the accepted base fails the same healthy replay with the same detector and violation, so this candidate did not cause it; recorded as a note, not a rejection; do not edit the detector to silence it"

// separatePreexisting moves a healthy-baseline fault into a non-blocking note
// only when the accepted base, replayed through the same compile runner on the
// same healthy sample, fails with the same detector and violation. That is
// the fact itself, not an inference from which paths the candidate changed:
// a shared-library edit no longer re-blocks unrelated accepted faults, and a
// fault the candidate introduced, however it got there, still rejects. Every
// other diagnostic rejects; the replay stage runs only after the registration
// and controller-start contracts passed, so detector IDs are trustworthy on
// both sides.
func separatePreexisting(ctx context.Context, request Request, diagnostics []Diagnostic, compile CompileRunner) ([]Diagnostic, error) {
	if !slices.ContainsFunc(diagnostics, attributableToDetector) || request.Base == "" {
		return diagnostics, nil
	}
	atBase, err := baseHealthyBaselineFaults(ctx, request, compile)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// Unestablished: every fault blocks, and the reason is recorded.
		request.noteGaps(Diagnostic{Rule: RuleDetectorHealthyBaseline, Path: layout.DiagnosticsDir, Message: "pre-existing healthy-baseline faults unverified at " + request.Base + ": " + err.Error()})
		atBase = nil
	}
	blocking := make([]Diagnostic, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		if !attributableToDetector(diagnostic) || !slices.Contains(atBase, faultKey(diagnostic)) {
			blocking = append(blocking, diagnostic)
			continue
		}
		diagnostic.Message = "pre-existing (also fails at " + request.Base + "): " + diagnostic.Message
		diagnostic.Hint = preexistingHint
		request.noteGaps(diagnostic)
	}
	return blocking, nil
}

func attributableToDetector(diagnostic Diagnostic) bool {
	return diagnostic.Rule == RuleDetectorHealthyBaseline && strings.HasPrefix(diagnostic.Path, layout.DetectorsDir+"/")
}

func faultKey(diagnostic Diagnostic) string { return diagnostic.Path + "\x00" + diagnostic.Message }

// baseHealthyBaselineFaults replays the base commit's tree through compile and
// returns its attributable healthy-baseline faults. A base that fails another
// gate first reports none, so every candidate fault blocks. The result is
// cached per base commit, healthy sample and validator identity.
func baseHealthyBaselineFaults(ctx context.Context, request Request, compile CompileRunner) ([]string, error) {
	commit, err := gitText(ctx, request.Root, "rev-parse", "--verify", request.Base+"^{commit}")
	if err != nil {
		return nil, err
	}
	cache := baseFaultCachePath(commit, request)
	if cache != "" {
		if data, err := os.ReadFile(cache); err == nil {
			var faults []string
			if json.Unmarshal(data, &faults) == nil {
				return faults, nil
			}
		}
	}
	scratch, err := os.MkdirTemp("", "sdo-validate-base-tree-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(scratch)
	tree := filepath.Join(scratch, "base")
	if _, err := git(ctx, scratch, "clone", "--quiet", "--shared", "--no-checkout", request.Root, tree); err != nil {
		return nil, err
	}
	if _, err := git(ctx, tree, "checkout", "--quiet", "--detach", commit); err != nil {
		return nil, err
	}
	base := request
	base.Root, base.Base, base.Layers, base.AcceptanceOnly = tree, "", Compile, true
	base.CoverageGaps, base.MitigationReplayNotes, base.StageTimings = nil, nil, nil
	diagnostics, err := compile(ctx, base)
	if err != nil {
		return nil, fmt.Errorf("replay base %s: %w", commit, err)
	}
	faults := []string{}
	for _, diagnostic := range diagnostics {
		if attributableToDetector(diagnostic) {
			faults = append(faults, faultKey(diagnostic))
		}
	}
	if cache != "" {
		if data, err := json.Marshal(faults); err == nil && os.MkdirAll(filepath.Dir(cache), 0o700) == nil {
			_ = writeFileAtomic(cache, data)
		}
	}
	return faults, nil
}

// baseFaultCachePath names the cached base result, or "" when the inputs
// cannot be identified: without a healthy sample file set (the broker's
// configured default) or a user cache directory the base is replayed again.
func baseFaultCachePath(commit string, request Request) string {
	if request.HealthyBaseline == "" {
		return ""
	}
	directory, err := os.UserCacheDir()
	if err != nil {
		return ""
	}
	hash := sha256.New()
	// The telemetry policy decides which generic detectors the base tree gets
	// (the alerts detector needs an alert-capable source), so it keys the cache.
	policy, err := json.Marshal(request.Telemetry)
	if err != nil {
		return ""
	}
	fmt.Fprintf(hash, "v2\x00%s\x00%s\x00%s\x00%s\x00", commit, request.Replay, request.OperatorRoot, policy)
	if executable, err := os.Executable(); err == nil {
		if info, err := os.Stat(executable); err == nil {
			fmt.Fprintf(hash, "%s\x00%d\x00%d\x00", executable, info.Size(), info.ModTime().UnixNano())
		}
	}
	err = filepath.WalkDir(request.HealthyBaseline, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(request.HealthyBaseline, path)
		fmt.Fprintf(hash, "%s\x00%d\x00", rel, len(data))
		hash.Write(data)
		return nil
	})
	if err != nil {
		return ""
	}
	return filepath.Join(directory, "sdo", "healthy-baseline-base", hex.EncodeToString(hash.Sum(nil))+".json")
}

func writeFileAtomic(path string, data []byte) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	_, writeErr := file.Write(data)
	if err := errors.Join(writeErr, file.Close()); err != nil {
		_ = os.Remove(file.Name())
		return err
	}
	return os.Rename(file.Name(), path)
}

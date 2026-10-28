package validate

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sdo.dev/operator/memory"
	"strings"
	"unicode/utf8"

	"sdo.dev/operator/memory/layout"
)

// ShellClassify extends the existing five artifact classes with free-form notes
// and scripts. It does not expand actor authority or affect Go validation.
func ShellClassify(p string) (layout.Kind, string, bool) {
	if kind, id, ok := layout.Classify(p); ok {
		return kind, id, true
	}
	if !filepath.IsLocal(p) || filepath.ToSlash(filepath.Clean(p)) != p {
		return "", "", false
	}
	parts := strings.Split(p, "/")
	if len(parts) >= 4 && parts[0] == layout.Dir && parts[1] == "playbooks" && layout.ValidPlaybookID(parts[2]) {
		if strings.HasSuffix(p, ".md") {
			return layout.KindPlaybook, parts[2], true
		}
		if strings.HasSuffix(p, ".sh") {
			return layout.KindPlaybookScript, strings.Join(parts[2:], "/"), true
		}
	}
	return "", "", false
}

var shellCredential = regexp.MustCompile(`(?im)(?:-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|(?:api[_-]?key|password|access[_-]?token|secret)\s*[:=]\s*["']?[a-z0-9_+/=-]{8,}|authorization:\s*bearer\s+[a-z0-9_+/=.-]{8,}|\b(?:ghp_|github_pat_|sk-proj-)[a-z0-9_]{12,})`)

// ShellHygiene inspects text and file shape only. It never executes authored code.
// Ownership is additionally checked by CheckShell using the broker's trusted actor.
func ShellHygiene(ctx context.Context, root string) ([]Diagnostic, error) {
	diagnostics := []Diagnostic{}
	add := func(rule, path, message string) {
		diagnostics = append(diagnostics, Diagnostic{Rule: rule, Path: path, Message: message})
	}
	base := filepath.Join(root, layout.Dir)
	err := filepath.WalkDir(base, func(p string, entry fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			if os.IsNotExist(err) && p == base {
				return nil
			}
			return err
		}
		rel, e := filepath.Rel(root, p)
		if e != nil {
			return e
		}
		rel = filepath.ToSlash(rel)
		for _, part := range strings.Split(rel, "/") {
			lower := strings.ToLower(part)
			if lower == ".git" || lower == ".env" || strings.Contains(lower, "secret") {
				add(RuleSecretPath, rel, "credential-bearing paths are forbidden")
				if entry.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
		}
		if entry.Type()&fs.ModeSymlink != 0 {
			add(RuleSymlink, rel, "symlinks are forbidden in operational memory")
			return nil
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			add(RuleNotRegular, rel, "operational memory requires regular files")
			return nil
		}
		kind, _, ok := ShellClassify(rel)
		if !ok {
			add(RuleUnknownPath, rel, "path is outside the shell operational-memory layout")
			return nil
		}
		// Controller-owned records are validated by their storage edge, not agents.
		switch kind {
		case layout.KindOutcome, layout.KindLegacyOutcomes, layout.KindRenderedLock, layout.KindRenderedObject, layout.KindCorpus:
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Size() > layout.MaxArtifactBytes {
			add(RuleTooLarge, rel, "artifact exceeds the size cap")
			return nil
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		if !utf8.Valid(data) || strings.IndexByte(string(data), 0) >= 0 {
			add("shell.text", rel, "artifact must be UTF-8 text without NUL bytes")
		}
		if shellCredential.Match(data) {
			add("shell.credentials", rel, "literal credentials are forbidden; use runtime references")
		}
		if kind.IsDetectorSource() && strings.HasSuffix(rel, ".sh") && info.Mode().Perm()&0111 == 0 {
			add("shell.executable", rel, "detector shell scripts must be executable")
		}
		return nil
	})
	if err == nil && len(diagnostics) == 0 {
		_, snapshotErr := memory.CandidateShellSnapshot(root, "candidate")
		if errors.Is(snapshotErr, memory.ErrSnapshotBudget) {
			add(RuleSnapshotBudget, layout.Dir, snapshotErr.Error())
		}
	}
	sortDiagnostics(diagnostics)
	return diagnostics, err
}

// CheckShell is the opt-in acceptance path: hygiene and the shared grant table.
// It deliberately imposes no Go compilation, replay, note schema or cause gate.
func CheckShell(ctx context.Context, request Request) ([]Diagnostic, error) {
	if request.Base == "" || !knownActor(request.Actor) || !knownPhase(request.Phase) {
		return nil, fmt.Errorf("shell memory check requires a valid base, actor and phase")
	}
	if diagnostics := PhaseDiagnostics(request.Phase); len(diagnostics) > 0 {
		return diagnostics, nil
	}
	paths, err := ChangedPaths(ctx, request.Root, request.Base)
	if err != nil {
		return nil, err
	}
	diagnostics, err := ShellHygiene(ctx, request.Root)
	if err != nil {
		return nil, err
	}
	diagnostics = append(diagnostics, clusterLocalDiagnostics(request.Root, paths)...)
	for _, p := range paths {
		kind, id := kindSource, ""
		if !filepath.IsLocal(filepath.FromSlash(p)) || strings.Contains(p, "\\") {
			diagnostics = append(diagnostics, Diagnostic{Rule: RuleUnsafePath, Path: p, Message: "unsafe proposal path"})
			continue
		}
		if layout.IsMemoryPath(p) {
			var ok bool
			kind, id, ok = ShellClassify(p)
			if !ok {
				diagnostics = append(diagnostics, Diagnostic{Rule: RuleOwnershipDenied, Path: p, Message: "unclassified memory path"})
				continue
			}
		}
		if layout.IsMemoryPath(p) && (request.Actor == Responder || request.Actor == HealthJudge) && !strings.HasSuffix(p, ".md") && !strings.HasSuffix(p, ".sh") {
			if _, err := os.Stat(filepath.Join(request.Root, p)); err == nil {
				diagnostics = append(diagnostics, Diagnostic{Rule: "shell.format", Path: p, Message: "shell authors write markdown notes and shell scripts"})
			}
		}
		if !allowed(request.Actor, request.Phase, kind, id) {
			diagnostics = append(diagnostics, Diagnostic{Rule: RuleOwnershipDenied, Path: p, Message: fmt.Sprintf("actor %s may not change %s in %s", request.Actor, kind, request.Phase)})
		}
	}
	sortDiagnostics(diagnostics)
	return diagnostics, nil
}

// ShellHygieneValidator is the compiler-free mechanism bound by shell launchers.
func ShellHygieneValidator(ctx context.Context, root string, _ []string) error {
	diagnostics, err := ShellHygiene(ctx, root)
	if err != nil {
		return err
	}
	return Err(diagnostics)
}

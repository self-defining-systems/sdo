// Package patchset parses, validates and applies SDO deployment patches: the
// strict-header git-format diffs under .sdo/deployment/patches/ that turn the
// accepted base tree into the effective tree. It is a leaf package (standard
// library plus the git process edge in internal/gitio).
package patchset

import (
	"fmt"
	"strings"
)

// Directory is where a repository keeps its patch series.
const Directory = ".sdo/deployment/patches"

// Limits per series (design sdo-only-design D7). Raise the constants, with the
// same tests, if a real replay needs more; never remove them.
const (
	MaxPatches    = 64
	MaxPatchBytes = 256 << 10
	MaxTotalBytes = 2 << 20
)

// Reason length bounds, in runes.
const (
	MinReasonRunes = 40
	MaxReasonRunes = 2000
)

// Kind classifies a PatchError.
type Kind string

const (
	// KindMalformed: the file is not a well-formed SDO patch.
	KindMalformed Kind = "malformed"
	// KindForbidden: well formed but outside policy (path, mode, binary, size).
	KindForbidden Kind = "forbidden"
	// KindConflict: context no longer matches the tree the patch applies to.
	KindConflict Kind = "conflict"
	// KindUpstreamed: the forward apply fails and the reverse check succeeds,
	// so the change is already present.
	KindUpstreamed Kind = "upstreamed"
	// KindMissingFile: a file the patch modifies, deletes or renames does not exist.
	KindMissingFile Kind = "missing_file"
)

// PatchError is the typed failure of parsing, validating or applying a patch.
// Patch names the offending file ("" for a series-level error), Paths the
// repository paths involved.
type PatchError struct {
	Kind   Kind
	Patch  string
	Paths  []string
	Detail string
}

func (e *PatchError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "patch %s", e.Kind)
	if e.Patch != "" {
		fmt.Fprintf(&b, " %s", e.Patch)
	}
	if len(e.Paths) > 0 {
		fmt.Fprintf(&b, " (%s)", strings.Join(e.Paths, ", "))
	}
	if e.Detail != "" {
		fmt.Fprintf(&b, ": %s", e.Detail)
	}
	return b.String()
}

func perr(kind Kind, patch string, paths []string, format string, args ...any) *PatchError {
	return &PatchError{Kind: kind, Patch: patch, Paths: paths, Detail: fmt.Sprintf(format, args...)}
}

// Owner is the actor a patch belongs to.
type Owner string

const (
	OwnerDeployer  Owner = "deployer"
	OwnerResponder Owner = "responder"
)

// Header is the parsed preamble.
type Header struct {
	Owner  Owner
	Reason string
	// Incident is required for responder patches and forbidden otherwise.
	Incident string
	// UpstreamBase is the 40-hex upstream commit the patch was authored or
	// refreshed against; Go fills it, so it may be empty in an agent draft.
	UpstreamBase string
	UpstreamFix  string
}

// Op is what a patch does to one file.
type Op string

const (
	OpModify Op = "modify"
	OpAdd    Op = "add"
	OpDelete Op = "delete"
	OpRename Op = "rename"
	OpCopy   Op = "copy"
)

// FileChange is one file section of the diff. OldPath is set for rename and copy.
type FileChange struct {
	Op      Op
	Path    string
	OldPath string
	// Mode is the resulting mode (100644 or 100755); "" when the section does
	// not state one.
	Mode string
}

// Patch is a parsed patch file. Diff is the part after the preamble, the only
// bytes handed to git apply.
type Patch struct {
	Name   string
	Header Header
	Files  []FileChange
	Diff   []byte
	raw    []byte
}

// Raw is the exact file content (preamble and diff).
func (p Patch) Raw() []byte { return append([]byte(nil), p.raw...) }

// Paths lists every repository path the patch touches (targets and rename or
// copy sources), sorted and unique.
func (p Patch) Paths() []string {
	seen := map[string]bool{}
	var out []string
	for _, f := range p.Files {
		for _, path := range []string{f.OldPath, f.Path} {
			if path != "" && !seen[path] {
				seen[path] = true
				out = append(out, path)
			}
		}
	}
	sortStrings(out)
	return out
}

// Package provenance names the code a result came from: the repository commit a
// tool ran on and the operator build that ran it.
package provenance

import (
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
)

// Unknown is the build ID when neither a VCS stamp nor operator source is available.
const Unknown = "unknown"

// DefaultSourceRoot is where images and the fast dev mount place the operator source.
const DefaultSourceRoot = "/opt/sdo/sdo/operator"

// Record is the provenance a tool result carries. Commit is the repository
// HEAD the tool ran on; Snapshot is a commit object holding uncommitted work
// when Dirty, so the result can still be traced to exact code. OperatorBuild
// names the SDO operator that ran the tool.
type Record struct {
	Commit        string `json:"commit,omitempty"`
	Dirty         bool   `json:"dirty,omitempty"`
	Snapshot      string `json:"snapshot,omitempty"`
	OperatorBuild string `json:"operator_build,omitempty"`
}

// Repository is the state of one checkout: its HEAD, whether the working tree
// differs from it, and a commit holding the differing tree.
type Repository struct {
	Commit   string
	Dirty    bool
	Snapshot string
}

var (
	cacheMu sync.Mutex
	cache   = map[string]string{}
)

// OperatorBuild is the build ID of the running operator. A binary built with a
// VCS stamp reports it; otherwise (the dev-mount fast path builds with
// -buildvcs=false) it is a digest of the operator source in the first root that
// has any. Results are cached per root list.
func OperatorBuild(roots ...string) string {
	key := strings.Join(roots, "\x00")
	cacheMu.Lock()
	defer cacheMu.Unlock()
	if value, ok := cache[key]; ok {
		return value
	}
	value := resolve(roots)
	cache[key] = value
	return value
}

func resolve(roots []string) string {
	if info, ok := debug.ReadBuildInfo(); ok {
		var revision string
		var modified bool
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				revision = setting.Value
			case "vcs.modified":
				modified = setting.Value == "true"
			}
		}
		if revision != "" && !modified {
			return "vcs-" + revision
		}
	}
	for _, root := range append(slices.Clone(roots), DefaultSourceRoot) {
		if root == "" {
			continue
		}
		if digest, err := SourceDigest(root); err == nil {
			return digest
		}
	}
	return Unknown
}

// SourceDigest hashes the operator's non-test Go, proto and module files by
// relative path and content, so the same source gives the same ID wherever it
// is mounted. It errors when the root holds no such file.
func SourceDigest(root string) (string, error) {
	var files []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := entry.Name()
		if entry.IsDir() {
			if name == "testdata" || name == "vendor" || strings.HasPrefix(name, ".") && path != root {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(name, "_test.go") {
			return nil
		}
		switch filepath.Ext(name) {
		case ".go", ".proto", ".mod", ".sum":
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	if len(files) == 0 {
		return "", fs.ErrNotExist
	}
	slices.Sort(files)
	hash := sha256.New()
	for _, path := range files {
		relative, _ := filepath.Rel(root, path)
		content, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		hash.Write([]byte(filepath.ToSlash(relative)))
		hash.Write([]byte{0})
		hash.Write(content)
		hash.Write([]byte{0})
	}
	return "src-" + hex.EncodeToString(hash.Sum(nil))[:16], nil
}

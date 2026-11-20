package bench

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Lock is <root>/<exp>/spec.lock.yaml (JSON, which is YAML): the spec as it
// was added and its inputs by content. A later edit of an identity field is
// refused (status shows it); the manager always measures by the lock.
type Lock struct {
	Spec      Spec      `json:"spec"`
	Added     time.Time `json:"added"`
	Instances *Input    `json:"instances,omitempty"`
	Baseline  *Input    `json:"baseline,omitempty"`
	Seed      *Input    `json:"seed,omitempty"`
}

// Input is a file or directory stored by content under <root>/inputs/<sha256>/.
type Input struct {
	SHA256 string `json:"sha256"`
	Path   string `json:"path"` // relative to the root: inputs/<sha256>[/<file>]
}

// Store copies src (a file or a directory) into <root>/inputs/<sha256>/ and
// returns it. A file keeps its base name inside the directory.
func Store(root, src string) (Input, error) {
	st, err := os.Stat(src)
	if err != nil {
		return Input{}, err
	}
	sum, err := HashPath(src)
	if err != nil {
		return Input{}, err
	}
	dir := filepath.Join("inputs", sum)
	rel := dir
	if !st.IsDir() {
		rel = filepath.Join(dir, filepath.Base(src))
	}
	if _, err := os.Stat(filepath.Join(root, dir)); err == nil {
		return Input{sum, rel}, nil
	}
	tmp := filepath.Join(root, "inputs", ".tmp-"+sum)
	_ = os.RemoveAll(tmp)
	if st.IsDir() {
		err = copyTree(src, tmp, false)
	} else if err = os.MkdirAll(tmp, 0o755); err == nil {
		err = copyFile(src, filepath.Join(tmp, filepath.Base(src)), st.Mode())
	}
	if err == nil {
		err = os.Rename(tmp, filepath.Join(root, dir))
	}
	if err != nil {
		_ = os.RemoveAll(tmp)
		return Input{}, err
	}
	return Input{sum, rel}, nil
}

// HashPath is the sha256 of a file, or of a directory's tree (each entry's
// relative path with its content's sha256 or its link target, sorted).
func HashPath(p string) (string, error) {
	st, err := os.Stat(p)
	if err != nil {
		return "", err
	}
	if !st.IsDir() {
		return hashFile(p)
	}
	m, err := Manifest(p)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	for _, e := range m {
		fmt.Fprintf(h, "%s\x00%s\n", e.Path, e.SHA256)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// ManifestEntry is one file of a directory: its content hash and size, or a link target.
type ManifestEntry struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

// nfsSillyRename is the name an NFS client gives a file deleted while a process still holds it open: ".nfs" and
// at least twelve hex digits (the client removes it when the last handle closes).
var nfsSillyRename = regexp.MustCompile(`^\.nfs[0-9a-f]{12,}$`)

// nfsLeftover reports a regular file of that name. It is not content of a step: a manifest that listed it would
// never be satisfied by a copy made a moment later, and the lane's step could not be adopted. Directories and
// every other name are kept.
func nfsLeftover(d fs.DirEntry) bool {
	return d.Type().IsRegular() && nfsSillyRename.MatchString(d.Name())
}

// rsyncNFSExclude is nfsSillyRename for rsync (which has no repetition count: twelve hex digits, then anything).
const rsyncNFSExclude = "--exclude=.nfs[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]*"

// Manifest lists every file under dir (sorted), the check that a copy is whole: the content hash of an input.
func Manifest(dir string) ([]ManifestEntry, error) { return manifest(dir, false) }

// StepManifest is Manifest of a step directory: it leaves out the NFS leftovers of files the step deleted while open.
func StepManifest(dir string) ([]ManifestEntry, error) { return manifest(dir, true) }

func manifest(dir string, step bool) ([]ManifestEntry, error) {
	var out []ManifestEntry
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dir, p)
		switch {
		case d.IsDir():
			return nil
		case step && nfsLeftover(d):
			return nil
		case d.Type()&fs.ModeSymlink != 0:
			t, err := os.Readlink(p)
			if err != nil {
				return err
			}
			out = append(out, ManifestEntry{Path: rel, SHA256: "->" + t})
		case d.Type().IsRegular():
			sum, err := hashFile(p)
			if err != nil {
				return err
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			out = append(out, ManifestEntry{Path: rel, SHA256: sum, Size: info.Size()})
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, err
}

// VerifyManifest checks that dir holds every file of m with the same size and content.
func VerifyManifest(dir string, m []ManifestEntry) error {
	have, err := StepManifest(dir)
	if err != nil {
		return err
	}
	got := map[string]ManifestEntry{}
	for _, e := range have {
		got[e.Path] = e
	}
	for _, e := range m {
		g, ok := got[e.Path]
		if !ok {
			return fmt.Errorf("%s: missing", e.Path)
		}
		if g.SHA256 != e.SHA256 || g.Size != e.Size {
			return fmt.Errorf("%s: differs from the staged copy", e.Path)
		}
	}
	return nil
}

func hashFile(p string) (string, error) {
	f, err := os.Open(p)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func copyFile(src, dst string, mode fs.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode.Perm())
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// copyTree copies dir src to dst (files, directories and symlinks).
func copyTree(src, dst string, step bool) error {
	return filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if step && nfsLeftover(d) {
			return nil
		}
		rel, _ := filepath.Rel(src, p)
		t := filepath.Join(dst, rel)
		info, err := d.Info()
		if err != nil {
			return err
		}
		switch {
		case d.IsDir():
			return os.MkdirAll(t, info.Mode().Perm()|0o700)
		case d.Type()&fs.ModeSymlink != 0:
			l, err := os.Readlink(p)
			if err != nil {
				return err
			}
			return os.Symlink(l, t)
		case d.Type().IsRegular():
			return copyFile(p, t, info.Mode())
		}
		return nil
	})
}

// writeJSON writes v to p atomically (a temp file renamed into place).
func writeJSON(p string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return writeFile(p, append(b, '\n'))
}

func writeFile(p string, b []byte) error {
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, p)
}

func readJSON(p string, v any) error {
	b, err := os.ReadFile(p)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("%s: %w", p, err)
	}
	return nil
}

// appendJSONL appends v as one line to p.
func appendJSONL(p string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(p, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(append(b, '\n'))
	return err
}

// readJSONL reads every line of p into a new T.
func readJSONL[T any](p string) ([]T, error) {
	b, err := os.ReadFile(p)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []T
	for _, line := range strings.Split(string(b), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var v T
		if err := json.Unmarshal([]byte(line), &v); err != nil {
			continue // a torn last line
		}
		out = append(out, v)
	}
	return out, nil
}

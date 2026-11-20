package bench

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/self-defining-systems/sdo/benchmarks/lanes/deploy"
)

// The lane platform's own code (the deploy/ scripts and the step binary, which is this executable) comes from the
// lanes install, never from a workload checkout. It is installed on each host, on first use, under
//
//	<staging>/lanes-<rev>/deploy/...   the embedded scripts (executable)
//	<staging>/lanes-<rev>/bin/lanes    the step binary (remote hosts; a local step runs the manager's own executable)
//
// where <rev> is the first 12 hex digits of the SHA-256 of the lanes executable, so two lanes revisions never share a
// directory and an upgrade installs beside the running one. Each part is written under a temporary name and renamed
// into place: a reader sees all of a part or none, and installing again is a no-op.
const platformPrefix = "lanes-"

// platformExecutable is the lanes executable the install is made from (a var for tests that stand in a fake step binary).
var platformExecutable = os.Executable

var revCache sync.Map // executable path -> revision

// platformRev names the running lanes revision.
func platformRev() (string, error) {
	exe, err := platformExecutable()
	if err != nil {
		return "", err
	}
	if v, ok := revCache.Load(exe); ok {
		return v.(string), nil
	}
	f, err := os.Open(exe)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	rev := hex.EncodeToString(h.Sum(nil))[:12]
	revCache.Store(exe, rev)
	return rev, nil
}

// PlatformDir is where this lanes revision's platform files live under staging.
func PlatformDir(staging string) (string, error) {
	rev, err := platformRev()
	if err != nil {
		return "", err
	}
	return filepath.Join(staging, platformPrefix+rev), nil
}

// isPlatformEntry reports whether a staging entry belongs to the platform install (staging gc must keep it).
func isPlatformEntry(name string) bool { return strings.HasPrefix(name, platformPrefix) }

// InstallPlatformScripts installs the embedded scripts under staging and returns the platform directory.
func InstallPlatformScripts(staging string) (string, error) {
	dir, err := PlatformDir(staging)
	if err != nil {
		return "", err
	}
	dst := filepath.Join(dir, "deploy")
	if st, err := os.Stat(dst); err == nil && st.IsDir() {
		return dir, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	tmp, err := os.MkdirTemp(dir, ".deploy-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(tmp)
	err = fs.WalkDir(deploy.Files, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		target := filepath.Join(tmp, filepath.FromSlash(p))
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		b, err := deploy.Files.ReadFile(p)
		if err != nil {
			return err
		}
		return os.WriteFile(target, b, 0o755)
	})
	if err != nil {
		return "", err
	}
	if err := os.Rename(tmp, dst); err != nil {
		if st, serr := os.Stat(dst); serr == nil && st.IsDir() {
			return dir, nil // another installer won the rename
		}
		return "", err
	}
	return dir, nil
}

// platformTar is the install as a tar stream: deploy/... and bin/lanes (this executable).
func platformTar() ([]byte, error) {
	exe, err := platformExecutable()
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	dirs := map[string]bool{}
	add := func(name string, b []byte) error {
		for d := path.Dir(name); d != "."; d = path.Dir(d) {
			if !dirs[d] {
				dirs[d] = true
				if err := tw.WriteHeader(&tar.Header{Name: d + "/", Typeflag: tar.TypeDir, Mode: 0o755}); err != nil {
					return err
				}
			}
		}
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o755, Size: int64(len(b)), Typeflag: tar.TypeReg}); err != nil {
			return err
		}
		_, err := tw.Write(b)
		return err
	}
	err = fs.WalkDir(deploy.Files, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := deploy.Files.ReadFile(p)
		if err != nil {
			return err
		}
		return add(path.Join("deploy", p), b)
	})
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(exe)
	if err != nil {
		return nil, err
	}
	if err := add("bin/lanes", b); err != nil {
		return nil, err
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

var installed sync.Map // ssh target + platform dir -> true once the host has this revision

// InstallPlatform makes sure the host has this revision's platform files and returns their directory there. A host that
// has them costs one cheap ssh; otherwise the scripts and the step binary are shipped in one tar stream and moved into
// place (temp directory, then rename).
func (h sshHost) InstallPlatform(ctx context.Context) (string, error) {
	rev, err := platformRev()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(h.h.Staging, platformPrefix+rev)
	key := h.h.SSH + "|" + dir
	if _, ok := installed.Load(key); ok {
		return dir, nil
	}
	q := shellQuote(dir)
	if _, err := h.run(ctx, nil, fmt.Sprintf("test -x %s/bin/lanes && test -d %s/deploy", q, q)); err != nil {
		b, err := platformTar()
		if err != nil {
			return "", err
		}
		script := fmt.Sprintf(`set -e
d=%s
mkdir -p "$d"
t=$(mktemp -d "$d/.install-XXXXXX")
trap 'rm -rf "$t"' EXIT
tar -x -C "$t"
[ -d "$d/deploy" ] || mv "$t/deploy" "$d/deploy" || [ -d "$d/deploy" ]
mkdir -p "$d/bin"
mv -f "$t/bin/lanes" "$d/bin/lanes"`, q)
		if _, err := h.runFor(ctx, 10*time.Minute, b, script); err != nil {
			return "", fmt.Errorf("install the lanes platform on %s: %w", h.name, err)
		}
	}
	installed.Store(key, true)
	return dir, nil
}

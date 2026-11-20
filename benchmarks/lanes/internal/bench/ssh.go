package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// sshHost reaches a remote host with one short ssh per call. Steps are
// started detached (setsid), so a dropped connection kills nothing; the
// manager retries a failed probe on its next pass.
type sshHost struct {
	name    string
	h       Host
	fuzzops map[string]map[string]string
}

// sshCommand is the ssh program ($FUZZOPS_BENCH_SSH, default ssh); rsync uses it too.
func sshCommand() string {
	if s := os.Getenv("FUZZOPS_BENCH_SSH"); s != "" {
		return s
	}
	return "ssh -o BatchMode=yes -o ConnectTimeout=20 -o ServerAliveInterval=15 -o ServerAliveCountMax=4"
}

// sshForRsync is the ssh command rsync runs (rsync -e), with the host's jump host.
func (h sshHost) sshForRsync() string {
	if h.h.SSHJump == "" {
		return sshCommand()
	}
	return sshCommand() + " -J " + h.h.SSHJump
}

func (h sshHost) run(ctx context.Context, stdin []byte, script string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	f := strings.Fields(h.sshForRsync())
	cmd := exec.CommandContext(ctx, f[0], append(f[1:], h.h.SSH, script)...)
	cmd.Stdin = bytes.NewReader(stdin)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return out.Bytes(), fmt.Errorf("ssh %s: %v: %s", h.h.SSH, err, strings.TrimSpace(errb.String()))
	}
	return out.Bytes(), nil
}

func (h sshHost) Start(ctx context.Context, s Step) error {
	dir := filepath.Join(h.h.Staging, s.Lane, s.ID)
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	p := filepath.Join(dir, "step.json")
	_, err = h.run(ctx, b, fmt.Sprintf("mkdir -p %s && cat > %s && %s", shellQuote(dir), shellQuote(p),
		detach(filepath.Join(s.Fuzzops, "bin", "fuzzops-bench"), p, laneExecPrefix(s))))
	return err
}

func (h sshHost) Probe(ctx context.Context, steps map[string]string, fuzzops []string) (Probe, error) {
	if len(fuzzops) == 0 {
		return Probe{}, fmt.Errorf("no fuzzops checkout on %s", h.name)
	}
	args := []string{shellQuote(filepath.Join(fuzzops[0], "bin", "fuzzops-bench")), "probe", "--staging", shellQuote(h.h.Staging),
		"--fuzzops", shellQuote(strings.Join(fuzzops, ","))}
	var lanes []string
	for l := range steps {
		lanes = append(lanes, l)
	}
	sort.Strings(lanes)
	for _, l := range lanes {
		args = append(args, shellQuote(l+"="+steps[l]))
	}
	out, err := h.run(ctx, nil, strings.Join(args, " "))
	if err != nil {
		return Probe{}, err
	}
	var p Probe
	if err := json.Unmarshal(out, &p); err != nil {
		return Probe{}, fmt.Errorf("probe on %s: %v: %q", h.name, err, out)
	}
	if p.Free <= 0 {
		// The probe ran the first pin's binary, which may predate free_bytes; the disk floor must not depend on it.
		if free, err := h.freeBytes(ctx); err == nil {
			p.Free = free
		}
	}
	return p, nil
}

// freeBytes is the free space of the staging filesystem, read with df so that no pinned binary is involved.
func (h sshHost) freeBytes(ctx context.Context) (int64, error) {
	script := "mkdir -p " + shellQuote(h.h.Staging) + " && df -Pk " + shellQuote(h.h.Staging) + " | tail -n 1"
	out, err := h.run(ctx, nil, script)
	if err != nil {
		return 0, err
	}
	f := strings.Fields(string(out))
	if len(f) < 4 {
		return 0, fmt.Errorf("df on %s: unexpected output %q", h.name, out)
	}
	kb, err := strconv.ParseInt(f[3], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("df on %s: %v", h.name, err)
	}
	return kb * 1024, nil
}

// rsync copies with checksums over the same ssh.
func rsync(ctx context.Context, ssh, src, dst string, exclude ...string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	args := append([]string{"-a", "--checksum"}, exclude...)
	out, err := exec.CommandContext(ctx, "rsync", append(args, "-e", ssh, src, dst)...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("rsync %s %s: %v: %s", src, dst, err, strings.TrimSpace(string(out)))
	}
	return nil
}

func (h sshHost) Fetch(ctx context.Context, lane, id, dest string) error {
	tmp := dest + ".partial"
	if err := os.MkdirAll(tmp, 0o755); err != nil {
		return err
	}
	if err := rsync(ctx, h.sshForRsync(), h.h.SSH+":"+filepath.Join(h.h.Staging, lane, id)+"/", tmp+"/", rsyncNFSExclude); err != nil {
		return err
	}
	var m []ManifestEntry
	if err := readJSON(filepath.Join(tmp, "manifest.json"), &m); err != nil {
		return err
	}
	if err := VerifyManifest(tmp, m); err != nil {
		return fmt.Errorf("copy from %s: %w", h.name, err)
	}
	_ = os.RemoveAll(dest)
	return os.Rename(tmp, dest)
}

func (h sshHost) Clean(ctx context.Context, lane, id string) error {
	_, err := h.run(ctx, nil, "rm -rf "+shellQuote(filepath.Join(h.h.Staging, lane, id)))
	return err
}

// Stage ships an input to <staging>/inputs/<sha256>/ once (its content names it).
func (h sshHost) Stage(ctx context.Context, root Root, in Input) (string, error) {
	rel := strings.TrimPrefix(in.Path, "inputs/")
	dst := filepath.Join(h.h.Staging, "inputs", in.SHA256)
	if _, err := h.run(ctx, nil, "test -d "+shellQuote(dst)); err == nil {
		return filepath.Join(h.h.Staging, "inputs", rel), nil
	}
	tmp := dst + ".partial"
	if _, err := h.run(ctx, nil, "mkdir -p "+shellQuote(tmp)); err != nil {
		return "", err
	}
	if err := rsync(ctx, h.sshForRsync(), root.path("inputs", in.SHA256)+"/", h.h.SSH+":"+tmp+"/"); err != nil {
		return "", err
	}
	if _, err := h.run(ctx, nil, fmt.Sprintf("rm -rf %s && mv %s %s", shellQuote(dst), shellQuote(tmp), shellQuote(dst))); err != nil {
		return "", err
	}
	return filepath.Join(h.h.Staging, "inputs", rel), nil
}

func (h sshHost) GC(ctx context.Context, keep []string) ([]string, error) {
	var fz string
	for _, byHost := range h.fuzzops {
		if p := byHost[h.name]; p != "" {
			fz = p
		}
	}
	if fz == "" {
		return nil, fmt.Errorf("no fuzzops checkout on %s", h.name)
	}
	args := []string{shellQuote(filepath.Join(fz, "bin", "fuzzops-bench")), "gc-staging", "--staging", shellQuote(h.h.Staging)}
	for _, k := range keep {
		args = append(args, shellQuote(k))
	}
	out, err := h.run(ctx, nil, strings.Join(args, " "))
	if err != nil {
		return nil, err
	}
	var removed []string
	if err := json.Unmarshal(out, &removed); err != nil {
		return nil, fmt.Errorf("gc on %s: %v: %q", h.name, err, out)
	}
	return removed, nil
}

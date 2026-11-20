package bench

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// HostOps is how the manager reaches one host: start a step there detached,
// see how the host's steps stand, and bring a finished step's files home.
type HostOps interface {
	// Start writes the step into <staging>/<lane>/<id>/step.json and starts it detached.
	Start(ctx context.Context, s Step) error
	// Probe reports how the given steps stand (lane -> step id), the host's
	// 5-minute load and the in-flight markers in the given fuzzops checkouts.
	Probe(ctx context.Context, steps map[string]string, fuzzops []string) (Probe, error)
	// Fetch copies a finished step's directory to dest and checks it against its manifest.
	Fetch(ctx context.Context, lane, id, dest string) error
	// Clean removes a step's staging directory (only after Fetch).
	Clean(ctx context.Context, lane, id string) error
	// Kill ends a running step (its process group): TERM, then KILL after a few seconds. A step that is not there
	// is no error.
	Kill(ctx context.Context, lane, id string) error
	// Stage makes an input of the root available on the host and returns its path there.
	Stage(ctx context.Context, root Root, in Input) (string, error)
	// GC removes from the host's staging every lane directory, step directory and staged input not in
	// keep (paths relative to the staging directory) and returns what it removed.
	GC(ctx context.Context, keep []string) ([]string, error)
}

// Step states a probe reports.
const (
	StepRunning  = "running"  // the step holds its lane's step lock
	StepFinished = "finished" // result.json is written
	StepDead     = "dead"     // no result and nobody holds the lock: it was killed
)

// Probe is what a host's probe returns.
type Probe struct {
	Load5    float64           `json:"load5"`
	Free     int64             `json:"free_bytes"`
	Steps    map[string]string `json:"steps"`
	Inflight []string          `json:"inflight,omitempty"`
	// Phases is what each running step is doing (phase.json); a host binary from before phases reports none.
	Phases map[string]*Phase `json:"phases,omitempty"`
	// Canary is the host health reading (hosthealth.go); nil when the transport takes none.
	Canary *Canary `json:"canary,omitempty"`
}

// startGrace is how long a step that does not hold its lock yet counts as starting, not dead (a var for tests).
var startGrace = 30 * time.Second

// ProbeStaging reads the steps' states under staging on this machine.
func ProbeStaging(staging string, steps map[string]string, fuzzops []string) Probe {
	p := Probe{Steps: map[string]string{}}
	for lane, id := range steps {
		dir := filepath.Join(staging, lane, id)
		switch {
		case fileExists(filepath.Join(dir, "manifest.json")):
			p.Steps[lane] = StepFinished
		case lockHeld(filepath.Join(staging, lane, "step.lock")):
			p.Steps[lane] = StepRunning
		case fileExists(filepath.Join(dir, "step.json")) && time.Since(modTime(filepath.Join(dir, "step.json"))) < startGrace:
			p.Steps[lane] = StepRunning // just started, not holding its lock yet
		default:
			p.Steps[lane] = StepDead
		}
		var ph Phase
		if p.Steps[lane] == StepRunning && readJSON(filepath.Join(dir, phaseFile), &ph) == nil {
			if p.Phases == nil {
				p.Phases = map[string]*Phase{}
			}
			p.Phases[lane] = &ph
		}
	}
	var fs syscall.Statfs_t
	if err := os.MkdirAll(staging, 0o755); err == nil && syscall.Statfs(staging, &fs) == nil {
		p.Free = int64(fs.Bavail) * int64(fs.Bsize)
	}
	if b, err := os.ReadFile("/proc/loadavg"); err == nil {
		if f := strings.Fields(string(b)); len(f) >= 2 {
			p.Load5, _ = strconv.ParseFloat(f[1], 64)
		}
	}
	for _, fz := range fuzzops {
		ms, _ := filepath.Glob(filepath.Join(fz, "results", "state", "inflight", "*.json"))
		for _, m := range ms {
			p.Inflight = append(p.Inflight, strings.TrimSuffix(filepath.Base(m), ".json"))
		}
	}
	return p
}

func fileExists(p string) bool { _, err := os.Stat(p); return err == nil }

func modTime(p string) time.Time {
	st, err := os.Stat(p)
	if err != nil {
		return time.Time{}
	}
	return st.ModTime()
}

func lockHeld(p string) bool {
	f, err := os.OpenFile(p, os.O_RDWR, 0)
	if err != nil {
		return false
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return true
	}
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return false
}

// NewStepID names a step: <kind>-<UTC time>-<random>.
func NewStepID(kind string) string {
	b := make([]byte, 3)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%s-%s-%s", kind, time.Now().UTC().Format("20060102T150405"), hex.EncodeToString(b))
}

// detach is the shell line that starts a step so that it outlives whoever started it.
func detach(bin, stepPath string, prefix ...string) string {
	dir := filepath.Dir(stepPath)
	// setsid -f forks at once and every descriptor of the step is redirected, so the caller (an ssh session)
	// returns while the step runs; no part of the line is left in the background holding the caller's output.
	return fmt.Sprintf("cd %s && setsid -f %s%s step %s > %s 2>&1 < /dev/null", shellQuote(dir), strings.Join(prefix, ""), shellQuote(bin), shellQuote(stepPath), shellQuote(filepath.Join(dir, "runner.out")))
}

// localHost runs steps on the manager's machine.
type localHost struct {
	staging string
	bin     string // fuzzops-bench (the manager's own executable)
	health  HealthThresholds
	mins    map[string]int64 // kernel minimums to read (kernelmin.go)
}

func (h localHost) Start(ctx context.Context, s Step) error {
	dir := filepath.Join(h.staging, s.Lane, s.ID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	platform, err := InstallPlatformScripts(h.staging)
	if err != nil {
		return err
	}
	s.Platform = platform
	p := filepath.Join(dir, "step.json")
	if err := writeJSON(p, s); err != nil {
		return err
	}
	return exec.CommandContext(ctx, "sh", "-c", detach(h.bin, p, laneExecPrefix(s))).Run()
}

func (h localHost) Kill(ctx context.Context, lane, id string) error {
	return exec.CommandContext(ctx, "sh", "-c", killScript(filepath.Join(h.staging, lane, id, "step.json"), 5)).Run()
}

func (h localHost) Probe(ctx context.Context, steps map[string]string, fuzzops []string) (Probe, error) {
	p := ProbeStaging(h.staging, steps, fuzzops)
	p.Canary, _ = localCanary(ctx, h.health.ProbeTimeout, h.mins)
	return p, nil
}

func (h localHost) Fetch(_ context.Context, lane, id, dest string) error {
	return fetchDir(filepath.Join(h.staging, lane, id), dest)
}

// fetchDir copies src to dest through a temporary directory and checks the copy against src's manifest.
func fetchDir(src, dest string) error {
	var m []ManifestEntry
	if err := readJSON(filepath.Join(src, "manifest.json"), &m); err != nil {
		return err
	}
	tmp := dest + ".partial"
	_ = os.RemoveAll(tmp)
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	if err := copyTree(src, tmp, true); err != nil {
		return err
	}
	if err := VerifyManifest(tmp, m); err != nil {
		return fmt.Errorf("copy of %s: %w", src, err)
	}
	_ = os.RemoveAll(dest)
	return os.Rename(tmp, dest)
}

func (h localHost) Clean(_ context.Context, lane, id string) error {
	return os.RemoveAll(filepath.Join(h.staging, lane, id))
}

func (h localHost) Stage(_ context.Context, root Root, in Input) (string, error) {
	return root.path(in.Path), nil
}

// GCStaging removes under staging every <lane>/<entry> and inputs/<sha> not in keep.
func GCStaging(staging string, keep []string) []string {
	kept := map[string]bool{}
	for _, k := range keep {
		kept[k] = true
	}
	var removed []string
	lanes, _ := os.ReadDir(staging)
	for _, l := range lanes {
		if !l.IsDir() || isPlatformEntry(l.Name()) {
			continue
		}
		ents, _ := os.ReadDir(filepath.Join(staging, l.Name()))
		for _, e := range ents {
			rel := filepath.Join(l.Name(), e.Name())
			if kept[rel] {
				continue
			}
			if l.Name() == "inputs" && strings.HasSuffix(e.Name(), ".partial") {
				continue
			}
			if os.RemoveAll(filepath.Join(staging, rel)) == nil {
				removed = append(removed, rel)
			}
		}
	}
	return removed
}

func (h localHost) GC(_ context.Context, keep []string) ([]string, error) {
	return GCStaging(h.staging, keep), nil
}

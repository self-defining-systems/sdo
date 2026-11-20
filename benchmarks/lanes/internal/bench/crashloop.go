package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/self-defining-systems/sdo/benchmarks/lanes/internal/plan"
)

// Crash-loop detector (docs/host-safety.md). Pods killed at their memory limit restart again and again, and every
// restart creates a container and its network namespace. A lane whose pods restart more than `restarts` times in
// `window` is marked unhealthy: it is given no new build or run and an alarm fires. Nothing running is touched.

// CrashLoopConf is a host's `crashloop:` block of hosts.yaml; zero values mean the defaults (10 restarts in 10 minutes).
type CrashLoopConf struct {
	Restarts int    `json:"restarts,omitempty"`
	Window   string `json:"window,omitempty"`
}

// CrashLoopThresholds are CrashLoopConf with the defaults filled in.
type CrashLoopThresholds struct {
	Restarts int64
	Window   time.Duration
}

func (c CrashLoopConf) Thresholds() CrashLoopThresholds {
	t := CrashLoopThresholds{Restarts: 10, Window: 10 * time.Minute}
	if c.Restarts > 0 {
		t.Restarts = int64(c.Restarts)
	}
	if d, err := time.ParseDuration(c.Window); err == nil && d > 0 {
		t.Window = d
	}
	return t
}

func (c CrashLoopConf) validate() []string {
	var errs []string
	if d, err := time.ParseDuration(c.Window); c.Window != "" && (err != nil || d <= 0) {
		errs = append(errs, fmt.Sprintf("crashloop.window %q: a positive duration such as 10m", c.Window))
	}
	if c.Restarts < 0 {
		errs = append(errs, "crashloop.restarts must not be negative")
	}
	return errs
}

type crashSnap struct {
	at   time.Time
	pods map[string]int64
}

// crashTracker counts the container restarts of one lane over a sliding window.
type crashTracker struct{ snaps []crashSnap }

// Observe takes one reading (container -> restart count) and returns the restarts since the oldest reading still in
// the window. The first reading is only a baseline: restarts before it are history.
func (t *crashTracker) Observe(now time.Time, pods map[string]int64, window time.Duration) int64 {
	t.snaps = append(t.snaps, crashSnap{now, pods})
	keep := 0
	for i, s := range t.snaps {
		if now.Sub(s.at) <= window {
			break
		}
		keep = i // the newest reading older than the window stays as the baseline
	}
	t.snaps = t.snaps[keep:]
	// Restarts are summed over consecutive readings, so a pod that was deleted and recreated (its count falls) or that
	// appeared (no earlier count) contributes the restarts it has, never a negative number.
	var n int64
	for i := 1; i < len(t.snaps); i++ {
		prev := t.snaps[i-1].pods
		for k, v := range t.snaps[i].pods {
			switch p, seen := prev[k]; {
			case !seen || v < p:
				n += v
			default:
				n += v - p
			}
		}
	}
	return n
}

// podReader is implemented by hosts that can read a lane's pods (one bounded kubectl call).
type podReader interface {
	PodRestarts(ctx context.Context, lane, kubeconfig string) (map[string]int64, error)
}

const podReadBound = 15 * time.Second

// kubectlPods is the read: restart counts of every container of every pod of the lane's cluster.
func kubectlPods(kubeconfig string) string {
	return fmt.Sprintf("timeout %d kubectl --kubeconfig %s get pods -A -o json --request-timeout=%ds",
		int(podReadBound.Seconds())-3, shellQuote(kubeconfig), int(podReadBound.Seconds())-5)
}

func parsePods(out []byte) (map[string]int64, error) {
	var l struct {
		Items []struct {
			Metadata struct{ Namespace, Name string }
			Status   struct {
				ContainerStatuses []struct {
					Name         string
					RestartCount int64
				}
				InitContainerStatuses []struct {
					Name         string
					RestartCount int64
				}
			}
		}
	}
	if err := json.Unmarshal(out, &l); err != nil {
		return nil, err
	}
	m := map[string]int64{}
	for _, p := range l.Items {
		for _, c := range append(p.Status.ContainerStatuses, p.Status.InitContainerStatuses...) {
			m[p.Metadata.Namespace+"/"+p.Metadata.Name+"/"+c.Name] = c.RestartCount
		}
	}
	return m, nil
}

func (h localHost) PodRestarts(ctx context.Context, _, kubeconfig string) (map[string]int64, error) {
	ctx, cancel := context.WithTimeout(ctx, podReadBound)
	defer cancel()
	cmd := exec.CommandContext(ctx, "sh", "-c", kubectlPods(kubeconfig))
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = time.Second
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("kubectl get pods: %v", err)
	}
	return parsePods(out)
}

func (h sshHost) PodRestarts(ctx context.Context, _, kubeconfig string) (map[string]int64, error) {
	ctx, cancel := context.WithTimeout(ctx, podReadBound)
	defer cancel()
	f := strings.Fields(h.sshForRsync())
	cmd := exec.CommandContext(ctx, f[0], append(f[1:], h.h.SSH, kubectlPods(kubeconfig))...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	cmd.WaitDelay = time.Second
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("kubectl get pods on %s: %v: %s", h.name, err, strings.TrimSpace(errb.String()))
	}
	return parsePods(out.Bytes())
}

func (g guardedOps) PodRestarts(ctx context.Context, lane, kubeconfig string) (map[string]int64, error) {
	r, ok := g.in.(podReader)
	if !ok {
		return nil, fmt.Errorf("host %s cannot read pods", g.host)
	}
	if err := g.down(); err != nil {
		return nil, err
	}
	return bounded(ctx, podReadBound+5*time.Second, "pod read on "+g.host, func(c context.Context) (map[string]int64, error) {
		return r.PodRestarts(c, lane, kubeconfig)
	})
}

// crashBlock says why lane gets no new steps because its pods crash-loop ("" when it may).
func (m *Manager) crashBlock(lane string) string { return m.crash[lane] }

// observeCrashLoops takes one pod reading per lane with a cluster, all at once and each bounded, and updates which lanes
// are crash-looping. A failed read is no reading: it neither marks nor clears a lane.
func (m *Manager) observeCrashLoops(ctx context.Context, reachable map[string]bool) {
	if m.crash == nil {
		m.crash, m.crashTrack = map[string]string{}, map[string]*crashTracker{}
	}
	type read struct {
		pods map[string]int64
		err  error
	}
	reads := map[string]*read{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, l := range m.sortedLanes() {
		if !reachable[l.Host] || l.Gone {
			continue
		}
		h := m.inv.Hosts[l.Host]
		ops, lane, kc := m.ops(l.Host), l.Lane, filepath.Join(h.Kubeconfigs, l.Lane)
		pr, ok := ops.(podReader)
		if !ok {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			pods, err := pr.PodRestarts(ctx, lane, kc)
			mu.Lock()
			reads[lane] = &read{pods, err}
			mu.Unlock()
		}()
	}
	wg.Wait()
	for _, l := range m.sortedLanes() {
		r := reads[l.Lane]
		if r == nil || r.err != nil {
			continue
		}
		th := m.inv.Hosts[l.Host].CrashLoop.Thresholds()
		tr := m.crashTrack[l.Lane]
		if tr == nil {
			tr = &crashTracker{}
			m.crashTrack[l.Lane] = tr
		}
		n := tr.Observe(m.now(), r.pods, th.Window)
		was := m.crash[l.Lane]
		why := ""
		if n > th.Restarts {
			why = fmt.Sprintf("lane %s is crash-looping: %d container restarts in %s (limit %d)", l.Lane, n, th.Window, th.Restarts)
		}
		m.crash[l.Lane] = why
		switch {
		case why != "" && was == "":
			m.logf("ALARM %s, no new steps on it (what runs is left alone)", why)
			(&AlarmLog{Root: m.Root, Logf: m.logf}).Raise(plan.Alarm{At: m.now().UTC(), Alarm: "lane-crash-looping", Key: "lane-crash-looping/" + l.Lane, Subject: l.Lane, Host: l.Host, Detail: why})
		case why == "" && was != "":
			m.logf("lane %s: restarts are back under %d in %s, new steps resume", l.Lane, th.Restarts, th.Window)
		}
	}
}

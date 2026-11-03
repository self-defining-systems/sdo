//go:build integration && controller_e2e

package e2e

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// The two properties every run checks, from controller logs, API state and
// accepted memory only (nothing the controller does not already publish):
//
//	(a) no wedge: every incident the controller opened reaches exactly one
//	    terminal outcome once faults stop, and an incident that failed never
//	    stops a later one from getting its outcome.
//	(b) detector cadence: every active detector appears in the controller's
//	    evaluation passes within IntervalFactor times its own interval while
//	    a controller process is up, through memory swaps, closure and agent
//	    work; a detector activated by a memory commit appears with no
//	    controller restart.
//
// HARNESS-1 states the same two checks over the in-process simulation's event
// log; the definitions here are the same ones, read from process-level evidence.

// Cadence bounds. A pass takes up to a second under load, so the factor has a floor.
const (
	IntervalFactor  = 5
	CadenceFloor    = 5 * time.Second
	StartupGrace    = 30 * time.Second // first pass of a detector after a controller process starts
	ActivationGrace = 60 * time.Second // memory commit to first pass, with no restart
)

// DetectorSpec is an active detector read from accepted memory.
type DetectorSpec struct {
	ID       string
	Interval time.Duration
	AddedAt  time.Time
}

var (
	idPattern       = regexp.MustCompile(`(?m)^id:\s*(\S+)`)
	intervalPattern = regexp.MustCompile(`(?m)^interval:\s*(\S+)`)
	incidentPattern = regexp.MustCompile(`incident_workspace_preparing incident_id=(\S+)`)
)

// Detectors lists the detectors in accepted memory at HEAD with their intervals and the commit time that added them.
func (w *World) Detectors() []DetectorSpec {
	repo := w.AcceptedRepo()
	names, err := exec.Command("git", "-C", repo, "ls-tree", "-r", "--name-only", "HEAD", ".sdo/diagnostics/detectors").Output()
	if err != nil {
		return nil
	}
	var out []DetectorSpec
	for _, name := range strings.Fields(string(names)) {
		if !strings.HasSuffix(name, "/manifest.yaml") {
			continue
		}
		data, err := exec.Command("git", "-C", repo, "show", "HEAD:"+name).Output()
		if err != nil {
			continue
		}
		id := idPattern.FindStringSubmatch(string(data))
		interval := intervalPattern.FindStringSubmatch(string(data))
		if id == nil || interval == nil {
			continue
		}
		duration, err := time.ParseDuration(interval[1])
		if err != nil {
			continue
		}
		added, _ := exec.Command("git", "-C", repo, "log", "--diff-filter=A", "--format=%ct", "-1", "--", name).Output()
		seconds, _ := strconv.ParseInt(strings.TrimSpace(string(added)), 10, 64)
		out = append(out, DetectorSpec{ID: id[1], Interval: duration, AddedAt: time.Unix(seconds, 0)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// OpenedIncidents are the incident IDs the controller dispatched, from its own log.
func (w *World) OpenedIncidents() []string {
	seen := map[string]bool{}
	var ids []string
	for _, line := range w.ControllerLines() {
		if m := incidentPattern.FindStringSubmatch(line.Text); m != nil && !seen[m[1]] {
			seen[m[1]] = true
			ids = append(ids, m[1])
		}
	}
	return ids
}

// CheckNoWedge verifies property (a) against the current evidence. Call it after faults stop and the run has settled.
func (w *World) CheckNoWedge() []string {
	var violations []string
	outcomes := map[string]Outcome{}
	for _, outcome := range w.Outcomes() {
		if id, ok := outcome.Raw["incident_id"].(string); ok {
			sum := sha256.Sum256([]byte(id))
			if want := ".sdo/outcomes/" + hex.EncodeToString(sum[:]) + ".json"; outcome.File != want {
				violations = append(violations, fmt.Sprintf("outcome for %s is stored at %s, want %s", id, outcome.File, want))
			}
			if _, dup := outcomes[id]; dup {
				violations = append(violations, fmt.Sprintf("incident %s has two outcome documents", id))
			}
			outcomes[id] = outcome
		}
	}
	for _, id := range w.OpenedIncidents() {
		outcome, ok := outcomes[id]
		if !ok {
			violations = append(violations, fmt.Sprintf("incident %s opened but has no terminal outcome", id))
			continue
		}
		if class, _ := outcome.Raw["classification"].(string); class == "" {
			violations = append(violations, fmt.Sprintf("incident %s outcome has no classification", id))
		}
	}
	return violations
}

// CheckCadence verifies property (b) over every controller process's evaluation passes.
// exempt names detectors the run broke on purpose (their own cadence is not checked; everyone else's is);
// pauses are windows the run declared as deliberate faults.
func (w *World) CheckCadence(exempt map[string]bool, pauses ...Window) []string {
	byPod := map[string][]Iteration{}
	var pods []string
	for _, it := range w.Iterations() {
		if _, ok := byPod[it.Pod]; !ok {
			pods = append(pods, it.Pod)
		}
		byPod[it.Pod] = append(byPod[it.Pod], it)
	}
	running := map[string]bool{}
	for _, pod := range pods {
		running[pod] = w.podRunning(pod)
	}
	return cadenceViolations(byPod, pods, running, w.Detectors(), exempt, pauses, time.Now())
}

// CadenceCoverage counts, per detector, the passes that listed it. A check over no passes proves nothing, so runs log this.
func (w *World) CadenceCoverage() map[string]int {
	counts := map[string]int{}
	for _, it := range w.Iterations() {
		for _, id := range it.Detectors {
			counts[id]++
		}
	}
	return counts
}

// cadenceViolations is the pure form of property (b): passes per controller process (oldest first), which of those
// processes are still running at now, and the detectors in accepted memory.
func cadenceViolations(byPod map[string][]Iteration, pods []string, running map[string]bool, specs []DetectorSpec, exempt map[string]bool, pauses []Window, now time.Time) []string {
	var violations []string
	for _, pod := range pods {
		passes := byPod[pod]
		first, last := passes[0].At, passes[len(passes)-1].At
		if running[pod] {
			last = now // a live controller that went quiet is the wedge this check exists for
		}
		for _, spec := range specs {
			if exempt[spec.ID] {
				continue
			}
			bound := max(IntervalFactor*spec.Interval, CadenceFloor)
			start, grace := first, StartupGrace
			if spec.AddedAt.After(first) {
				start, grace = spec.AddedAt, ActivationGrace // activated while this process was running
			}
			if !start.Before(last) {
				continue
			}
			previous, allowed := start, bound+grace
			seen := false
			for _, pass := range passes {
				if pass.At.Before(start) || !contains(pass.Detectors, spec.ID) {
					continue
				}
				if gap := pass.At.Sub(previous); gap > allowed && !paused(pauses, previous, pass.At) {
					violations = append(violations, fmt.Sprintf("detector %s: no pass for %s (bound %s) in %s ending %s", spec.ID, gap.Round(time.Millisecond), allowed, pod, pass.At.Format("15:04:05.000")))
				}
				previous, allowed, seen = pass.At, bound, true
			}
			if gap := last.Sub(previous); gap > allowed && !paused(pauses, previous, last) {
				what := "last pass"
				if !seen {
					what = "start"
				}
				violations = append(violations, fmt.Sprintf("detector %s: no pass for %s (bound %s) from %s to the end of %s", spec.ID, gap.Round(time.Millisecond), allowed, what, pod))
			}
		}
	}
	return violations
}

func contains(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}
	return false
}

func (w *World) podRunning(name string) bool {
	pod, err := w.Cluster.Client.CoreV1().Pods(ControlNamespace).Get(context.Background(), name, metav1.GetOptions{})
	return err == nil && pod.Status.Phase == corev1.PodRunning
}

// paused reports whether a gap overlaps a window the run declared as a deliberate fault.
func paused(windows []Window, from, to time.Time) bool {
	for _, w := range windows {
		if from.Before(w.To) && to.After(w.From) {
			return true
		}
	}
	return false
}

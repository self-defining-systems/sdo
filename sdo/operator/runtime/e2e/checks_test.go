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

	"sdo.dev/operator/runtime/sim"
)

// The two properties every run checks are the ones the in-process harness checks, with one definition:
// the run's process-level evidence (controller logs, API state, accepted memory) is turned into a
// sim.Log and handed to sim.Check. Nothing here defines "no wedge" or "detector cadence" again.
//
//	every incident ends: each incident the controller opened reaches a terminal outcome once faults
//	    stop, a failed incident never blocks a later one, and the controller never exits on its own;
//	detection keeps running: every active detector is evaluated within GapFactor times its own interval
//	    (plus Slack) while a controller process is up, through memory swaps, closure and agent work.

// simConfig is the harness's bound (runtime/harness_integration_test.go) with the restart allowance a real
// Pod relaunch needs. Settle is zero: the scenarios wait for every outcome themselves before checking.
var simConfig = sim.Config{GapFactor: 3, Slack: 12 * time.Second, Restart: 90 * time.Second}

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

type openedAt struct {
	ID string
	At time.Time
}

// openedIncidents are the incidents the controller dispatched, from its own log, with the time of the line.
func (w *World) openedIncidents() []openedAt {
	seen := map[string]bool{}
	var out []openedAt
	for _, line := range w.ControllerLines() {
		if m := incidentPattern.FindStringSubmatch(line.Text); m != nil && !seen[m[1]] {
			seen[m[1]] = true
			out = append(out, openedAt{m[1], line.At})
		}
	}
	return out
}

// OpenedIncidents are the incident IDs the controller dispatched, from its own log.
func (w *World) OpenedIncidents() []string {
	var ids []string
	for _, opened := range w.openedIncidents() {
		ids = append(ids, opened.ID)
	}
	return ids
}

// Quiesce marks the instant after which the run injects no more faults. Call it before the final settle.
func (w *World) Quiesce() { w.quiesced = time.Now() }

// logInput is the process-level evidence of a run.
type logInput struct {
	Passes   []Iteration       // controller evaluation passes, oldest Pod first
	Pods     []string          // controller Pods, oldest first
	Running  map[string]bool   // Pods still running
	Killed   map[string]bool   // Pods the run killed on purpose
	Specs    []DetectorSpec    // detectors in accepted memory
	Opened   []openedAt        // incidents the controller dispatched
	Outcomes map[string]string // incident -> outcome classification, for incidents with an outcome document
	Windows  []Window          // deliberate faults that excuse detection
	Quiesced time.Time
	End      time.Time
}

// simLog maps the evidence onto the harness's event log:
//   - a detector in accepted memory is Activated from the first pass (or its commit, if later) and each
//     listing in a pass is an Evaluated;
//   - the gap between a Pod's last pass and the next Pod's first pass, and every declared fault window,
//     is a ControllerDown/ControllerUp interval; a Pod that is gone and was not killed is a ControllerExited;
//   - an incident is Opened when the controller logs its dispatch; its outcome document is an Outcome at
//     End (a document exists at the head of accepted memory when the evidence is read).
func simLog(in logInput) sim.Log {
	if len(in.Passes) == 0 {
		return sim.Log{sim.FaultsQuiesced{At: in.Quiesced}, sim.RunEnded{At: in.End}}
	}
	start := in.Passes[0].At
	for _, pass := range in.Passes {
		if pass.At.Before(start) {
			start = pass.At
		}
	}
	type entry struct {
		at    time.Time
		order int
		sim.Entry
	}
	var entries []entry
	add := func(order int, e sim.Entry) { entries = append(entries, entry{e.Time(), order, e}) }
	times := map[time.Time]bool{start: true}
	for _, spec := range in.Specs {
		if spec.AddedAt.After(start) {
			times[spec.AddedAt] = true
		}
	}
	for at := range times {
		var decls []sim.DetectorDecl
		for _, spec := range in.Specs {
			if !spec.AddedAt.After(at) {
				decls = append(decls, sim.DetectorDecl{ID: spec.ID, Interval: spec.Interval})
			}
		}
		add(0, sim.Activated{At: at, Revision: at.Format(time.RFC3339), Detectors: decls})
	}
	for _, pass := range in.Passes {
		for _, id := range pass.Detectors {
			add(1, sim.Evaluated{At: pass.At, Detector: id})
		}
	}
	// Downtime: Pod gaps and declared windows, merged so ControllerDown/Up pair up.
	type span struct{ from, to time.Time }
	var spans []span
	byPod := map[string][]Iteration{}
	for _, pass := range in.Passes {
		byPod[pass.Pod] = append(byPod[pass.Pod], pass)
	}
	var live []string
	for _, pod := range in.Pods {
		if len(byPod[pod]) > 0 {
			live = append(live, pod)
		}
	}
	for i, pod := range live {
		passes := byPod[pod]
		last := passes[len(passes)-1].At
		switch {
		case i+1 < len(live):
			spans = append(spans, span{last, byPod[live[i+1]][0].At})
		case !in.Running[pod]:
			spans = append(spans, span{last, in.End})
		}
		if i+1 < len(live) || !in.Running[pod] {
			if !in.Killed[pod] {
				add(2, sim.ControllerExited{At: last, Reason: "Pod " + pod + " ended and the run did not kill it"})
			}
		}
	}
	for _, window := range in.Windows {
		spans = append(spans, span{window.From, window.To})
	}
	sort.Slice(spans, func(i, j int) bool { return spans[i].from.Before(spans[j].from) })
	var merged []span
	for _, s := range spans {
		if n := len(merged); n > 0 && !s.from.After(merged[n-1].to) {
			if s.to.After(merged[n-1].to) {
				merged[n-1].to = s.to
			}
			continue
		}
		merged = append(merged, s)
	}
	for _, s := range merged {
		add(2, sim.ControllerDown{At: s.from})
		if s.to.Before(in.End) {
			add(3, sim.ControllerUp{At: s.to})
		}
	}
	ids := map[string]bool{}
	for _, opened := range in.Opened {
		ids[opened.ID] = true
		add(2, sim.Opened{At: opened.At, Incident: opened.ID})
	}
	for id, class := range in.Outcomes {
		if !ids[id] {
			continue
		}
		result := sim.Closed
		if class == "failed" || class == "failure" {
			result = sim.Failed
		}
		add(4, sim.Outcome{At: in.End, Incident: id, Result: result})
	}
	add(5, sim.FaultsQuiesced{At: in.Quiesced})
	add(6, sim.RunEnded{At: in.End})
	sort.SliceStable(entries, func(i, j int) bool {
		if !entries[i].at.Equal(entries[j].at) {
			return entries[i].at.Before(entries[j].at)
		}
		return entries[i].order < entries[j].order
	})
	log := make(sim.Log, len(entries))
	for i, e := range entries {
		log[i] = e.Entry
	}
	return log
}

// storageViolations check the outcome documents' physical layout, which only the storage edge knows:
// one document per incident at .sdo/outcomes/<sha256(incident_id)>.json, with a classification.
func (w *World) storageViolations() ([]string, map[string]string) {
	var violations []string
	classes := map[string]string{}
	for _, outcome := range w.Outcomes() {
		id, ok := outcome.Raw["incident_id"].(string)
		if !ok {
			continue
		}
		sum := sha256.Sum256([]byte(id))
		if want := ".sdo/outcomes/" + hex.EncodeToString(sum[:]) + ".json"; outcome.File != want {
			violations = append(violations, fmt.Sprintf("outcome for %s is stored at %s, want %s", id, outcome.File, want))
		}
		if _, dup := classes[id]; dup {
			violations = append(violations, fmt.Sprintf("incident %s has two outcome documents", id))
		}
		class, _ := outcome.Raw["classification"].(string)
		if class == "" {
			violations = append(violations, fmt.Sprintf("incident %s outcome has no classification", id))
		}
		classes[id] = class
	}
	return violations, classes
}

// Log is the run's evidence as the harness's event log. Quiesce must have been called.
func (w *World) Log(exempt map[string]bool, windows ...Window) (sim.Log, []string) {
	if w.quiesced.IsZero() {
		w.T.Fatal("Quiesce was not called before the property checks")
	}
	violations, classes := w.storageViolations()
	in := logInput{Passes: w.Iterations(), Specs: w.Detectors(), Opened: w.openedIncidents(), Outcomes: classes,
		Windows: windows, Quiesced: w.quiesced, End: time.Now(), Running: map[string]bool{}, Killed: w.killed}
	in.Pods = w.ControllerPods()
	for _, pod := range in.Pods {
		in.Running[pod] = w.podRunning(pod)
	}
	return simLog(in), violations
}

// CheckProperties runs sim.Check, both properties, over the run. exempt names detectors the run broke on purpose
// (their own cadence is not checked; everyone else's is); windows are deliberate faults.
func (w *World) CheckProperties(exempt map[string]bool, windows ...Window) []string {
	log, violations := w.Log(exempt, windows...)
	for _, err := range sim.Check(log, w.simConfig(exempt)) {
		violations = append(violations, err.Error())
	}
	return violations
}

// CheckEveryIncidentEnds is the reliability property alone, for runs whose faults (API errors, a hung agent)
// make detector cadence uninformative.
func (w *World) CheckEveryIncidentEnds() []string {
	log, violations := w.Log(nil)
	if err := sim.EveryIncidentEnds(log, w.simConfig(nil)); err != nil {
		violations = append(violations, err.Error())
	}
	return violations
}

func (w *World) simConfig(exempt map[string]bool) sim.Config {
	cfg := simConfig
	cfg.Hung = exempt
	return cfg
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

func (w *World) podRunning(name string) bool {
	pod, err := w.Cluster.Client.CoreV1().Pods(ControlNamespace).Get(context.Background(), name, metav1.GetOptions{})
	return err == nil && pod.Status.Phase == corev1.PodRunning
}

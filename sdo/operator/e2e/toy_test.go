//go:build integration && controller_e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// python runs a snippet in the repository's uv environment.
func (w *World) python(code string, args ...string) string {
	w.T.Helper()
	cmd := exec.Command(filepath.Join(w.Root, ".venv/bin/python"), append([]string{"-c", code}, args...)...)
	cmd.Dir = w.Root
	cmd.Env = append(os.Environ(), "PYTHONPATH="+w.Root, "PATH="+filepath.Dir(mustLookPath("kubectl"))+":"+os.Getenv("PATH"))
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	if err := cmd.Run(); err != nil {
		w.T.Fatalf("python: %v\n%s", err, errOut.String())
	}
	return strings.TrimSpace(out.String())
}

func mustLookPath(name string) string {
	path, err := exec.LookPath(name)
	if err != nil {
		panic(err)
	}
	return path
}

// Inject applies a toy fault with the toy lane's own fault definitions (apps/toy/faults.py).
func (w *World) Inject(problem string) {
	w.T.Helper()
	w.python("import sys; from apps.toy.faults import fault_spec; fault_spec(sys.argv[1]).inject(sys.argv[2], sys.argv[3])", problem, w.Cluster.Kubeconfig, AppNamespace)
}

// Repaired is the toy lane's repair oracle for a fault.
func (w *World) Repaired(problem string) bool {
	w.T.Helper()
	return w.python("import sys; from apps.toy.faults import fault_spec; print(fault_spec(sys.argv[1]).oracle(sys.argv[2], sys.argv[3]))", problem, w.Cluster.Kubeconfig, AppNamespace) == "True"
}

// AcceptedRepo is the controller's accepted-memory repository on the claim directory.
func (w *World) AcceptedRepo() string {
	return filepath.Join(w.Kubelet.Run.PVCDir(ControlNamespace, RepositoryPVC), "application")
}

// AcceptedLog lists accepted-memory commits, newest first.
func (w *World) AcceptedLog() []string {
	out, err := exec.Command("git", "-C", w.AcceptedRepo(), "log", "--format=%h %s").Output()
	if err != nil {
		return nil
	}
	return strings.Split(strings.TrimSpace(string(out)), "\n")
}

// Outcome is one incident outcome document from accepted memory (only the fields the run checks).
type Outcome struct {
	File string
	Raw  map[string]any
}

// Outcomes reads .sdo/outcomes from the accepted branch head.
func (w *World) Outcomes() []Outcome {
	repo := w.AcceptedRepo()
	names, err := exec.Command("git", "-C", repo, "ls-tree", "-r", "--name-only", "HEAD", ".sdo/outcomes").Output()
	if err != nil {
		return nil
	}
	var out []Outcome
	for _, name := range strings.Fields(string(names)) {
		data, err := exec.Command("git", "-C", repo, "show", "HEAD:"+name).Output()
		if err != nil {
			continue
		}
		var doc map[string]any
		if json.Unmarshal(data, &doc) == nil {
			out = append(out, Outcome{File: name, Raw: doc})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].File < out[j].File })
	return out
}

// ControllerPods lists controller Pods oldest first.
func (w *World) ControllerPods() []string {
	pods, err := w.Cluster.Client.CoreV1().Pods(ControlNamespace).List(context.Background(), metav1.ListOptions{LabelSelector: "app.kubernetes.io/name=sdo-controller"})
	if err != nil {
		return nil
	}
	sort.Slice(pods.Items, func(i, j int) bool {
		return pods.Items[i].CreationTimestamp.Before(&pods.Items[j].CreationTimestamp) || (pods.Items[i].CreationTimestamp.Equal(&pods.Items[j].CreationTimestamp) && pods.Items[i].Name < pods.Items[j].Name)
	})
	names := make([]string, len(pods.Items))
	for i, pod := range pods.Items {
		names[i] = pod.Name
	}
	return names
}

// ControllerLines is every controller container's output across restarts, oldest first.
func (w *World) ControllerLines() []LogLine {
	var lines []LogLine
	for _, name := range w.ControllerPods() {
		lines = append(lines, w.Kubelet.Run.Lines(ControlNamespace, name, "controller")...)
	}
	sort.SliceStable(lines, func(i, j int) bool { return lines[i].At.Before(lines[j].At) })
	return lines
}

// Iteration is one controller evaluation pass, as the controller prints it.
type Iteration struct {
	Pod       string
	At        time.Time
	N         int
	Detectors []string
	Findings  int
}

// Iterations parses the controller's per-pass records, oldest Pod first.
func (w *World) Iterations() []Iteration {
	var out []Iteration
	for _, pod := range w.ControllerPods() {
		for _, line := range w.Kubelet.Run.Lines(ControlNamespace, pod, "controller") {
			if !strings.HasPrefix(line.Text, "{") || !strings.Contains(line.Text, "controller_iteration") {
				continue
			}
			var record struct {
				N         int               `json:"controller_iteration"`
				Detectors []string          `json:"evaluated_detectors"`
				Findings  []json.RawMessage `json:"findings"`
			}
			if json.Unmarshal([]byte(line.Text), &record) == nil {
				out = append(out, Iteration{Pod: pod, At: line.At, N: record.N, Detectors: record.Detectors, Findings: len(record.Findings)})
			}
		}
	}
	return out
}

// WaitBaseline waits until the controller has evaluated a clear pass.
func (w *World) WaitBaseline(limit time.Duration) {
	w.T.Helper()
	w.waitFor(limit, "a clear controller pass", func() bool {
		its := w.Iterations()
		return len(its) > 0 && its[len(its)-1].Findings == 0
	})
}

// Activation is a memory-host activation record the controller printed.
type Activation struct {
	At       time.Time
	Pod      string
	Revision string
}

// Activations lists memory-host activations in the controller log.
func (w *World) Activations() []Activation {
	var out []Activation
	for _, pod := range w.ControllerPods() {
		for _, line := range w.Kubelet.Run.Lines(ControlNamespace, pod, "controller") {
			if !strings.HasPrefix(line.Text, "{") || !strings.Contains(line.Text, "memory_host_activation") {
				continue
			}
			var record struct {
				Revision string `json:"memory_host_activation"`
			}
			if json.Unmarshal([]byte(line.Text), &record) == nil {
				out = append(out, Activation{At: line.At, Pod: pod, Revision: record.Revision})
			}
		}
	}
	return out
}

// ResponderJobs lists the names of responder Jobs the controller created for an incident.
func (w *World) ResponderJobs(incident string) []string {
	jobs, err := w.Cluster.Client.BatchV1().Jobs(ControlNamespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return nil
	}
	var names []string
	for _, job := range jobs.Items {
		if strings.HasPrefix(job.Name, "sdo-"+incident+"-") {
			names = append(names, job.Name)
		}
	}
	sort.Strings(names)
	return names
}

// Incident returns the outcome for an incident ID, if accepted memory has one.
func (w *World) Incident(id string) (Outcome, bool) {
	for _, o := range w.Outcomes() {
		if o.Raw["incident_id"] == id {
			return o, true
		}
	}
	return Outcome{}, false
}

// ActiveWorkers lists worker Jobs (responder, reflection, judge) that are still running.
func (w *World) ActiveWorkers() []string {
	jobs, err := w.Cluster.Client.BatchV1().Jobs(ControlNamespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return nil
	}
	var names []string
	for _, job := range jobs.Items {
		if job.Name != "sdo-controller-run" && job.Status.Active > 0 && job.Spec.Template.Labels["sdo.dev/standby"] != "true" {
			names = append(names, job.Name)
		}
	}
	return names
}

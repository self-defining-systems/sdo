package program

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"

	"sdo.dev/operator/sdk"
)

// ProcedureRun is what a run of an application procedure records on its
// attempt: the procedure, its effect class and the bound parameter values.
type ProcedureRun struct {
	ID     string         `json:"id"`
	Effect string         `json:"effect"`
	Params map[string]any `json:"params,omitempty"`
}

// procedureHeader opens the first line after the shebang of a procedure
// script, and the Reason of a re-created Job, followed by the run as JSON.
const procedureHeader = "# sdo-procedure "

// ProcedureExitMarker is the last line a procedure script prints: the
// procedure's exit code.
const ProcedureExitMarker = "sdo-procedure-exit"

// RunProcedure runs a procedure the application ships, from the accepted map.
// It binds and validates params against the procedure's typed parameters and
// returns:
//
//   - for a procedure in a component, a RunScript that runs the bound command
//     in the declared container of one Pod of the component's workload and
//     exits with the command's exit code;
//   - for a Job procedure, a RunScript that deletes and re-creates the live Job
//     from its own template and waits for it to complete, or CreateFromSource
//     when the Job is no longer live.
//
// Both run through the RunScript executor (or the typed source path) with the
// same budgets, timeouts, output capture and record/replay, and the run's
// effect class is recorded on the attempt. The script's authority is the
// accepted architecture entry, so it needs no original receipt.
func RunProcedure(view ReadView, id string, params map[string]any) (Action, error) {
	m, err := view.AppMap()
	if err != nil {
		return nil, err
	}
	p, run, err := bindProcedure(m, id, params)
	if err != nil {
		return nil, err
	}
	if p.Job != "" {
		ref := jobRef(p, view.Namespace())
		if live, readErr := view.Object(ref); readErr != nil || live == nil {
			return CreateFromSource{Ref: ref, Reason: procedureHeader + run.text()}, nil
		}
	}
	script, err := renderProcedure(m, p, run)
	if err != nil {
		return nil, err
	}
	return RunScript{Script: script, Timeout: p.Timeout, Reason: "application procedure " + p.ID + " (effect " + p.Effect + ")"}, nil
}

// ObserveProcedure runs a check procedure as Diagnose evidence; 0 is a pass.
// It runs the bound command in the declared container of one running Pod of
// the component's workload (ready first, then by name) through
// ReadView.Observe and returns the exit code. Only a check procedure is a read.
func ObserveProcedure(view ReadView, id string, params map[string]any) (int32, error) {
	m, err := view.AppMap()
	if err != nil {
		return 0, err
	}
	p, run, err := bindProcedure(m, id, params)
	if err != nil {
		return 0, err
	}
	if p.Effect != ProcedureCheck || p.Job != "" {
		return 0, fmt.Errorf("procedure %s is %s; only a check procedure in a component is a Diagnose read", p.ID, p.Effect)
	}
	c, _ := m.Component(p.Component)
	pod, err := runningPod(view, c)
	if err != nil {
		return 0, err
	}
	argv, err := p.Bind(run.Params)
	if err != nil {
		return 0, err
	}
	return view.Observe(ExternalOperation{Exec: &Command{Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name}, Container: p.Container, Args: argv}})
}

// ProcedureOf returns the procedure run an action carries: a RunScript or a
// CreateFromSource that RunProcedure built.
func ProcedureOf(action Action) (ProcedureRun, bool) {
	var text string
	switch a := action.(type) {
	case RunScript:
		text = a.Script
	case *RunScript:
		if a != nil {
			text = a.Script
		}
	case CreateFromSource:
		text = a.Reason
	case *CreateFromSource:
		if a != nil {
			text = a.Reason
		}
	}
	for _, line := range strings.SplitN(text, "\n", 3) {
		if rest, ok := strings.CutPrefix(line, procedureHeader); ok {
			var run ProcedureRun
			if json.Unmarshal([]byte(rest), &run) == nil && run.ID != "" {
				return run, true
			}
		}
	}
	return ProcedureRun{}, false
}

// AcceptedProcedureScript reports whether script is exactly what RunProcedure
// renders for a procedure of the accepted map, so that the map entry is its
// authority.
func AcceptedProcedureScript(m AppMap, script string) bool {
	run, ok := ProcedureOf(RunScript{Script: script})
	if !ok {
		return false
	}
	p, bound, err := bindProcedure(m, run.ID, run.Params)
	if err != nil {
		return false
	}
	rendered, err := renderProcedure(m, p, bound)
	return err == nil && rendered == script
}

func bindProcedure(m AppMap, id string, params map[string]any) (Procedure, ProcedureRun, error) {
	p, ok := m.Procedure(id)
	if !ok {
		return Procedure{}, ProcedureRun{}, fmt.Errorf("the application map has no procedure %q", id)
	}
	if _, err := p.Bind(params); err != nil {
		return Procedure{}, ProcedureRun{}, err
	}
	if p.Component != "" {
		if _, ok := m.Component(p.Component); !ok {
			return Procedure{}, ProcedureRun{}, fmt.Errorf("procedure %s: the application map has no component %q", p.ID, p.Component)
		}
	}
	run := ProcedureRun{ID: p.ID, Effect: p.Effect}
	if len(params) > 0 {
		run.Params = map[string]any{}
		for name, value := range params {
			run.Params[name] = value
		}
	}
	return p, run, nil
}

func (r ProcedureRun) text() string {
	data, _ := json.Marshal(r)
	return string(data)
}

func jobRef(p Procedure, namespace string) sdk.ObjectRef {
	return sdk.ObjectRef{APIVersion: "batch/v1", Kind: "Job", Namespace: namespace, Name: strings.TrimPrefix(p.Job, "Job/")}
}

// shellQuote quotes one argument for bash: single quotes, each embedded single
// quote closed, escaped and reopened.
func shellQuote(text string) string {
	return "'" + strings.ReplaceAll(text, "'", `'\''`) + "'"
}

// jobRebuild removes the fields the API server owns from a live Job, so its
// own template can be created again under the same name.
const jobRebuild = `{"metadata":{"uid":null,"resourceVersion":null,"creationTimestamp":null,"generation":null,"managedFields":null,"ownerReferences":null},"status":null,"spec":{"selector":null,"manualSelector":null,"template":{"metadata":{"labels":{"controller-uid":null,"batch.kubernetes.io/controller-uid":null,"job-name":null,"batch.kubernetes.io/job-name":null}}}}}`

// renderProcedure is the deterministic script of a bound procedure run. The
// run's JSON is stable (sorted keys), so equal runs render equal bytes.
func renderProcedure(m AppMap, p Procedure, run ProcedureRun) (string, error) {
	timeout := p.Timeout
	if timeout == 0 {
		timeout = DefaultScriptTimeout
	}
	var b strings.Builder
	b.WriteString("#!/bin/bash\n" + procedureHeader + run.text() + "\n")
	b.WriteString("set -u\nns=\"$SDO_SCRIPT_NAMESPACE\"\n")
	if p.Job != "" {
		name := shellQuote(strings.TrimPrefix(p.Job, "Job/"))
		b.WriteString("work=$(mktemp -d) || exit 1\n")
		fmt.Fprintf(&b, "kubectl get job -n \"$ns\" %s -o json > \"$work/live.json\" || exit 1\n", name)
		fmt.Fprintf(&b, "kubectl patch --local -f \"$work/live.json\" --type=merge -p %s -o json > \"$work/job.json\" || exit 1\n", shellQuote(jobRebuild))
		fmt.Fprintf(&b, "kubectl delete job -n \"$ns\" %s --cascade=background --wait=true || exit 1\n", name)
		b.WriteString("kubectl create -f \"$work/job.json\" || exit 1\n")
		fmt.Fprintf(&b, "kubectl wait -n \"$ns\" --for=condition=complete --timeout=%ds job/%s\n", int(timeout.Seconds()), name)
	} else {
		c, ok := m.Component(p.Component)
		if !ok {
			return "", fmt.Errorf("procedure %s: the application map has no component %q", p.ID, p.Component)
		}
		kind, name, _ := strings.Cut(c.Workload, "/")
		argv, err := p.Bind(run.Params)
		if err != nil {
			return "", err
		}
		quoted := make([]string, len(argv))
		for i, arg := range argv {
			quoted[i] = shellQuote(arg)
		}
		fmt.Fprintf(&b, "kubectl exec -n \"$ns\" %s -c %s -- %s\n", shellQuote(strings.ToLower(kind)+"/"+name), shellQuote(p.Container), strings.Join(quoted, " "))
	}
	b.WriteString("code=$?\necho \"" + ProcedureExitMarker + " $code\"\nexit $code\n")
	return b.String(), nil
}

// runningPod is one running Pod of the component's workload, ready first and
// then by name.
func runningPod(view ReadView, c AppComponent) (corev1.Pod, error) {
	var pods []corev1.Pod
	for _, pod := range view.Pods() {
		if pod.Status.Phase == corev1.PodRunning && pod.DeletionTimestamp == nil &&
			WorkloadOf(view, sdk.ObjectRef{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name}) == c.Workload {
			pods = append(pods, pod)
		}
	}
	if len(pods) == 0 {
		return corev1.Pod{}, fmt.Errorf("component %s has no running Pod", c.Name)
	}
	sort.SliceStable(pods, func(i, j int) bool {
		if ready(pods[i]) != ready(pods[j]) {
			return ready(pods[i])
		}
		return pods[i].Name < pods[j].Name
	})
	return pods[0], nil
}

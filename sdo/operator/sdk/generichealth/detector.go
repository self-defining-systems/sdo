// Package generichealth implements relation-based, application-independent health checks.
// Snapshot decisions use the observation clock, never wall time. Cause findings
// warn; workload unavailability is critical. Missing optional inputs are errors.
package generichealth

import (
	"context"
	"fmt"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"time"
)

// ProgressWindow allows ordinary controller convergence before reporting faults.
const ProgressWindow = 10 * time.Minute

type detector struct {
	spec sdk.DetectorSpec
	kind string
}

func (d detector) Spec() sdk.DetectorSpec { return d.spec }

// NewWiring checks selectors, ports, references, storage, ingress and DNS aliases.
func NewWiring(s sdk.DetectorSpec) sdk.Detector { return detector{s, "wiring"} }

// NewWorkloads checks StatefulSets, DaemonSets, Jobs and CronJobs.
func NewWorkloads(s sdk.DetectorSpec) sdk.Detector { return detector{s, "workloads"} }

// NewWarnings checks persistent recent warning events on extant application objects.
func NewWarnings(s sdk.DetectorSpec) sdk.Detector { return detector{s, "warnings"} }

// NewNodes checks only nodes hosting application pods.
func NewNodes(s sdk.DetectorSpec) sdk.Detector { return detector{s, "nodes"} }

// Watches returns all inputs used by the named detector, shared by installation.
func Watches(kind string) []sdk.WatchKind {
	var result []sdk.WatchKind
	add := func(api string, kinds ...string) {
		for _, k := range kinds {
			result = append(result, sdk.WatchKind{APIVersion: api, Kind: k})
		}
	}
	switch kind {
	case "wiring":
		add("v1", "Service", "Pod", "ConfigMap", "Secret", "PersistentVolumeClaim", "ServiceAccount")
		add("apps/v1", "Deployment", "ReplicaSet", "StatefulSet", "DaemonSet")
		add("batch/v1", "Job", "CronJob")
		add("networking.k8s.io/v1", "Ingress", "NetworkPolicy")
	case "workloads":
		add("apps/v1", "StatefulSet", "DaemonSet")
		add("batch/v1", "Job", "CronJob")
		add("v1", "Pod")
	case "warnings":
		add("v1", "Event", "Pod", "Service", "PersistentVolumeClaim")
		add("apps/v1", "Deployment", "ReplicaSet", "StatefulSet", "DaemonSet")
		add("batch/v1", "Job", "CronJob")
	case "nodes":
		add("v1", "Node", "Pod")
	case "saturation":
		add("sdo.dev/v1alpha1", "ResourceUsage")
	case "crashes":
		add("sdo.dev/v1alpha1", "Logs")
	}
	return result
}

type evaluation struct {
	ctx      sdk.DetectionContext
	now      time.Time
	findings []sdk.Finding
}

func (e *evaluation) emit(rule string, r sdk.ObjectRef, detail string, critical bool) {
	severity := sdk.SeverityWarning
	if critical {
		severity = sdk.SeverityCritical
	}
	e.findings = append(e.findings, sdk.Finding{RuleID: rule, Status: sdk.FindingActive, Severity: severity, Summary: rule + ": " + r.Kind + "/" + r.Name, Evidence: detail, PrimaryResource: r, ParameterBindings: map[string]sdk.ObjectRef{"affected_resource": r}, Fingerprint: rule + "/" + r.Namespace + "/" + r.Kind + "/" + r.Name})
}
func (d detector) Detect(_ context.Context, ctx sdk.DetectionContext) ([]sdk.Finding, error) {
	e := evaluation{ctx: ctx, now: sdk.ObservationTime(ctx)}
	switch d.kind {
	case "wiring":
		e.wiring()
	case "workloads":
		e.workloads()
	case "warnings":
		e.warnings()
	case "nodes":
		e.nodes()
	}
	return finish(e.findings, d.spec), nil
}
func ref(kind, api string, o meta.Object) sdk.ObjectRef { return sdk.ObjectRefFrom(kind, api, o) }
func (e *evaluation) old(t meta.Time) bool {
	return !t.IsZero() && !e.now.IsZero() && e.now.Sub(t.Time) > ProgressWindow
}
func (e *evaluation) nodes() {
	used := map[string]bool{}
	for _, p := range e.ctx.Pods() {
		if p.Namespace == e.ctx.Namespace() && p.DeletionTimestamp == nil && p.Status.Phase != core.PodSucceeded && p.Status.Phase != core.PodFailed {
			used[p.Spec.NodeName] = true
		}
	}
	for _, n := range e.ctx.Nodes() {
		if !used[n.Name] {
			continue
		}
		for _, c := range n.Status.Conditions {
			bad := c.Type == core.NodeReady && c.Status != core.ConditionTrue || (c.Type == core.NodeMemoryPressure || c.Type == core.NodeDiskPressure || c.Type == core.NodePIDPressure) && c.Status == core.ConditionTrue
			if bad {
				e.emit("node-"+string(c.Type), ref("Node", "v1", &n), fmt.Sprintf("condition=%s status=%s reason=%s message=%s; node hosts application pods", c.Type, c.Status, c.Reason, c.Message), false)
			}
		}
	}
}
func (e *evaluation) warnings() {
	reasons := map[string]bool{"FailedMount": true, "FailedAttachVolume": true, "FailedScheduling": true, "BackOff": true, "Unhealthy": true, "Evicted": true, "OOMKilling": true, "FailedCreate": true, "FailedCreatePodSandBox": true}
	objects := map[string]bool{}
	for _, t := range e.templates() {
		objects[t.r.Kind+"/"+t.r.Name] = !t.rolling
	}
	for _, p := range e.ctx.Pods() {
		if p.Namespace == e.ctx.Namespace() && p.DeletionTimestamp == nil && !e.podRolling(p) {
			objects["Pod/"+p.Name] = true
		}
	}
	for _, s := range e.ctx.Services() {
		if s.Namespace == e.ctx.Namespace() {
			objects["Service/"+s.Name] = true
		}
	}
	for _, p := range e.ctx.PersistentVolumeClaims() {
		if p.Namespace == e.ctx.Namespace() {
			objects["PersistentVolumeClaim/"+p.Name] = true
		}
	}
	for _, v := range e.ctx.Events() {
		r := v.InvolvedObject
		if r.Namespace != e.ctx.Namespace() || !objects[r.Kind+"/"+r.Name] || v.Type != "Warning" || !reasons[v.Reason] {
			continue
		}
		first, last, count := v.FirstTimestamp.Time, v.LastTimestamp.Time, v.Count
		if !v.EventTime.IsZero() {
			first = v.EventTime.Time
		}
		if v.Series != nil {
			last = v.Series.LastObservedTime.Time
			count = v.Series.Count
		}
		// A recovered pod's old failures must not remain active merely because
		// Kubernetes retains the Event object after recovery.
		recovered := false
		if r.Kind == "Pod" {
			for _, pod := range e.ctx.Pods() {
				if pod.Namespace != r.Namespace || pod.Name != r.Name {
					continue
				}
				for _, c := range pod.Status.Conditions {
					if c.Type == core.PodReady && c.Status == core.ConditionTrue && !c.LastTransitionTime.IsZero() && !last.After(c.LastTransitionTime.Time) {
						recovered = true
					}
				}
			}
		}
		if recovered {
			continue
		}
		if last.IsZero() {
			last = first
		}
		age := e.now.Sub(last)
		if count < 2 || first.IsZero() || last.Sub(first) < time.Minute || age < 0 || age > 2*time.Minute {
			continue
		}
		e.emit("warning-"+v.Reason, sdk.ObjectRef{APIVersion: r.APIVersion, Kind: r.Kind, Namespace: r.Namespace, Name: r.Name}, fmt.Sprintf("reason=%s count=%d first=%s last=%s message=%s", v.Reason, count, first, last, v.Message), false)
	}
}

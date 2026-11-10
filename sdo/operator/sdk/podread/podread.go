// Package podread exposes bounded command output from inside running containers
// to deterministic detectors. A detector declares the command and which Pods
// hold the role; the runtime runs it on the declared interval and freezes the
// output into the snapshot. No exec occurs in detectors.
package podread

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sdo.dev/operator/sdk"
)

// Limits bound declarations, target fan-out and retained output, so a read can
// neither load the application nor stall the controller's evaluation loop.
// MaxConcurrent and CollectionBudget apply across every query of a cycle.
const (
	MaxQueries       = 8
	MaxTargets       = 16
	MaxArgs          = 32
	MaxArgBytes      = 1024
	MaxIDBytes       = 128
	LimitBytes       = 4 * 1024
	MaxConcurrent    = 4
	MinInterval      = 30 * time.Second
	ExecTimeout      = 3 * time.Second
	CollectionBudget = 5 * time.Second
)

// Target selects Pods by role from observed objects in the application
// namespace, never by Pod name. Every label must match, Workload ("Kind/name"
// of a Deployment, StatefulSet, DaemonSet or ReplicaSet, as the application map
// names it) must control the Pod, and Image (an RE2 over the container image)
// selects the container. Without Image the Pod's first container is read. Pods
// that are terminating or finished are not targets.
type Target struct {
	Labels   map[string]string `json:"labels,omitempty"`
	Workload string            `json:"workload,omitempty"`
	Image    string            `json:"image,omitempty"`
}

var workloadKinds = map[string]bool{"Deployment": true, "StatefulSet": true, "DaemonSet": true, "ReplicaSet": true}

// Validate requires a role: a target that selects every Pod is refused.
func (t Target) Validate() error {
	if len(t.Labels) == 0 && strings.TrimSpace(t.Image) == "" && t.Workload == "" {
		return fmt.Errorf("target requires labels, a workload or an image pattern")
	}
	if t.Workload != "" {
		kind, name, ok := strings.Cut(t.Workload, "/")
		if !ok || !workloadKinds[kind] || name == "" || strings.Contains(name, "/") {
			return fmt.Errorf("target workload %q is not Kind/name of a Deployment, StatefulSet, DaemonSet or ReplicaSet", t.Workload)
		}
	}
	if t.Image != "" {
		if _, err := regexp.Compile(t.Image); err != nil {
			return fmt.Errorf("target image: %w", err)
		}
	}
	return nil
}

// Select returns the container to read in pod, or false when pod does not hold
// the role. The target must have passed Validate.
func (t Target) Select(pod corev1.Pod) (string, bool) {
	if pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed || len(pod.Spec.Containers) == 0 {
		return "", false
	}
	for key, value := range t.Labels {
		if got, ok := pod.Labels[key]; !ok || got != value {
			return "", false
		}
	}
	if t.Workload != "" && !controlledBy(pod, t.Workload) {
		return "", false
	}
	if t.Image == "" {
		return pod.Spec.Containers[0].Name, true
	}
	image := regexp.MustCompile(t.Image)
	for _, container := range pod.Spec.Containers {
		if image.MatchString(container.Image) {
			return container.Name, true
		}
	}
	return "", false
}

// controlledBy reports whether workload ("Kind/name") controls pod. A
// Deployment controls the Pods of its ReplicaSets, which are named after it and
// the Pod's template hash.
func controlledBy(pod corev1.Pod, workload string) bool {
	kind, name, _ := strings.Cut(workload, "/")
	for _, owner := range pod.OwnerReferences {
		if owner.Controller == nil || !*owner.Controller {
			continue
		}
		if owner.Kind == kind && owner.Name == name {
			return true
		}
		hash := pod.Labels["pod-template-hash"]
		return kind == "Deployment" && owner.Kind == "ReplicaSet" && hash != "" && owner.Name == name+"-"+hash
	}
	return false
}

// Query declares one read-only command and its role. Args run without a shell;
// use "sh", "-c" for a pipeline. Read only the field you judge: the output is
// recorded in captures, so never select credential values, and resolve
// credentials inside the container through its environment.
type Query struct {
	ID       string        `json:"id"`
	Target   Target        `json:"target"`
	Args     []string      `json:"args"`
	Interval time.Duration `json:"interval"`
}

// Consumer declares a detector's pod reads. It must watch v1 Pod.
type Consumer interface{ PodReads() []Query }

// Validate checks a query's identifiers, role, command and bounds.
func (q Query) Validate() error {
	if q.ID == "" || len(q.ID) > MaxIDBytes || strings.TrimSpace(q.ID) != q.ID {
		return fmt.Errorf("invalid pod read id %q", q.ID)
	}
	if err := q.Target.Validate(); err != nil {
		return fmt.Errorf("pod read %q: %w", q.ID, err)
	}
	if len(q.Args) == 0 || len(q.Args) > MaxArgs || q.Args[0] == "" {
		return fmt.Errorf("pod read %q requires an executable and at most %d arguments", q.ID, MaxArgs)
	}
	for _, arg := range q.Args {
		if len(arg) > MaxArgBytes {
			return fmt.Errorf("pod read %q argument exceeds %d bytes", q.ID, MaxArgBytes)
		}
	}
	if q.Interval < MinInterval {
		return fmt.Errorf("pod read %q interval %s is below the minimum %s", q.ID, q.Interval, MinInterval)
	}
	return nil
}

// ValidateQueries rejects malformed, duplicate or excessive declarations.
func ValidateQueries(qs []Query) error {
	if len(qs) > MaxQueries {
		return fmt.Errorf("pod reads exceed %d", MaxQueries)
	}
	seen := map[string]bool{}
	for _, q := range qs {
		if err := q.Validate(); err != nil {
			return err
		}
		if seen[q.ID] {
			return fmt.Errorf("duplicate pod read %q", q.ID)
		}
		seen[q.ID] = true
	}
	return nil
}

// ValidateDetector validates a consumer at registration. Targets come from the
// snapshot's Pods, which only a Pod watch keeps observed.
func ValidateDetector(d sdk.Detector) error {
	c, ok := d.(Consumer)
	if !ok || len(c.PodReads()) == 0 {
		return nil
	}
	if err := ValidateQueries(c.PodReads()); err != nil {
		return err
	}
	for _, watch := range d.Spec().Watches {
		if watch.APIVersion == "v1" && watch.Kind == "Pod" {
			return nil
		}
	}
	return fmt.Errorf("pod reads require a v1 Pod watch")
}

// Queries merges identical query IDs and rejects conflicting definitions.
func Queries(ds []sdk.Detector) ([]Query, error) {
	byID := map[string]Query{}
	for _, d := range ds {
		if err := ValidateDetector(d); err != nil {
			return nil, err
		}
		if c, ok := d.(Consumer); ok {
			for _, q := range c.PodReads() {
				if old, ok := byID[q.ID]; ok && !reflect.DeepEqual(old, q) {
					return nil, fmt.Errorf("conflicting pod read %q", q.ID)
				}
				byID[q.ID] = q
			}
		}
	}
	qs := make([]Query, 0, len(byID))
	for _, q := range byID {
		qs = append(qs, q)
	}
	sort.Slice(qs, func(i, j int) bool { return qs[i].ID < qs[j].ID })
	return qs, ValidateQueries(qs)
}

// LongestInterval is how stale a declared read can be at evaluation, so how long
// a repaired condition can keep reporting.
func LongestInterval(d sdk.Detector) time.Duration {
	var longest time.Duration
	if c, ok := d.(Consumer); ok {
		for _, q := range c.PodReads() {
			longest = max(longest, q.Interval)
		}
	}
	return longest
}

// Reading is one container's output. A nonzero ExitCode is a result the command
// reported; Error means the command gave no answer. Stdout is capped at
// LimitBytes and Truncated marks a prefix. Stderr is not collected.
type Reading struct {
	Namespace  string    `json:"namespace"`
	Pod        string    `json:"pod"`
	UID        string    `json:"uid,omitempty"`
	Container  string    `json:"container"`
	ObservedAt time.Time `json:"observed_at"`
	ExitCode   int32     `json:"exit_code"`
	Stdout     string    `json:"stdout,omitempty"`
	Truncated  bool      `json:"truncated,omitempty"`
	Error      string    `json:"error,omitempty"`
}

// Ref names the read Pod, for a finding's primary resource.
func (r Reading) Ref() sdk.ObjectRef {
	return sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: r.Namespace, Name: r.Pod}
}

// Result is a query's frozen readings; Error is neither health nor a finding.
// Baseline holds the complete readings of the same query at the controller's
// last promoted healthy baseline, one per Pod read then. It is empty until the
// application has been healthy once, and it is a reference, never a current
// read.
type Result struct {
	Query    Query     `json:"query"`
	Readings []Reading `json:"readings,omitempty"`
	Baseline []Reading `json:"baseline,omitempty"`
	Error    string    `json:"error,omitempty"`
}

// Source supplies frozen query results.
type Source interface {
	PodReadResult(string) (Result, bool)
}

// ResultSet lists every carried result, for snapshot capture.
type ResultSet interface {
	PodReadResults() map[string]Result
}

// ResultFrom reads a frozen result without I/O, including failed readings.
func ResultFrom(ctx sdk.DetectionContext, id string) (Result, bool) {
	if s, ok := ctx.(Source); ok {
		return s.PodReadResult(id)
	}
	return Result{}, false
}

// Require returns a result in which every reading is complete, or a detector
// error. A missing, failed, empty or truncated read cannot establish a clear.
func Require(ctx sdk.DetectionContext, id string) (Result, error) {
	r, ok := ResultFrom(ctx, id)
	if !ok {
		return r, fmt.Errorf("pod read %q unavailable", id)
	}
	if r.Error != "" {
		return r, fmt.Errorf("pod read %q: %s", id, r.Error)
	}
	if len(r.Readings) == 0 {
		return r, fmt.Errorf("pod read %q has no readings", id)
	}
	for _, reading := range r.Readings {
		if reading.Error != "" {
			return r, fmt.Errorf("pod read %q Pod %s: %s", id, reading.Pod, reading.Error)
		}
		if reading.Truncated {
			return r, fmt.Errorf("pod read %q Pod %s: output exceeds %d bytes; narrow the command", id, reading.Pod, LimitBytes)
		}
	}
	return r, nil
}

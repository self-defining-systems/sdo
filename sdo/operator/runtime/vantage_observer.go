package runtime

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes"

	"sdo.dev/operator/kubereads"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/vantage"
)

// DefaultVantageImage is the pinned minimal tools image of the vantage helper
// (BusyBox: sh, awk, adjtimex, df, nc, tc, wget). SDO_VANTAGE_IMAGE overrides
// it for clusters that pull from a mirror.
const DefaultVantageImage = "docker.io/library/busybox@sha256:a46257e06c82ad85242d0d38077d0a52d9896389ca60a1a59d3463db586e5fa4"

// vantageHelperPrefix names the helper containers; one per target container.
const vantageHelperPrefix = "sdo-vantage-"

// maxVantageHelpers bounds the helpers one target container gets: an
// ephemeral container that exited cannot restart, so a new name is used.
const maxVantageHelpers = 3

var errVantageHelperStarting = errors.New("vantage helper is starting")

// VantageObserver runs declared vantage probes in a helper container it
// attaches to each target Pod: an ephemeral container of the pinned tools
// image that targets the probed container (sharing its process namespace) and
// shares the Pod's network namespace. It runs each probe on its interval, at
// most vantage.MaxConcurrent commands at once, each within
// vantage.ExecTimeout, and gives up after vantage.CollectionBudget. A Pod
// whose helper is still starting answers with an error and stays due.
type VantageObserver struct {
	mu        sync.Mutex
	probes    []vantage.Probe
	client    kubernetes.Interface
	exec      kubereads.PodExec
	get       func(context.Context, sdk.ObjectRef) (*unstructured.Unstructured, error)
	protected string
	image     string
	clock     func() time.Time
	last      map[string]vantageRun
}

type vantageRun struct {
	at     time.Time
	result vantage.Result
}

// NewVantageObserver validates declarations and returns nil when unused.
func NewVantageObserver(client kubernetes.Interface, exec kubereads.PodExec, protected, image string, detectors []sdk.Detector, clock func() time.Time) (*VantageObserver, error) {
	probes, err := vantage.Probes(detectors)
	if err != nil || len(probes) == 0 {
		return nil, err
	}
	if image == "" {
		image = DefaultVantageImage
	}
	return &VantageObserver{
		probes: probes, client: client, exec: exec, protected: protected, image: image, clock: clock, last: map[string]vantageRun{},
		get: func(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
			return kubereads.GetPod(ctx, client, protected, ref)
		},
	}, nil
}

// VantageSnapshotProvider adds the frozen vantage readings to a snapshot, each
// with the promoted healthy readings of its probe when Baseline holds them.
type VantageSnapshotProvider struct {
	Base     SnapshotProvider
	Observer *VantageObserver
	Baseline *VantageBaseline
}

func (p VantageSnapshotProvider) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	base, err := p.Base.Snapshot(ctx)
	if err != nil {
		return base, err
	}
	results := p.Observer.collect(ctx, base)
	p.Baseline.attach(results)
	return inputs.New(base).WithVantage(vantageResults(results)), nil
}

type vantageResults map[string]vantage.Result

func (r vantageResults) VantageResult(id string) (vantage.Result, bool) {
	v, ok := r[id]
	v.Readings = slices.Clone(v.Readings)
	return v, ok
}

func (r vantageResults) VantageResults() map[string]vantage.Result {
	out := maps.Clone(map[string]vantage.Result(r))
	for id, v := range out {
		v.Readings = slices.Clone(v.Readings)
		out[id] = v
	}
	return out
}

const vantageNotStarted = "not read within the collection budget"

type vantageJob struct {
	probe     vantage.Probe
	index     int
	pod       corev1.Pod
	container string
}

func (o *VantageObserver) collect(ctx context.Context, base sdk.DetectionContext) map[string]vantage.Result {
	o.mu.Lock()
	defer o.mu.Unlock()
	now := o.clock().UTC()
	results := map[string]vantage.Result{}
	pods := base.Pods()
	sort.Slice(pods, func(i, j int) bool { return pods[i].Name < pods[j].Name })
	ctx, cancel := context.WithTimeout(ctx, vantage.CollectionBudget)
	defer cancel()
	due := map[string]bool{}
	var jobs []vantageJob
	for _, probe := range o.probes {
		var selected []vantageJob
		for _, pod := range pods {
			if pod.Namespace != base.Namespace() {
				continue
			}
			if container, ok := probe.From.Select(pod); ok {
				selected = append(selected, vantageJob{probe: probe, pod: pod, container: container})
			}
		}
		retained, ran := o.last[probe.ID]
		retained.result.Readings = slices.DeleteFunc(slices.Clone(retained.result.Readings), func(r vantage.Reading) bool {
			return !slices.ContainsFunc(selected, func(job vantageJob) bool { return string(job.pod.UID) == r.UID })
		})
		if ran && now.Sub(retained.at) < probe.Interval && len(retained.result.Readings) > 0 {
			results[probe.ID] = retained.result
			continue
		}
		result := vantage.Result{Probe: probe}
		switch {
		case len(selected) == 0:
			result.Error = "no running Pod holds the probe's role"
		case len(selected) > vantage.MaxTargets:
			result.Error = fmt.Sprintf("%d Pods hold the probe's role, over the limit of %d; narrow the role", len(selected), vantage.MaxTargets)
		default:
			due[probe.ID] = true
			result.Readings = make([]vantage.Reading, len(selected))
			for i, job := range selected {
				job.index = i
				result.Readings[i] = vantage.Reading{Namespace: job.pod.Namespace, Pod: job.pod.Name, UID: string(job.pod.UID), Container: job.container, ObservedAt: now, Kind: probe.Kind, Error: vantageNotStarted}
				jobs = append(jobs, job)
			}
		}
		results[probe.ID] = result
	}
	o.run(ctx, jobs, results)
	for id := range due {
		result := results[id]
		previous := o.last[id].result.Readings
		for i, reading := range result.Readings {
			for _, old := range previous {
				if old.UID == reading.UID && reading.Error == "" {
					result.Readings[i] = vantage.WithHistory(old, reading)
				}
			}
		}
		results[id] = result
		if slices.ContainsFunc(result.Readings, func(r vantage.Reading) bool { return r.Error == "" }) {
			o.last[id] = vantageRun{at: now, result: result}
		}
	}
	return results
}

// vantageHelperResult is one target container's helper, resolved once per
// cycle so probes of the same Pod never race to attach it.
type vantageHelperResult struct {
	name string
	err  error
}

func (o *VantageObserver) run(ctx context.Context, jobs []vantageJob, results map[string]vantage.Result) {
	helpers := map[string]vantageHelperResult{}
	for _, job := range jobs {
		key := string(job.pod.UID) + "/" + job.container
		if _, done := helpers[key]; done || !podContainerRunning(job.pod, job.container) || ctx.Err() != nil {
			continue
		}
		name, err := o.helper(ctx, job.pod, job.container)
		helpers[key] = vantageHelperResult{name: name, err: err}
	}
	slots := make(chan struct{}, vantage.MaxConcurrent)
	var running sync.WaitGroup
	for _, job := range jobs {
		select {
		case <-ctx.Done():
		case slots <- struct{}{}:
		}
		if ctx.Err() != nil {
			break
		}
		reading := &results[job.probe.ID].Readings[job.index]
		helper := helpers[string(job.pod.UID)+"/"+job.container]
		running.Go(func() {
			defer func() { <-slots }()
			o.read(ctx, job, helper, reading)
		})
	}
	running.Wait()
}

func (o *VantageObserver) read(ctx context.Context, job vantageJob, resolved vantageHelperResult, reading *vantage.Reading) {
	reading.Error = ""
	if !podContainerRunning(job.pod, job.container) {
		reading.Error = "container not running"
		return
	}
	if resolved.err != nil || resolved.name == "" {
		reading.Error = "vantage helper unavailable"
		if resolved.err != nil {
			reading.Error = resolved.err.Error()
		}
		return
	}
	helper := resolved.name
	args, err := vantage.Command(job.probe)
	if err != nil {
		reading.Error = err.Error()
		return
	}
	ctx, cancel := context.WithTimeout(ctx, vantage.ExecTimeout)
	defer cancel()
	stdout := &boundedOutput{limit: vantage.LimitBytes}
	command := program.Command{Pod: reading.Ref(), Container: helper, Args: args}
	code, err := kubereads.ReadOutput(ctx, o.get, o.exec, o.protected, reading.UID, command, stdout)
	switch {
	case ctx.Err() != nil:
		reading.Error = fmt.Sprintf("probe gave no answer within %s", vantage.ExecTimeout)
		return
	case err != nil:
		reading.Error = err.Error()
		return
	case stdout.truncated:
		reading.Error = fmt.Sprintf("probe output exceeds %d bytes", vantage.LimitBytes)
		return
	case code != 0 && !job.probe.Kind.Dials():
		reading.Error = fmt.Sprintf("probe failed (exit %d): %s", code, strings.TrimSpace(firstLine(stdout.text.String())))
		return
	}
	facts, dials, err := vantage.Parse(job.probe.Kind, stdout.text.String(), reading.ObservedAt)
	if err != nil {
		reading.Error = err.Error()
		return
	}
	reading.Facts, reading.Dials, reading.OK = facts, dials, true
	if job.probe.Kind.Dials() {
		durations := make([]time.Duration, 0, len(dials))
		for _, dial := range dials {
			reading.OK = reading.OK && dial.OK
			durations = append(durations, dial.Duration)
		}
		slices.Sort(durations)
		reading.Latency = durations[(len(durations)-1)/2]
	}
}

func firstLine(text string) string {
	line, _, _ := strings.Cut(text, "\n")
	return line
}

// helper returns the running helper container of the Pod's target
// container, or attaches one (and reports it starting).
func (o *VantageObserver) helper(ctx context.Context, pod corev1.Pod, target string) (string, error) {
	statuses := map[string]corev1.ContainerStatus{}
	for _, status := range pod.Status.EphemeralContainerStatuses {
		statuses[status.Name] = status
	}
	declared := map[string]bool{}
	for _, container := range pod.Spec.EphemeralContainers {
		declared[container.Name] = true
	}
	var name string
	for attempt := 1; attempt <= maxVantageHelpers; attempt++ {
		candidate := vantageHelperName(target, attempt)
		status, observed := statuses[candidate]
		switch {
		case observed && status.State.Running != nil:
			return candidate, nil
		case observed && status.State.Terminated != nil:
			continue
		case observed || declared[candidate]:
			if status.State.Waiting != nil && status.State.Waiting.Reason != "" && status.State.Waiting.Reason != "ContainerCreating" {
				return "", fmt.Errorf("%w: %s", errVantageHelperStarting, status.State.Waiting.Reason)
			}
			return "", errVantageHelperStarting
		}
		name = candidate
		break
	}
	if name == "" {
		return "", fmt.Errorf("vantage helpers of container %s exited %d times", target, maxVantageHelpers)
	}
	if o.client == nil {
		return "", errors.New("vantage helper cannot be attached without a Kubernetes client")
	}
	if o.protected != "" && pod.Namespace == o.protected {
		return "", errors.New("vantage probes never enter the SDO control namespace")
	}
	fresh, err := o.client.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("vantage helper: read Pod: %w", err)
	}
	if fresh.UID != pod.UID {
		return "", errors.New("vantage helper: Pod identity changed")
	}
	if slices.ContainsFunc(fresh.Spec.EphemeralContainers, func(c corev1.EphemeralContainer) bool { return c.Name == name }) {
		return "", errVantageHelperStarting
	}
	attach := func(privileged bool) error {
		next := fresh.DeepCopy()
		next.Spec.EphemeralContainers = append(next.Spec.EphemeralContainers, vantageHelper(name, target, o.image, privileged))
		_, err := o.client.CoreV1().Pods(pod.Namespace).UpdateEphemeralContainers(ctx, pod.Name, next, metav1.UpdateOptions{})
		return err
	}
	// SYS_PTRACE lets the helper read the target's descriptors and cgroup
	// when it runs as another user; a namespace whose Pod Security level
	// refuses the capability still gets a helper without it.
	if err := attach(true); err != nil {
		if !apierrors.IsForbidden(err) && !apierrors.IsInvalid(err) {
			return "", fmt.Errorf("vantage helper: attach: %w", err)
		}
		if err := attach(false); err != nil {
			return "", fmt.Errorf("vantage helper: attach: %w", err)
		}
	}
	return "", errVantageHelperStarting
}

func vantageHelperName(target string, attempt int) string {
	name := vantageHelperPrefix + target
	if attempt > 1 {
		name += "-" + strconv.Itoa(attempt)
	}
	if len(name) > 63 {
		name = name[:63]
	}
	return strings.TrimRight(name, "-")
}

// vantageHelper is the helper container: it idles, and each probe execs in
// it. It requests no resources and takes no volumes or environment.
func vantageHelper(name, target, image string, ptrace bool) corev1.EphemeralContainer {
	container := corev1.EphemeralContainer{
		EphemeralContainerCommon: corev1.EphemeralContainerCommon{
			Name: name, Image: image, ImagePullPolicy: corev1.PullIfNotPresent,
			Command:                  []string{"sleep", "2147483647"},
			TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
		},
		TargetContainerName: target,
	}
	if ptrace {
		container.SecurityContext = &corev1.SecurityContext{Capabilities: &corev1.Capabilities{Add: []corev1.Capability{"SYS_PTRACE"}}}
	}
	return container
}

// VantageBaseline holds the complete vantage readings of the controller's
// last promoted healthy baseline, per probe, settled like PodReadBaseline.
type VantageBaseline struct {
	baseline *settledBaseline[map[string]vantage.Result]
}

// NewVantageBaseline returns an empty holder whose promotions settle for settle.
func NewVantageBaseline(settle time.Duration) *VantageBaseline {
	return &VantageBaseline{baseline: newSettledBaseline[map[string]vantage.Result](settle)}
}

func (b *VantageBaseline) observe(now time.Time, healthy bool, snapshot sdk.DetectionContext) {
	if b == nil {
		return
	}
	set, ok := snapshot.(vantage.ResultSet)
	if !ok {
		return
	}
	results := set.VantageResults()
	if len(results) == 0 {
		return
	}
	complete := map[string]vantage.Result{}
	if previous, ok := b.baseline.Current(); ok {
		maps.Copy(complete, previous)
	}
	for id, result := range results {
		readings := slices.DeleteFunc(slices.Clone(result.Readings), func(r vantage.Reading) bool { return r.Error != "" })
		if result.Error == "" && len(readings) > 0 {
			complete[id] = vantage.Result{Probe: result.Probe, Readings: readings}
		}
	}
	b.baseline.Observe(now, healthy, complete)
}

func (b *VantageBaseline) attach(results map[string]vantage.Result) {
	if b == nil {
		return
	}
	promoted, ok := b.baseline.Current()
	if !ok {
		return
	}
	for id, result := range results {
		base, ok := promoted[id]
		if !ok || base.Probe.Kind != result.Probe.Kind || !samePeer(base.Probe.To, result.Probe.To) {
			continue
		}
		result.Baseline = slices.Clone(base.Readings)
		results[id] = result
	}
}

func samePeer(a, b *vantage.Peer) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}

// promoteObservationBaselines feeds each complete evaluation to the vantage
// and platform baselines, whose own settle decides promotion.
func (c *Controller) promoteObservationBaselines(now time.Time, snapshot sdk.DetectionContext, healthy bool) {
	c.VantageBaseline.observe(now.UTC(), healthy, snapshot)
	c.Platform.observe(now.UTC(), healthy, snapshot)
}

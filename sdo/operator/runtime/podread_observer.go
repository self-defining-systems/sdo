package runtime

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/kubereads"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/program"
)

// PodReadObserver runs declared read commands inside Pods on each query's
// interval and retains the last readings between runs. A cycle runs at most
// podread.MaxConcurrent commands at once, each within podread.ExecTimeout, and
// gives up after podread.CollectionBudget. A query it could not start stays due
// and one it started does not, so a hung store cannot starve the other queries.
// Collection is idle unless a loaded detector implements podread.Consumer.
type PodReadObserver struct {
	mu        sync.Mutex
	queries   []podread.Query
	exec      kubereads.PodExec
	get       func(context.Context, sdk.ObjectRef) (*unstructured.Unstructured, error)
	protected string
	clock     func() time.Time
	last      map[string]podReadRun
}

type podReadRun struct {
	at     time.Time
	result podread.Result
}

// NewPodReadObserver validates declarations and returns nil when unused.
func NewPodReadObserver(client kubernetes.Interface, exec kubereads.PodExec, protected string, detectors []sdk.Detector, clock func() time.Time) (*PodReadObserver, error) {
	queries, err := podread.Queries(detectors)
	if err != nil || len(queries) == 0 {
		return nil, err
	}
	return newPodReadObserver(client, exec, protected, queries, clock), nil
}

func newPodReadObserver(client kubernetes.Interface, exec kubereads.PodExec, protected string, queries []podread.Query, clock func() time.Time) *PodReadObserver {
	return &PodReadObserver{
		queries: queries, exec: exec, protected: protected, clock: clock, last: map[string]podReadRun{},
		get: func(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
			return kubereads.GetPod(ctx, client, protected, ref)
		},
	}
}

// PodReadsSnapshotProvider adds the frozen pod reads to a snapshot, each with
// the promoted healthy readings of its query when Baseline holds them.
type PodReadsSnapshotProvider struct {
	Base     SnapshotProvider
	Observer *PodReadObserver
	Baseline *PodReadBaseline
}

func (p PodReadsSnapshotProvider) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	base, err := p.Base.Snapshot(ctx)
	if err != nil {
		return base, err
	}
	results := maps.Clone(p.Observer.collect(ctx, base))
	p.Baseline.attach(results)
	return inputs.New(base).WithPodReads(podReadResults(results)), nil
}

type podReadResults map[string]podread.Result

func (r podReadResults) PodReadResult(id string) (podread.Result, bool) {
	v, ok := r[id]
	v.Readings = slices.Clone(v.Readings)
	return v, ok
}

func (r podReadResults) PodReadResults() map[string]podread.Result {
	out := maps.Clone(map[string]podread.Result(r))
	for id, v := range out {
		v.Readings = slices.Clone(v.Readings)
		out[id] = v
	}
	return out
}

const podReadNotStarted = "skipped: pod-read collection budget ended before this read started"

// podReadJob is one command in one selected container.
type podReadJob struct {
	id        string
	args      []string
	index     int
	pod       corev1.Pod
	container string
}

// collect runs the due queries in two waves under one collection budget: the
// plain queries, then each lookup (podread.Keys) whose source ran, with the
// keys that source printed. A lookup otherwise keeps its last answer.
func (o *PodReadObserver) collect(ctx context.Context, base sdk.DetectionContext) map[string]podread.Result {
	o.mu.Lock()
	defer o.mu.Unlock()
	now := o.clock().UTC()
	results := map[string]podread.Result{}
	pods := base.Pods()
	sort.Slice(pods, func(i, j int) bool { return pods[i].Name < pods[j].Name })
	ctx, cancel := context.WithTimeout(ctx, podread.CollectionBudget)
	defer cancel()
	due := map[string]bool{}
	var jobs []podReadJob
	for _, query := range o.queries {
		if query.Keys == nil {
			jobs = append(jobs, o.plan(query, false, pods, base.Namespace(), now, results, due)...)
		}
	}
	o.run(ctx, jobs, results)
	jobs = nil
	for _, query := range o.queries {
		if query.Keys == nil {
			continue
		}
		fresh := due[query.Keys.From]
		if fresh || !o.retained(query, now) {
			keys, err := podread.KeysOf(results[query.Keys.From], *query.Keys)
			if err != nil {
				results[query.ID] = podread.Result{Query: query, Error: fmt.Sprintf("keys from %s: %v", query.Keys.From, err)}
				continue
			}
			query.Args = podread.ExpandKeys(query.Args, keys)
		}
		jobs = append(jobs, o.plan(query, fresh, pods, base.Namespace(), now, results, due)...)
	}
	o.run(ctx, jobs, results)
	for id := range due {
		if slices.ContainsFunc(results[id].Readings, func(r podread.Reading) bool { return r.Error != podReadNotStarted }) {
			o.last[id] = podReadRun{at: now, result: results[id]}
		}
	}
	return results
}

// collectFresh runs every query now, ignoring retained answers. An app-state
// reader owns its observer and its interval, so each of its reads sees current
// output rather than an answer kept from the previous interval.
func (o *PodReadObserver) collectFresh(ctx context.Context, base sdk.DetectionContext) map[string]podread.Result {
	o.mu.Lock()
	clear(o.last)
	o.mu.Unlock()
	return o.collect(ctx, base)
}

// retained reports whether the query has an answer from within its interval.
func (o *PodReadObserver) retained(query podread.Query, now time.Time) bool {
	last, ran := o.last[query.ID]
	return ran && now.Sub(last.at) < query.Interval && len(last.result.Readings) > 0
}

// plan records the query's retained answer, or its pending readings and the
// jobs that fill them when it is due or forced.
func (o *PodReadObserver) plan(query podread.Query, force bool, pods []corev1.Pod, namespace string, now time.Time, results map[string]podread.Result, due map[string]bool) []podReadJob {
	selected := selectPodReadTargets(query, pods, namespace)
	retained, ran := o.last[query.ID]
	retained.result.Readings = keepObservedReadings(retained.result.Readings, selected)
	if !force && ran && now.Sub(retained.at) < query.Interval && len(retained.result.Readings) > 0 {
		results[query.ID] = retained.result
		return nil
	}
	result := podread.Result{Query: query}
	var jobs []podReadJob
	switch {
	case len(selected) == 0:
		result.Error = "no running Pod holds the target role"
	case len(selected) > podread.MaxTargets:
		result.Error = fmt.Sprintf("%d Pods hold the target role, over the limit of %d; narrow the target", len(selected), podread.MaxTargets)
	default:
		due[query.ID] = true
		result.Readings = make([]podread.Reading, len(selected))
		for i, job := range selected {
			job.id, job.args, job.index = query.ID, query.Args, i
			result.Readings[i] = podread.Reading{Namespace: job.pod.Namespace, Pod: job.pod.Name, UID: string(job.pod.UID), Container: job.container, ObservedAt: now, Error: podReadNotStarted}
			jobs = append(jobs, job)
		}
	}
	results[query.ID] = result
	return jobs
}

// selectPodReadTargets lists the application Pods holding the query's role.
func selectPodReadTargets(query podread.Query, pods []corev1.Pod, namespace string) []podReadJob {
	var selected []podReadJob
	for _, pod := range pods {
		if pod.Namespace != namespace {
			continue
		}
		if container, ok := query.Target.Select(pod); ok {
			selected = append(selected, podReadJob{pod: pod, container: container})
		}
	}
	return selected
}

// keepObservedReadings drops retained readings of Pods that left the role, so a
// finding never names a Pod the snapshot no longer holds.
func keepObservedReadings(readings []podread.Reading, selected []podReadJob) []podread.Reading {
	return slices.DeleteFunc(slices.Clone(readings), func(r podread.Reading) bool {
		return !slices.ContainsFunc(selected, func(job podReadJob) bool { return string(job.pod.UID) == r.UID })
	})
}

// run executes jobs under the cycle's concurrency bound until ctx, which
// carries the collection budget, ends. Each job writes only its own
// preallocated reading.
func (o *PodReadObserver) run(ctx context.Context, jobs []podReadJob, results map[string]podread.Result) {
	slots := make(chan struct{}, podread.MaxConcurrent)
	var running sync.WaitGroup
	for _, job := range jobs {
		select {
		case <-ctx.Done():
		case slots <- struct{}{}:
		}
		if ctx.Err() != nil {
			break
		}
		reading := &results[job.id].Readings[job.index]
		running.Go(func() {
			defer func() { <-slots }()
			o.read(ctx, job, reading)
		})
	}
	running.Wait()
}

// read fills one reading. Its Error is set for every way the command gave no
// answer, so a failure is never read as a result.
func (o *PodReadObserver) read(ctx context.Context, job podReadJob, reading *podread.Reading) {
	reading.Error = ""
	if !podContainerRunning(job.pod, job.container) {
		reading.Error = "container not running"
		return
	}
	ctx, cancel := context.WithTimeout(ctx, podread.ExecTimeout)
	defer cancel()
	stdout := &boundedOutput{limit: podread.LimitBytes}
	command := program.Command{Pod: reading.Ref(), Container: job.container, Args: job.args}
	code, err := kubereads.ReadOutput(ctx, o.get, o.exec, o.protected, reading.UID, command, stdout)
	switch {
	case ctx.Err() != nil:
		reading.Error = fmt.Sprintf("command gave no answer within %s", podread.ExecTimeout)
	case err != nil:
		reading.Error = err.Error()
	case code == 126 || code == 127:
		// The shell's codes for a command that could not start: not its answer.
		reading.Error = fmt.Sprintf("command could not start (exit %d)", code)
	default:
		reading.ExitCode = code
		reading.Stdout = strings.ToValidUTF8(stdout.text.String(), "�")
		reading.Truncated = stdout.truncated
	}
}

func podContainerRunning(pod corev1.Pod, container string) bool {
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == container {
			return status.State.Running != nil
		}
	}
	return false
}

// boundedOutput keeps a prefix and drains the rest, so an oversized answer never
// blocks the command on a full stream.
type boundedOutput struct {
	text      strings.Builder
	limit     int
	truncated bool
}

func (b *boundedOutput) Write(p []byte) (int, error) {
	keep := p
	if room := b.limit - b.text.Len(); len(keep) > room {
		keep = keep[:room]
		b.truncated = true
	}
	b.text.Write(keep)
	return len(p), nil
}

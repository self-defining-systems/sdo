package runtime

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/logs"
)

// LogsObserverOptions supplies the cycle clock; zero uses time.Now.
type LogsObserverOptions struct{ Clock func() time.Time }

// LogsObserver collects at most 64 KiB per container per cycle (including
// previous logs), with a 5 second request timeout and 20 retained cycles.
// It keeps no raw logs, only counts and at most three 256-byte excerpts per
// pattern per cycle. Initial current reads cover the preceding minute. A capped
// read drops the unread remainder through the cycle timestamp (counts are lower
// bounds), preventing a single oversized line from stalling the cursor.
// More than 256 containers fails closed. Collection is idle
// unless a loaded detector implements logs.Consumer.
type LogsObserver struct {
	mu           sync.Mutex
	queries      []logs.Query
	patterns     map[string]*regexp.Regexp
	previous     bool
	terminations bool
	clock        func() time.Time
	read         func(context.Context, string, string, *corev1.PodLogOptions) (io.ReadCloser, error)
	states       map[string]*logState
}
type logState struct {
	window       logs.Window
	last         time.Time
	equal        int
	previousRead bool
}

// NewLogsObserver validates declarations and returns nil when unused.
func NewLogsObserver(client kubernetes.Interface, ds []sdk.Detector, opt LogsObserverOptions) (*LogsObserver, error) {
	qs, err := logs.Queries(ds)
	if err != nil {
		return nil, err
	}
	if len(qs) == 0 {
		return nil, nil
	}
	if opt.Clock == nil {
		opt.Clock = time.Now
	}
	o := &LogsObserver{queries: qs, patterns: map[string]*regexp.Regexp{}, clock: opt.Clock, states: map[string]*logState{}}
	for _, q := range qs {
		o.previous = o.previous || q.Previous
		o.terminations = o.terminations || q.Terminations
		for _, p := range q.Patterns {
			o.patterns[p.RE2] = regexp.MustCompile(p.RE2)
		}
	}
	o.read = func(ctx context.Context, ns, pod string, opt *corev1.PodLogOptions) (io.ReadCloser, error) {
		if client == nil {
			return nil, fmt.Errorf("log client not configured")
		}
		return client.CoreV1().Pods(ns).GetLogs(pod, opt).Stream(ctx)
	}
	return o, nil
}

// LogsSnapshotProvider adds a cycle's frozen log counts to a snapshot.
type LogsSnapshotProvider struct {
	Base     SnapshotProvider
	Observer *LogsObserver
}

func (p LogsSnapshotProvider) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	base, err := p.Base.Snapshot(ctx)
	if err != nil || p.Observer == nil {
		return base, err
	}
	return inputs.New(base).WithLogs(logResults(p.Observer.collect(ctx, base))), nil
}

type logResults map[string]logs.Result

func (r logResults) LogResult(id string) (logs.Result, bool) {
	v, ok := r[id]
	return cloneLogResult(v), ok
}
func cloneLogResult(r logs.Result) logs.Result {
	r.Query.Patterns = append([]logs.Pattern(nil), r.Query.Patterns...)
	r.Windows = append([]logs.Window(nil), r.Windows...)
	for i := range r.Windows {
		w := &r.Windows[i]
		w.Terminations = append([]logs.Termination(nil), w.Terminations...)
		for j := range w.Terminations {
			w.Terminations[j].Tail = cloneLogCycle(w.Terminations[j].Tail)
		}
		w.Cycles = append([]logs.Cycle(nil), w.Cycles...)
		for j := range w.Cycles {
			w.Cycles[j] = cloneLogCycle(w.Cycles[j])
		}
	}
	return r
}
func cloneLogCycle(c logs.Cycle) logs.Cycle {
	m := map[string]logs.Match{}
	for k, v := range c.Matches {
		v.Excerpts = append([]string(nil), v.Excerpts...)
		m[k] = v
	}
	c.Matches = m
	if c.Previous != nil {
		p := cloneLogCycle(*c.Previous)
		c.Previous = &p
	}
	return c
}
func (o *LogsObserver) collect(ctx context.Context, base sdk.DetectionContext) map[string]logs.Result {
	o.mu.Lock()
	defer o.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	at := o.clock().UTC()
	results := map[string]logs.Result{}
	for _, q := range o.queries {
		results[q.ID] = logs.Result{Query: q, ObservedAt: at}
	}
	pods := base.Pods()
	sort.Slice(pods, func(i, j int) bool { return pods[i].Name < pods[j].Name })
	count := 0
	for _, p := range pods {
		if p.Namespace == base.Namespace() {
			count += len(p.Spec.Containers) + len(p.Spec.InitContainers) + len(p.Spec.EphemeralContainers)
		}
	}
	if count > logs.MaxContainers {
		for id, r := range results {
			r.Error = fmt.Sprintf("log source exceeds %d containers", logs.MaxContainers)
			results[id] = r
		}
		o.states = map[string]*logState{}
		return results
	}
	next := map[string]*logState{}
	for _, pod := range pods {
		if pod.Namespace != base.Namespace() {
			continue
		}
		statuses := append(append(append([]corev1.ContainerStatus{}, pod.Status.ContainerStatuses...), pod.Status.InitContainerStatuses...), pod.Status.EphemeralContainerStatuses...)
		names := []string{}
		for _, c := range pod.Spec.Containers {
			names = append(names, c.Name)
		}
		for _, c := range pod.Spec.InitContainers {
			names = append(names, c.Name)
		}
		for _, c := range pod.Spec.EphemeralContainers {
			names = append(names, c.Name)
		}
		sort.Strings(names)
		for _, name := range names {
			var status corev1.ContainerStatus
			for _, s := range statuses {
				if s.Name == name {
					status = s
				}
			}
			key := string(pod.UID) + "/" + name
			state := o.states[key]
			if state == nil {
				state = &logState{window: logs.Window{Namespace: pod.Namespace, Pod: pod.Name, UID: string(pod.UID), Container: name, RestartCount: status.RestartCount}, last: at.Add(-time.Minute)}
			}
			if state.window.RestartCount != status.RestartCount {
				state.window.RestartCount = status.RestartCount
				state.window.Cycles = nil
				state.last, state.equal, state.previousRead = at.Add(-time.Minute), 0, false
			}
			start := state.last
			if n := len(state.window.Cycles); n > 0 {
				start = state.window.Cycles[n-1].End
			}
			budget := int64(logs.LimitBytes)
			var previous *logs.Cycle
			// Termination tails have no incremental cursor: the decisive line may
			// predate our initial observation or the reset of the current window.
			for _, prev := range []bool{true, false} {
				if !o.terminations {
					break
				}
				term := status.State.Terminated
				incarnation := status.RestartCount
				if prev {
					term = status.LastTerminationState.Terminated
					// LastTerminationState refers to the preceding incarnation.
					if incarnation > 0 {
						incarnation--
					}
				}
				if term == nil {
					continue
				}
				found := -1
				for i, t := range state.window.Terminations {
					if t.FinishedAt.Equal(term.FinishedAt.Time) && t.RestartCount == incarnation {
						found = i
						break
					}
				}
				if found < 0 || state.window.Terminations[found].Tail.Error != "" {
					tail, used := o.fetch(ctx, pod.Namespace, pod.Name, name, prev, true, &logState{}, at, budget/2)
					budget -= used
					observation := logs.Termination{RestartCount: incarnation, FinishedAt: term.FinishedAt.Time, ExitCode: term.ExitCode, Tail: tail}
					if found < 0 {
						state.window.Terminations = append(state.window.Terminations, observation)
						found = len(state.window.Terminations) - 1
					} else {
						state.window.Terminations[found] = observation
					}
				}
				if prev && o.previous && !state.previousRead {
					p := state.window.Terminations[found].Tail
					// Failed tails remain in Terminations for this evaluation and retry;
					// do not poison every later cycle with the failed previous evidence.
					if p.Error == "" {
						previous = &p
					}
					state.previousRead = p.Error == ""
				}
			}
			if len(state.window.Terminations) > logs.WindowCycles {
				state.window.Terminations = state.window.Terminations[len(state.window.Terminations)-logs.WindowCycles:]
			}
			if o.previous && status.RestartCount > 0 && !state.previousRead && (!o.terminations || status.LastTerminationState.Terminated == nil) {
				p, used := o.fetch(ctx, pod.Namespace, pod.Name, name, true, true, &logState{}, at, budget/2)
				previous = &p
				budget -= used
				state.previousRead = p.Error == ""
			}
			c := logs.Cycle{End: at, Matches: map[string]logs.Match{}}
			// Waiting/never-started containers have no current log source.
			if status.State.Running != nil {
				c, _ = o.fetch(ctx, pod.Namespace, pod.Name, name, false, false, state, at, budget)
			}
			c.Start = start
			c.Running = status.State.Running != nil
			c.Previous = previous
			state.window.Cycles = append(state.window.Cycles, c)
			if len(state.window.Cycles) > logs.WindowCycles {
				state.window.Cycles = append([]logs.Cycle(nil), state.window.Cycles[len(state.window.Cycles)-logs.WindowCycles:]...)
			}
			next[key] = state
			for _, q := range o.queries {
				w := state.window
				w.Terminations = append([]logs.Termination(nil), state.window.Terminations...)
				for i, t := range w.Terminations {
					w.Terminations[i].Tail = projectLogCycle(t.Tail, q)
				}
				if !q.Terminations {
					w.Terminations = nil
				}
				w.Cycles = make([]logs.Cycle, len(state.window.Cycles))
				for i, c := range state.window.Cycles {
					w.Cycles[i] = projectLogCycle(c, q)
					if !q.Previous {
						w.Cycles[i].Previous = nil
					}
				}
				r := results[q.ID]
				r.Windows = append(r.Windows, w)
				results[q.ID] = r
			}
		}
	}
	o.states = next
	return results
}
func projectLogCycle(c logs.Cycle, q logs.Query) logs.Cycle {
	m := map[string]logs.Match{}
	for _, p := range q.Patterns {
		m[p.ID] = c.Matches[p.RE2]
	}
	c.Matches = m
	if c.Previous != nil {
		p := projectLogCycle(*c.Previous, q)
		c.Previous = &p
	}
	return c
}
func (o *LogsObserver) fetch(ctx context.Context, ns, pod, container string, previous, tail bool, state *logState, at time.Time, budget int64) (logs.Cycle, int64) {
	c := logs.Cycle{End: at, Matches: map[string]logs.Match{}}
	opt := &corev1.PodLogOptions{Container: container, Previous: previous, Timestamps: true, LimitBytes: &budget}
	if tail {
		tail := int64(1000)
		opt.TailLines = &tail
	}
	if !tail {
		opt.SinceTime = &metav1.Time{Time: state.last.Truncate(time.Second)}
	}
	request, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	stream, err := o.read(request, ns, pod, opt)
	if err != nil {
		c.Error = err.Error()
		return c, 0
	}
	defer stream.Close()
	data, err := io.ReadAll(io.LimitReader(stream, budget))
	if err != nil {
		c.Error = err.Error()
		return c, int64(len(data))
	}
	c.Truncated = int64(len(data)) >= budget
	oldLast, oldEqual := state.last, state.equal
	newest, equal := oldLast, oldEqual
	skipped := 0
	lines := strings.Split(string(data), "\n")
	for index, line := range lines {
		if line == "" {
			continue
		}
		// A cut last line is not a complete observation.
		if c.Truncated && index == len(lines)-1 {
			break
		}
		stamp, text, ok := strings.Cut(line, " ")
		ts, err := time.Parse(time.RFC3339Nano, stamp)
		if !ok || err != nil {
			if c.Truncated {
				continue
			}
			c.Error = "malformed timestamped container log"
			return c, int64(len(data))
		}
		if !tail {
			if ts.Before(oldLast) {
				continue
			}
			if ts.Equal(oldLast) {
				skipped++
				if skipped <= oldEqual {
					continue
				}
			}
		}
		c.Lines++
		if ts.After(newest) {
			newest = ts
			equal = 1
		} else if ts.Equal(newest) {
			equal++
		}
		for expr, re := range o.patterns {
			if re.MatchString(text) {
				m := c.Matches[expr]
				m.Count++
				if len(m.Excerpts) < logs.MaxExcerpts {
					m.Excerpts = append(m.Excerpts, text[:min(len(text), logs.MaxExcerptBytes)])
				}
				c.Matches[expr] = m
			}
		}
	}
	if c.Error == "" {
		if c.Truncated && newest.Before(at) {
			newest, equal = at, 0
		}
		state.last, state.equal = newest, equal
	}
	return c, int64(len(data))
}

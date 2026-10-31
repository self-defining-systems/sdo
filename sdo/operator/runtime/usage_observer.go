package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/common/expfmt"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/usage"
)

// usageSnapshotProvider freezes bounded kubelet usage windows into each cycle.
// It serializes collection and caches even errors for at least fifteen seconds.
type usageSnapshotProvider struct {
	Base       SnapshotProvider
	client     kubernetes.Interface
	throttling bool
	clock      func() time.Time
	mu         sync.Mutex
	last       time.Time
	result     usage.Result
}

func withUsage(base SnapshotProvider, detectors []sdk.Detector, client kubernetes.Interface, clock func() time.Time) SnapshotProvider {
	consumed, throttling := false, false
	for _, d := range detectors {
		if c, ok := d.(usage.Consumer); ok {
			consumed = true
			throttling = throttling || c.UsageRequirements().Throttling
		}
	}
	if !consumed {
		return base
	}
	if clock == nil {
		clock = time.Now
	}
	return &usageSnapshotProvider{Base: base, client: client, throttling: throttling, clock: clock}
}
func (p *usageSnapshotProvider) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	base, err := p.Base.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.clock().UTC()
	if p.last.IsZero() || now.Sub(p.last) >= 15*time.Second {
		previous := p.result
		p.last = now
		next, err := p.collect(ctx, base, now)
		if err != nil {
			previous.Error = err.Error()
			previous.ObservedAt = now
			p.result = previous
		} else {
			p.result = mergeUsage(previous, next)
		}
	}
	return usageContext{metricsContext: metricsContext{DetectionContext: base}, result: usage.Clone(p.result)}, nil
}

type usageContext struct {
	metricsContext
	result usage.Result
}

func (c usageContext) UsageResult() (usage.Result, bool) { return usage.Clone(c.result), true }
func (c usageContext) MetricResult(id string) (metrics.Result, bool) {
	return metrics.ResultFrom(c.DetectionContext, id)
}
func (c usageContext) AlertsResult() (metrics.AlertsResult, bool) {
	return metrics.AlertsFrom(c.DetectionContext)
}
func (c metricsContext) UsageResult() (usage.Result, bool) {
	s, ok := c.DetectionContext.(usage.Source)
	if !ok {
		return usage.Result{}, false
	}
	return s.UsageResult()
}

const maxUsageBytes = 16 << 20
const maxUsageNodes = 128
const maxUsageContainers = 4096

func (p *usageSnapshotProvider) collect(ctx context.Context, base sdk.DetectionContext, now time.Time) (usage.Result, error) {
	r := usage.Result{ObservedAt: now}
	if p.client == nil {
		return r, fmt.Errorf("Kubernetes usage client unavailable")
	}
	pods := base.Pods()
	byNode := map[string][]corev1.Pod{}
	for _, pod := range pods {
		if pod.Namespace == base.Namespace() && pod.Spec.NodeName != "" {
			byNode[pod.Spec.NodeName] = append(byNode[pod.Spec.NodeName], pod)
		}
	}
	if len(byNode) > maxUsageNodes || len(pods) > maxUsageContainers {
		return r, fmt.Errorf("usage inventory exceeds bounds")
	}
	nodes := make([]string, 0, len(byNode))
	for node := range byNode {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for _, node := range nodes {
		data, err := p.read(ctx, node, "stats/summary")
		if err != nil {
			return r, err
		}
		part, err := parseUsageSummary(data, base.Namespace(), byNode[node], now)
		if err != nil {
			return r, fmt.Errorf("node %s summary: %w", node, err)
		}
		if p.throttling {
			data, err = p.read(ctx, node, "metrics/cadvisor")
			if err != nil {
				return r, err
			}
			counters, err := parseUsageCounters(data, base.Namespace())
			if err != nil {
				return r, err
			}
			for i := range part.Containers {
				w := &part.Containers[i]
				if w.Identity.Container == "" {
					continue
				}
				s := &w.Samples[0]
				if c, ok := counters[w.Identity.Pod+"/"+w.Identity.Container]; ok && c.available == 3 {
					s.Periods = c.periods
					s.ThrottledPeriods = c.throttled
					s.ThrottlingAvailable = true
				} else {
					return r, fmt.Errorf("CFS counters unavailable for %s/%s", w.Identity.Pod, w.Identity.Container)
				}
			}
		}
		r.Containers = append(r.Containers, part.Containers...)
		r.Volumes = append(r.Volumes, part.Volumes...)
		if len(r.Containers)+len(r.Volumes) > maxUsageContainers {
			return r, fmt.Errorf("usage observations exceed bounds")
		}
	}
	return r, nil
}
func (p *usageSnapshotProvider) read(ctx context.Context, node, path string) ([]byte, error) {
	// Stream rather than DoRaw so the API response cannot allocate without bound.
	client := p.client.CoreV1().RESTClient()
	if client == nil {
		return nil, fmt.Errorf("Kubernetes node proxy transport unavailable")
	}
	stream, err := client.Get().AbsPath("/api/v1/nodes/" + node + "/proxy/" + path).Stream(ctx)
	if err != nil {
		return nil, fmt.Errorf("node %s %s: %w", node, path, err)
	}
	defer stream.Close()
	return readUsageBounded(stream)
}
func mergeUsage(old, next usage.Result) usage.Result {
	containers := map[usage.Identity][]usage.Sample{}
	for _, w := range old.Containers {
		containers[w.Identity] = w.Samples
	}
	for i := range next.Containers {
		w := &next.Containers[i]
		history := containers[w.Identity]
		if len(history) > 0 && !w.Samples[0].At.After(history[len(history)-1].At) {
			w.Samples = history
			continue
		}
		w.Samples = append(append([]usage.Sample(nil), history...), w.Samples...)
		if len(w.Samples) > usage.MaxSamples {
			w.Samples = w.Samples[len(w.Samples)-usage.MaxSamples:]
		}
	}
	volumes := map[string][]usage.VolumeSample{}
	for _, w := range old.Volumes {
		volumes[w.PodUID+"/"+w.PVC] = w.Samples
	}
	for i := range next.Volumes {
		w := &next.Volumes[i]
		history := volumes[w.PodUID+"/"+w.PVC]
		if len(history) > 0 && !w.Samples[0].At.After(history[len(history)-1].At) {
			w.Samples = history
			continue
		}
		w.Samples = append(append([]usage.VolumeSample(nil), history...), w.Samples...)
		if len(w.Samples) > usage.MaxSamples {
			w.Samples = w.Samples[len(w.Samples)-usage.MaxSamples:]
		}
	}
	return next
}

// summary types mirror only the kubelet fields consumed here.
type usageCPU struct {
	Time           time.Time
	UsageNanoCores *float64
}
type usageMemory struct {
	Time                      time.Time
	WorkingSetBytes, RSSBytes *float64
}
type usageStorage struct {
	Time      time.Time
	UsedBytes *float64
}
type summaryContainer struct {
	Name      string
	StartTime time.Time
	CPU       usageCPU
	Memory    usageMemory
	Rootfs    usageStorage
	Logs      usageStorage
}
type summaryVolume struct {
	Time                                                                     time.Time
	PVCRef                                                                   *struct{ Name, Namespace string }
	CapacityBytes, UsedBytes, AvailableBytes, Inodes, InodesUsed, InodesFree *float64
}
type summaryPod struct {
	PodRef           struct{ Name, Namespace, UID string }
	Containers       []summaryContainer
	EphemeralStorage usageStorage `json:"ephemeral-storage"`
	Volume           []summaryVolume
}

func parseUsageSummary(data []byte, namespace string, pods []corev1.Pod, now time.Time) (usage.Result, error) {
	var summary struct{ Pods []summaryPod }
	r := usage.Result{ObservedAt: now}
	if len(data) > maxUsageBytes {
		return r, fmt.Errorf("summary exceeds byte bound")
	}
	if err := json.Unmarshal(data, &summary); err != nil {
		return r, err
	}
	index := map[string]corev1.Pod{}
	for _, p := range pods {
		index[string(p.UID)] = p
		if mirror := p.Annotations["kubernetes.io/config.mirror"]; mirror != "" {
			index[mirror] = p
		}
	}
	for _, p := range summary.Pods {
		spec, ok := index[p.PodRef.UID]
		if !ok || p.PodRef.Namespace != namespace || p.PodRef.Name != spec.Name {
			continue
		}
		p.PodRef.UID = string(spec.UID)
		containers := map[string]corev1.Container{}
		for _, c := range append(append([]corev1.Container{}, spec.Spec.Containers...), spec.Spec.InitContainers...) {
			containers[c.Name] = c
		}
		for _, c := range p.Containers {
			sc, ok := containers[c.Name]
			if !ok {
				continue
			}
			s := usage.Sample{At: c.CPU.Time, WorkingSetBytes: c.Memory.WorkingSetBytes, RSSBytes: c.Memory.RSSBytes}
			if s.At.IsZero() {
				s.At = c.Memory.Time
			}
			if s.At.IsZero() {
				s.At = now
			}
			if c.CPU.UsageNanoCores != nil {
				v := *c.CPU.UsageNanoCores / 1e9
				s.CPUCores = &v
			}
			if c.Rootfs.UsedBytes != nil && c.Logs.UsedBytes != nil {
				v := *c.Rootfs.UsedBytes + *c.Logs.UsedBytes
				s.EphemeralBytes = &v
			}
			s.CPURequest = resourceValue(sc.Resources.Requests, corev1.ResourceCPU)
			s.CPULimit = resourceValue(sc.Resources.Limits, corev1.ResourceCPU)
			s.MemoryRequest = resourceValue(sc.Resources.Requests, corev1.ResourceMemory)
			s.MemoryLimit = resourceValue(sc.Resources.Limits, corev1.ResourceMemory)
			s.EphemeralRequest = resourceValue(sc.Resources.Requests, corev1.ResourceEphemeralStorage)
			s.EphemeralLimit = resourceValue(sc.Resources.Limits, corev1.ResourceEphemeralStorage)
			r.Containers = append(r.Containers, usage.ContainerWindow{Identity: usage.Identity{PodUID: p.PodRef.UID, Pod: p.PodRef.Name, Container: c.Name, StartedAt: c.StartTime}, Samples: []usage.Sample{s}})
		}
		// Pod-level ephemeral usage is represented by the empty container identity.
		if p.EphemeralStorage.UsedBytes != nil {
			at := p.EphemeralStorage.Time
			if at.IsZero() {
				at = now
			}
			r.Containers = append(r.Containers, usage.ContainerWindow{Identity: usage.Identity{PodUID: p.PodRef.UID, Pod: p.PodRef.Name}, Samples: []usage.Sample{{At: at, EphemeralBytes: p.EphemeralStorage.UsedBytes}}})
		}
		for _, v := range p.Volume {
			if v.PVCRef == nil || v.PVCRef.Namespace != namespace {
				continue
			}
			at := v.Time
			if at.IsZero() {
				at = now
			}
			r.Volumes = append(r.Volumes, usage.VolumeWindow{PodUID: p.PodRef.UID, PVC: v.PVCRef.Name, Samples: []usage.VolumeSample{{At: at, PodUID: p.PodRef.UID, PVC: v.PVCRef.Name, CapacityBytes: v.CapacityBytes, UsedBytes: v.UsedBytes, AvailableBytes: v.AvailableBytes, Inodes: v.Inodes, InodesUsed: v.InodesUsed, InodesFree: v.InodesFree}}})
		}
	}
	// An omitted running container is an unavailable source, not an empty healthy window.
	observed := map[string]bool{}
	for _, w := range r.Containers {
		observed[w.Identity.PodUID+"/"+w.Identity.Container] = true
	}
	for _, p := range pods {
		for _, status := range append(append([]corev1.ContainerStatus{}, p.Status.ContainerStatuses...), p.Status.InitContainerStatuses...) {
			if status.State.Running != nil && !observed[string(p.UID)+"/"+status.Name] {
				return r, fmt.Errorf("summary missing running container %s/%s", p.Name, status.Name)
			}
		}
	}
	sort.Slice(r.Containers, func(i, j int) bool {
		a, b := r.Containers[i].Identity, r.Containers[j].Identity
		return a.PodUID+"/"+a.Container < b.PodUID+"/"+b.Container
	})
	sort.Slice(r.Volumes, func(i, j int) bool {
		return r.Volumes[i].PodUID+"/"+r.Volumes[i].PVC < r.Volumes[j].PodUID+"/"+r.Volumes[j].PVC
	})
	return r, nil
}
func resourceValue(resources corev1.ResourceList, name corev1.ResourceName) *float64 {
	q, ok := resources[name]
	if !ok {
		return nil
	}
	v := q.AsApproximateFloat64()
	return &v
}

type usageCounters struct {
	periods, throttled float64
	available          int
}

func parseUsageCounters(data []byte, namespace string) (map[string]usageCounters, error) {
	if len(data) > maxUsageBytes {
		return nil, fmt.Errorf("cAdvisor exceeds byte bound")
	}
	// Discard unrelated metric families before parsing; retain only bounded CFS lines.
	var filtered strings.Builder
	for line := range strings.SplitSeq(string(data), "\n") {
		if strings.HasPrefix(line, "container_cpu_cfs_periods_total{") || strings.HasPrefix(line, "container_cpu_cfs_throttled_periods_total{") {
			filtered.WriteString(line)
			filtered.WriteByte('\n')
		}
	}
	parser := expfmt.TextParser{}
	families, err := parser.TextToMetricFamilies(strings.NewReader(filtered.String()))
	if err != nil {
		return nil, fmt.Errorf("cAdvisor counters: %w", err)
	}
	result := map[string]usageCounters{}
	for name, family := range families {
		for _, metric := range family.Metric {
			labels := map[string]string{}
			for _, l := range metric.Label {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["namespace"] != namespace || labels["container"] == "" || labels["container"] == "POD" {
				continue
			}
			key := labels["pod"] + "/" + labels["container"]
			c := result[key]
			value := metric.GetUntyped().GetValue()
			if metric.Counter != nil {
				value = metric.GetCounter().GetValue()
			}
			if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
				return nil, fmt.Errorf("invalid CFS counter for %s", key)
			}
			if name == "container_cpu_cfs_periods_total" {
				if c.available&1 != 0 {
					return nil, fmt.Errorf("duplicate CFS series for %s", key)
				}
				c.periods = value
				c.available |= 1
			} else {
				if c.available&2 != 0 {
					return nil, fmt.Errorf("duplicate CFS series for %s", key)
				}
				c.throttled = value
				c.available |= 2
			}
			result[key] = c
			if len(result) > maxUsageContainers {
				return nil, fmt.Errorf("too many CFS series")
			}
		}
	}
	return result, nil
}

// Forward optional input views through the existing context wrappers.
func (c trafficContext) UsageResult() (usage.Result, bool) {
	source, ok := c.DetectionContext.(usage.Source)
	if !ok {
		return usage.Result{}, false
	}
	return source.UsageResult()
}

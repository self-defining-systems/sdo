//go:build integration && controller_e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// reconcilePods binds pods, drives container execution, and writes pod status.
func (k *Kubelet) reconcilePods(ctx context.Context) error {
	client := k.cluster.Client
	pods, err := client.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp != nil {
			k.Run.kill(pod.UID)
			if removeForegroundFinalizer(pod) {
				if _, err := client.CoreV1().Pods(pod.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
					return err
				}
			}
			if err := client.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{GracePeriodSeconds: new(int64)}); err != nil {
				return err
			}
			continue
		}
		if pod.Spec.NodeName == "" {
			binding := &corev1.Binding{ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace}, Target: corev1.ObjectReference{Kind: "Node", Name: nodeName}}
			if err := client.CoreV1().Pods(pod.Namespace).Bind(ctx, binding, metav1.CreateOptions{}); err != nil {
				return err
			}
			continue
		}
		var desired corev1.PodStatus
		if ownerKind(pod) == "ReplicaSet" {
			desired = k.appStatus(pod)
		} else {
			k.Run.ensure(pod)
			desired = k.runStatus(pod, k.Run.snapshot(pod.UID))
		}
		if sameJSON(statusKey(pod.Status), statusKey(desired)) {
			continue
		}
		update := pod.DeepCopy()
		update.Status = desired
		if _, err := client.CoreV1().Pods(pod.Namespace).UpdateStatus(ctx, update, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}
	return nil
}

// statusKey is the part of a pod status the kubelet owns, without timestamps that always differ.
func statusKey(s corev1.PodStatus) any {
	type container struct {
		Name     string
		Ready    bool
		State    string
		Reason   string
		Exit     int32
		Message  string
		Restarts int32
	}
	var out struct {
		Phase      corev1.PodPhase
		IP         string
		Conditions []string
		Containers []container
		Init       []container
	}
	out.Phase, out.IP = s.Phase, s.PodIP
	for _, c := range s.Conditions {
		out.Conditions = append(out.Conditions, string(c.Type)+"="+string(c.Status))
	}
	convert := func(in []corev1.ContainerStatus) (list []container) {
		for _, c := range in {
			item := container{Name: c.Name, Ready: c.Ready, Restarts: c.RestartCount}
			switch {
			case c.State.Running != nil:
				item.State = "running"
			case c.State.Terminated != nil:
				item.State, item.Reason, item.Exit, item.Message = "terminated", c.State.Terminated.Reason, c.State.Terminated.ExitCode, c.State.Terminated.Message
			case c.State.Waiting != nil:
				item.State, item.Reason = "waiting", c.State.Waiting.Reason
			}
			list = append(list, item)
		}
		return list
	}
	out.Containers, out.Init = convert(s.ContainerStatuses), convert(s.InitContainerStatuses)
	return out
}

func conditions(scheduled, initialized, containersReady, ready bool) []corev1.PodCondition {
	now := metav1.Now()
	status := func(v bool) corev1.ConditionStatus {
		if v {
			return corev1.ConditionTrue
		}
		return corev1.ConditionFalse
	}
	return []corev1.PodCondition{
		{Type: corev1.PodScheduled, Status: status(scheduled), LastTransitionTime: now},
		{Type: corev1.PodInitialized, Status: status(initialized), LastTransitionTime: now},
		{Type: corev1.ContainersReady, Status: status(containersReady), LastTransitionTime: now},
		{Type: corev1.PodReady, Status: status(ready), LastTransitionTime: now},
	}
}

// appStatus models the toy application: the image must exist, and the readiness
// probe must name a path the application serves.
func (k *Kubelet) appStatus(pod *corev1.Pod) corev1.PodStatus {
	now := metav1.Now()
	status := corev1.PodStatus{HostIP: "127.0.0.1", StartTime: &now}
	pullFails, notReady := false, false
	for _, c := range pod.Spec.Containers {
		if strings.Contains(c.Image, "nonexistent") {
			pullFails = true
		}
		if probe := c.ReadinessProbe; probe != nil && probe.HTTPGet != nil && !servedPath(probe.HTTPGet.Path) {
			notReady = true
		}
	}
	if pullFails {
		status.Phase = corev1.PodPending
		status.Conditions = conditions(true, true, false, false)
		for _, c := range pod.Spec.Containers {
			status.ContainerStatuses = append(status.ContainerStatuses, corev1.ContainerStatus{Name: c.Name, Image: c.Image, State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff", Message: "Back-off pulling image " + c.Image}}})
		}
		return status
	}
	status.Phase, status.PodIP = corev1.PodRunning, k.podIP(pod.UID)
	status.PodIPs = []corev1.PodIP{{IP: status.PodIP}}
	status.Conditions = conditions(true, true, !notReady, !notReady)
	for _, c := range pod.Spec.Containers {
		started := true
		status.ContainerStatuses = append(status.ContainerStatuses, corev1.ContainerStatus{Name: c.Name, Image: c.Image, ImageID: "e2e://" + c.Image, ContainerID: "e2e://" + string(pod.UID) + "/" + c.Name, Ready: !notReady, Started: &started, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: now}}})
	}
	k.appLog(pod, "listening on :8080")
	return status
}

func servedPath(path string) bool {
	switch path {
	case "", "/", "/ready", "/healthz", "/health":
		return true
	}
	return false
}

func (k *Kubelet) appLog(pod *corev1.Pod, line string) {
	k.logsMu.Lock()
	defer k.logsMu.Unlock()
	for _, c := range pod.Spec.Containers {
		key := pod.Namespace + "/" + pod.Name + "/" + c.Name
		if k.appLogs[key] == nil {
			buffer := &LogBuffer{}
			_, _ = buffer.Write([]byte(line + "\n"))
			k.appLogs[key] = buffer
		}
	}
}

// runStatus converts the runtime's view of a process-backed pod into pod status.
func (k *Kubelet) runStatus(pod *corev1.Pod, run podRun) corev1.PodStatus {
	now := metav1.Now()
	status := corev1.PodStatus{HostIP: "127.0.0.1", PodIP: k.podIP(pod.UID), StartTime: &now, Phase: run.phase}
	status.PodIPs = []corev1.PodIP{{IP: status.PodIP}}
	if status.Phase == "" {
		status.Phase = corev1.PodPending
	}
	convert := func(c corev1.Container, init bool) corev1.ContainerStatus {
		state := run.containers[c.Name]
		out := corev1.ContainerStatus{Name: c.Name, Image: c.Image, ImageID: "e2e://" + c.Image, ContainerID: "e2e://" + string(pod.UID) + "/" + c.Name}
		started := state.status == "running"
		switch state.status {
		case "running":
			out.State.Running = &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(state.started)}
			out.Ready, out.Started = !init, &started
		case "terminated":
			out.State.Terminated = &corev1.ContainerStateTerminated{ExitCode: int32(state.exit), Reason: state.reason, Message: state.message, StartedAt: metav1.NewTime(state.started), FinishedAt: metav1.NewTime(state.finished), ContainerID: out.ContainerID}
		default:
			out.State.Waiting = &corev1.ContainerStateWaiting{Reason: "PodInitializing"}
		}
		return out
	}
	initialized := true
	for _, c := range pod.Spec.InitContainers {
		s := convert(c, true)
		status.InitContainerStatuses = append(status.InitContainerStatuses, s)
		sidecar := c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways
		if sidecar && s.State.Running != nil {
			continue
		}
		if s.State.Terminated == nil || s.State.Terminated.ExitCode != 0 {
			initialized = false
		}
	}
	allRunning := true
	for _, c := range pod.Spec.Containers {
		s := convert(c, false)
		status.ContainerStatuses = append(status.ContainerStatuses, s)
		if s.State.Running == nil {
			allRunning = false
		}
	}
	status.Conditions = conditions(true, initialized, allRunning, allRunning)
	return status
}

// LogBuffer holds a container's output with the time each line arrived.
type LogBuffer struct {
	mu      sync.Mutex
	lines   []logLine
	partial string
	file    *os.File // optional write-through copy kept as run evidence
}

type logLine struct {
	at   time.Time
	text string
}

func (b *LogBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.file != nil {
		_, _ = b.file.Write(data)
	}
	text := b.partial + string(data)
	parts := strings.Split(text, "\n")
	b.partial = parts[len(parts)-1]
	for _, line := range parts[:len(parts)-1] {
		b.lines = append(b.lines, logLine{at: time.Now(), text: line})
	}
	if len(b.lines) > 20000 {
		b.lines = b.lines[len(b.lines)-20000:]
	}
	return len(data), nil
}

func (b *LogBuffer) render(timestamps bool, tail int, limit int64) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	lines := b.lines
	if b.partial != "" {
		lines = append(append([]logLine(nil), lines...), logLine{at: time.Now(), text: b.partial})
	}
	if tail > 0 && len(lines) > tail {
		lines = lines[len(lines)-tail:]
	}
	var out strings.Builder
	for _, line := range lines {
		if timestamps {
			out.WriteString(line.at.UTC().Format(time.RFC3339Nano) + " ")
		}
		out.WriteString(line.text + "\n")
	}
	text := out.String()
	if limit > 0 && int64(len(text)) > limit {
		text = text[:limit]
	}
	return text
}

// handler serves what the API server proxies to a kubelet: container logs and usage.
func (k *Kubelet) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/containerLogs/", func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/containerLogs/"), "/")
		if len(parts) != 3 {
			http.NotFound(w, r)
			return
		}
		query := r.URL.Query()
		if query.Get("previous") == "true" {
			http.Error(w, "previous terminated container not found", http.StatusBadRequest)
			return
		}
		tail, _ := strconv.Atoi(query.Get("tailLines"))
		limit, _ := strconv.ParseInt(query.Get("limitBytes"), 10, 64)
		buffer := k.Run.logs(parts[0], parts[1], parts[2])
		if buffer == nil {
			k.logsMu.Lock()
			buffer = k.appLogs[strings.Join(parts, "/")]
			k.logsMu.Unlock()
		}
		if buffer == nil {
			http.Error(w, "container not found", http.StatusNotFound)
			return
		}
		_, _ = fmt.Fprint(w, buffer.render(query.Get("timestamps") == "true", tail, limit))
	})
	mux.HandleFunc("/stats/summary", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		pods, err := k.cluster.Client.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		now := time.Now().UTC().Format(time.RFC3339Nano)
		var summary struct {
			Node map[string]any   `json:"node"`
			Pods []map[string]any `json:"pods"`
		}
		summary.Node = map[string]any{"nodeName": nodeName}
		for _, pod := range pods.Items {
			if pod.Spec.NodeName != nodeName {
				continue
			}
			var containers []map[string]any
			for _, status := range pod.Status.ContainerStatuses {
				if status.State.Running == nil {
					continue
				}
				containers = append(containers, map[string]any{
					"name": status.Name, "startTime": status.State.Running.StartedAt.UTC().Format(time.RFC3339Nano),
					"cpu":    map[string]any{"time": now, "usageNanoCores": 20000000},
					"memory": map[string]any{"time": now, "workingSetBytes": 16 << 20, "rssBytes": 12 << 20},
					"rootfs": map[string]any{"time": now, "usedBytes": 1 << 20}, "logs": map[string]any{"time": now, "usedBytes": 1 << 10},
				})
			}
			item := map[string]any{"podRef": map[string]any{"name": pod.Name, "namespace": pod.Namespace, "uid": string(pod.UID)}, "containers": containers,
				"ephemeral-storage": map[string]any{"time": now, "usedBytes": 2 << 20}}
			var volumes []map[string]any
			for _, v := range pod.Spec.Volumes {
				if v.PersistentVolumeClaim != nil {
					volumes = append(volumes, map[string]any{"time": now, "name": v.Name, "pvcRef": map[string]any{"name": v.PersistentVolumeClaim.ClaimName, "namespace": pod.Namespace},
						"capacityBytes": 10 << 30, "usedBytes": 1 << 20, "availableBytes": 10<<30 - 1<<20, "inodes": 1000000, "inodesUsed": 100, "inodesFree": 999900})
				}
			}
			item["volume"] = volumes
			summary.Pods = append(summary.Pods, item)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(summary)
	})
	mux.HandleFunc("/metrics/cadvisor", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		pods, err := k.cluster.Client.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var out strings.Builder
		for _, pod := range pods.Items {
			for _, status := range pod.Status.ContainerStatuses {
				if status.State.Running != nil {
					label := fmt.Sprintf(`{namespace=%q,pod=%q,container=%q}`, pod.Namespace, pod.Name, status.Name)
					fmt.Fprintf(&out, "container_cpu_cfs_periods_total%s 1000\ncontainer_cpu_cfs_throttled_periods_total%s 0\n", label, label)
				}
			}
		}
		_, _ = fmt.Fprint(w, out.String())
	})
	return mux
}

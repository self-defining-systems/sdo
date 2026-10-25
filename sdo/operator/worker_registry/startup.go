package worker_registry

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

type ContainerStartup struct {
	StartedAt   *metav1.Time `json:"started_at"`
	FinishedAt  *metav1.Time `json:"finished_at"`
	Restartable bool         `json:"restartable"`
}

// ResponderStartup is one structured controller telemetry record per launch.
// Absent observations remain explicit; failed startup never invents times.
type ResponderStartup struct {
	IncidentID              string                      `json:"incident_id"`
	JobName                 string                      `json:"job_name"`
	JobCreatedAt            *metav1.Time                `json:"job_created_at"`
	PodScheduledAt          *metav1.Time                `json:"pod_scheduled_at"`
	InitContainers          map[string]ContainerStartup `json:"init_containers"`
	AgentContainerStartedAt *metav1.Time                `json:"agent_container_started_at"`
	WorkerReady             *time.Time                  `json:"worker_ready"`
	AgentProcessStarted     *time.Time                  `json:"agent_process_started"`
	FirstModelRequestSent   *time.Time                  `json:"first_model_request_sent"`
	FirstModelRequestSource string                      `json:"first_model_request_source"`
	Missing                 []string                    `json:"missing"`
}

func (e KubernetesExecutor) startupRecord(task *contracts.AgentTask, jobName string) *ResponderStartup {
	request := new(contracts.IncidentRequest)
	if task.GetRequest().UnmarshalTo(request) != nil {
		return nil
	}
	return &ResponderStartup{IncidentID: request.GetIncidentId(), JobName: jobName, InitContainers: map[string]ContainerStartup{}, Missing: []string{}}
}

func (r *ResponderStartup) observe(job *batchv1.Job, pods []corev1.Pod) {
	if job != nil && !job.CreationTimestamp.IsZero() {
		r.JobCreatedAt = job.CreationTimestamp.DeepCopy()
	}
	for _, pod := range pods {
		for _, condition := range pod.Status.Conditions {
			if condition.Type == corev1.PodScheduled && condition.Status == corev1.ConditionTrue {
				r.PodScheduledAt = condition.LastTransitionTime.DeepCopy()
			}
		}
		for _, init := range pod.Spec.InitContainers {
			s := r.InitContainers[init.Name]
			s.Restartable = init.RestartPolicy != nil && *init.RestartPolicy == corev1.ContainerRestartPolicyAlways
			r.InitContainers[init.Name] = s
		}
		for _, status := range pod.Status.InitContainerStatuses {
			s := r.InitContainers[status.Name]
			if status.State.Running != nil {
				s.StartedAt = status.State.Running.StartedAt.DeepCopy()
			}
			if status.State.Terminated != nil {
				s.StartedAt = status.State.Terminated.StartedAt.DeepCopy()
				s.FinishedAt = status.State.Terminated.FinishedAt.DeepCopy()
			}
			r.InitContainers[status.Name] = s
		}
		for _, status := range pod.Status.ContainerStatuses {
			if status.Name != "agent" {
				continue
			}
			if status.State.Running != nil {
				r.AgentContainerStartedAt = status.State.Running.StartedAt.DeepCopy()
			}
			if status.State.Terminated != nil {
				r.AgentContainerStartedAt = status.State.Terminated.StartedAt.DeepCopy()
			}
		}
	}
}

func (e KubernetesExecutor) emitStartup(r *ResponderStartup) {
	if r == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	pods, err := e.Client.CoreV1().Pods(e.Namespace).List(ctx, metav1.ListOptions{LabelSelector: "batch.kubernetes.io/job-name=" + r.JobName})
	if err == nil {
		r.observe(nil, pods.Items)
	}
	if e.SessionPVCSubPath != "" {
		relative, err := filepath.Rel(filepath.Clean(e.RepositoryPVCSubPath+"/"), e.SessionPVCSubPath)
		if err == nil {
			data, err := os.ReadFile(filepath.Join(e.RepositoryMountPath, relative, "startup-worker.json"))
			if err == nil {
				var worker struct {
					WorkerReady           *time.Time `json:"worker_ready"`
					AgentProcessStarted   *time.Time `json:"agent_process_started"`
					FirstModelRequestSent *time.Time `json:"first_model_request_sent"`
					Source                string     `json:"first_model_request_source"`
				}
				if json.Unmarshal(data, &worker) == nil {
					r.WorkerReady = worker.WorkerReady
					r.AgentProcessStarted = worker.AgentProcessStarted
					r.FirstModelRequestSent = worker.FirstModelRequestSent
					r.FirstModelRequestSource = worker.Source
				}
			}
		}
	}
	for name, missing := range map[string]bool{"job_created_at": r.JobCreatedAt == nil, "pod_scheduled_at": r.PodScheduledAt == nil, "agent_container_started_at": r.AgentContainerStartedAt == nil, "worker_ready": r.WorkerReady == nil, "agent_process_started": r.AgentProcessStarted == nil, "first_model_request_sent": r.FirstModelRequestSent == nil} {
		if missing {
			r.Missing = append(r.Missing, name)
		}
	}
	for name, c := range r.InitContainers {
		if c.StartedAt == nil {
			r.Missing = append(r.Missing, "init."+name+".started_at")
		}
		if !c.Restartable && c.FinishedAt == nil {
			r.Missing = append(r.Missing, "init."+name+".finished_at")
		}
	}
	sort.Strings(r.Missing)
	encoded, err := json.Marshal(r)
	if err == nil {
		slog.Info("responder_startup", "startup_json", string(encoded))
	}
}

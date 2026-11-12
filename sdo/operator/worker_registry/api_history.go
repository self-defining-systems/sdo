package worker_registry

import (
	"cmp"
	"fmt"
	"path"
	"path/filepath"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sdo.dev/operator/apihistory"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/safefs"
)

// addAPIHistory fronts every agent Job's Kubernetes access with the recording proxy. The native sidecar alone
// writes the history, using the same Pod service account as the agent. A responder (incident) scope may write
// and its writes are forced to the responder field manager; every other agent gets a read-only task scope. The
// reflection agent reads the incident's own recording, so it records to a separate directory.
func (e KubernetesExecutor) addAPIHistory(job *batchv1.Job, task *contracts.AgentTask) error {
	if e.SessionPVCSubPath == "" {
		return nil
	}
	incident := task.GetRequest().MessageIs(&contracts.IncidentRequest{})
	reflection := task.GetRequest().MessageIs(&contracts.ReflectionRequest{})
	agent := &job.Spec.Template.Spec.Containers[0]
	directory := "api-history"
	if reflection {
		directory = "api-history-reflection"
	}
	if incident || reflection {
		sub := path.Join(e.SessionPVCSubPath, "api-history")
		agent.VolumeMounts = append(agent.VolumeMounts, corev1.VolumeMount{Name: "repository", MountPath: "/sdo/session/api-history", SubPath: sub, ReadOnly: true})
		agent.VolumeMounts = append(agent.VolumeMounts, corev1.VolumeMount{Name: "repository", MountPath: "/sdo/session/api-history.generation", SubPath: sub + ".generation", ReadOnly: true})
		agent.Env = append(agent.Env, corev1.EnvVar{Name: "SDO_API_HISTORY_DIR", Value: "/sdo/session/api-history"})
	}
	sub := path.Join(e.SessionPVCSubPath, directory)
	if !incident && !e.standbyMode {
		// The incident controller prepares its own directory and generation; every other scope prepares its here,
		// before the Job exists, so a failure prevents the launch and the kubelet never creates the mounts itself.
		if err := e.prepareHistory(directory); err != nil {
			return err
		}
	}
	agent.Env = append(agent.Env, corev1.EnvVar{Name: "SDO_API_PROXY_URL", Value: "http://127.0.0.1:18080"})
	image := e.APIProxyImage
	if image == "" {
		image = agent.Image
	}
	scope := "task:" + cmp.Or(task.GetTaskId(), "agent")
	if incident {
		typed := new(contracts.IncidentRequest)
		_ = task.GetRequest().UnmarshalTo(typed)
		scope = "incident:" + typed.GetIncidentId()
	}
	always := corev1.ContainerRestartPolicyAlways
	sidecar := corev1.Container{
		Name: "kube-api-history", Image: image,
		Command:         []string{"/usr/local/bin/sdo-operator", "api-proxy", "--scope", scope, "--directory", "/sdo/history"},
		RestartPolicy:   &always,
		StartupProbe:    &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"/usr/local/bin/sdo-operator", "api-proxy", "ready"}}}, PeriodSeconds: 1, TimeoutSeconds: 2, FailureThreshold: 60},
		VolumeMounts:    []corev1.VolumeMount{{Name: "repository", MountPath: "/sdo/history", SubPath: sub}, {Name: "repository", MountPath: "/sdo/history.generation", SubPath: sub + ".generation"}},
		SecurityContext: agent.SecurityContext.DeepCopy(),
		Resources:       corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m"), corev1.ResourceMemory: resource.MustParse("32Mi")}, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("256Mi")}},
	}
	job.Spec.Template.Spec.InitContainers = append(job.Spec.Template.Spec.InitContainers, sidecar)
	return nil
}

// prepareHistory creates one history directory and its generation boundary on the controller's repository mount.
func (e KubernetesExecutor) prepareHistory(directory string) error {
	if e.RepositoryMountPath == "" {
		return nil
	}
	relative, err := filepath.Rel(sessionPVCBase(e.RepositoryPVCSubPath), e.SessionPVCSubPath)
	if err != nil {
		return fmt.Errorf("api history: %w", err)
	}
	if err := safefs.MkdirAll(e.RepositoryMountPath, filepath.Join(relative, directory), 0700); err != nil {
		return fmt.Errorf("api history: %w", err)
	}
	if err := apihistory.BeginGeneration(filepath.Join(e.RepositoryMountPath, relative, directory)); err != nil {
		return fmt.Errorf("api history generation: %w", err)
	}
	return nil
}

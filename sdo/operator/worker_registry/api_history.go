package worker_registry

import (
	"path"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// addAPIHistory gives workers read-only incident observations. The native
// sidecar alone writes them, using the same Pod service account as the agent.
func (e KubernetesExecutor) addAPIHistory(job *batchv1.Job, task *contracts.AgentTask) {
	if e.SessionPVCSubPath == "" {
		return
	}
	incident := task.GetRequest().MessageIs(&contracts.IncidentRequest{})
	reflection := task.GetRequest().MessageIs(&contracts.ReflectionRequest{})
	if !incident && !reflection {
		return
	}
	agent := &job.Spec.Template.Spec.Containers[0]
	sub := path.Join(e.SessionPVCSubPath, "api-history")
	agent.VolumeMounts = append(agent.VolumeMounts, corev1.VolumeMount{Name: "repository", MountPath: "/sdo/session/api-history", SubPath: sub, ReadOnly: true})
	agent.VolumeMounts = append(agent.VolumeMounts, corev1.VolumeMount{Name: "repository", MountPath: "/sdo/session/api-history.generation", SubPath: sub + ".generation", ReadOnly: true})
	agent.Env = append(agent.Env, corev1.EnvVar{Name: "SDO_API_HISTORY_DIR", Value: "/sdo/session/api-history"})
	if !incident {
		return
	}
	agent.Env = append(agent.Env, corev1.EnvVar{Name: "SDO_API_PROXY_URL", Value: "http://127.0.0.1:18080"})
	image := e.APIProxyImage
	if image == "" {
		image = agent.Image
	}
	typed := new(contracts.IncidentRequest)
	_ = task.GetRequest().UnmarshalTo(typed)
	always := corev1.ContainerRestartPolicyAlways
	sidecar := corev1.Container{
		Name: "kube-api-history", Image: image,
		Command:         []string{"/usr/local/bin/sdo-operator", "api-proxy", "--incident", typed.GetIncidentId(), "--directory", "/sdo/history"},
		RestartPolicy:   &always,
		StartupProbe:    &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"/usr/local/bin/sdo-operator", "api-proxy", "ready"}}}, PeriodSeconds: 1, TimeoutSeconds: 2, FailureThreshold: 60},
		VolumeMounts:    []corev1.VolumeMount{{Name: "repository", MountPath: "/sdo/history", SubPath: sub}, {Name: "repository", MountPath: "/sdo/history.generation", SubPath: sub + ".generation"}},
		SecurityContext: agent.SecurityContext.DeepCopy(),
		Resources:       corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m"), corev1.ResourceMemory: resource.MustParse("32Mi")}, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("256Mi")}},
	}
	job.Spec.Template.Spec.InitContainers = append(job.Spec.Template.Spec.InitContainers, sidecar)
}

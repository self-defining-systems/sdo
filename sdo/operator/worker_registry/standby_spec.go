package worker_registry

import (
	"encoding/json"
	"path"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func (e KubernetesExecutor) configureStandby(job *batchv1.Job, launch *contracts.KubernetesJobLaunch) {
	pod := &job.Spec.Template.Spec
	command, _ := json.Marshal(launch.GetCommand())
	agent := &pod.Containers[0]
	agent.Command = []string{"/usr/local/bin/sdo-operator", "standby", "--command-json", string(command)}
	agent.VolumeMounts = append(agent.VolumeMounts, corev1.VolumeMount{Name: "repository", MountPath: "/sdo/standby/ready", SubPath: path.Join(e.standbySubPath, "ready")})
	agent.WorkingDir = "/sdo/proposal"
	for i := range pod.InitContainers {
		sidecar := &pod.InitContainers[i]
		if sidecar.Name != "kube-api-history" {
			continue
		}
		sidecar.Command = []string{"/usr/local/bin/sdo-operator", "standby-history", "--directory", "/sdo/history"}
		sidecar.VolumeMounts = append(sidecar.VolumeMounts,
			corev1.VolumeMount{Name: "repository", MountPath: "/sdo/request", SubPath: path.Join(e.standbySubPath, "control"), ReadOnly: true},
			corev1.VolumeMount{Name: "repository", MountPath: "/sdo/standby/ready", SubPath: path.Join(e.standbySubPath, "ready")})
		sidecar.StartupProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"/usr/local/bin/sdo-operator", "standby-ready", "/sdo/standby/ready/history"}}}, PeriodSeconds: 1, TimeoutSeconds: 2, FailureThreshold: 60}
	}
	job.Spec.Template.Labels["sdo.dev/standby"] = "true"
}

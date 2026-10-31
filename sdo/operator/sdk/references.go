package sdk

import (
	corev1 "k8s.io/api/core/v1"
	"sort"
)

// PodTemplateReference identifies a consumed ConfigMap, Secret, PVC or ServiceAccount.
// Key is empty for whole-object references. Required wins for duplicate references.
type PodTemplateReference struct {
	Kind     string `json:"kind"`
	Name     string `json:"name"`
	Key      string `json:"key,omitempty"`
	Optional bool   `json:"optional"`
}

// PodTemplateReferences returns sorted distinct dependencies of any workload pod
// template, including projected volumes, init/ephemeral containers and image pulls.
// Pass CronJob.Spec.JobTemplate.Spec.Template for CronJobs. An empty service account
// name means Kubernetes' default ServiceAccount.
func PodTemplateReferences(template corev1.PodTemplateSpec) []PodTemplateReference {
	refs := map[[3]string]bool{}
	add := func(kind, name, key string, optional *bool) {
		if name == "" {
			return
		}
		id := [3]string{kind, name, key}
		value := optional != nil && *optional
		if old, ok := refs[id]; !ok || old && !value {
			refs[id] = value
		}
	}
	items := func(kind, name string, keys []corev1.KeyToPath, optional *bool) {
		add(kind, name, "", optional)
		for _, key := range keys {
			add(kind, name, key.Key, optional)
		}
	}
	for _, volume := range template.Spec.Volumes {
		if v := volume.ConfigMap; v != nil {
			items("ConfigMap", v.Name, v.Items, v.Optional)
		}
		if v := volume.Secret; v != nil {
			items("Secret", v.SecretName, v.Items, v.Optional)
		}
		if v := volume.PersistentVolumeClaim; v != nil {
			add("PersistentVolumeClaim", v.ClaimName, "", nil)
		}
		if v := volume.Projected; v != nil {
			for _, source := range v.Sources {
				if r := source.ConfigMap; r != nil {
					items("ConfigMap", r.Name, r.Items, r.Optional)
				}
				if r := source.Secret; r != nil {
					items("Secret", r.Name, r.Items, r.Optional)
				}
			}
		}
	}
	container := func(env []corev1.EnvVar, from []corev1.EnvFromSource) {
		for _, source := range from {
			if r := source.ConfigMapRef; r != nil {
				add("ConfigMap", r.Name, "", r.Optional)
			}
			if r := source.SecretRef; r != nil {
				add("Secret", r.Name, "", r.Optional)
			}
		}
		for _, env := range env {
			if env.ValueFrom == nil {
				continue
			}
			if r := env.ValueFrom.ConfigMapKeyRef; r != nil {
				add("ConfigMap", r.Name, r.Key, r.Optional)
			}
			if r := env.ValueFrom.SecretKeyRef; r != nil {
				add("Secret", r.Name, r.Key, r.Optional)
			}
		}
	}
	for _, c := range template.Spec.Containers {
		container(c.Env, c.EnvFrom)
	}
	for _, c := range template.Spec.InitContainers {
		container(c.Env, c.EnvFrom)
	}
	for _, c := range template.Spec.EphemeralContainers {
		container(c.Env, c.EnvFrom)
	}
	for _, r := range template.Spec.ImagePullSecrets {
		add("Secret", r.Name, "", nil)
	}
	account := template.Spec.ServiceAccountName
	if account == "" {
		account = "default"
	}
	add("ServiceAccount", account, "", nil)
	result := make([]PodTemplateReference, 0, len(refs))
	for id, optional := range refs {
		result = append(result, PodTemplateReference{Kind: id[0], Name: id[1], Key: id[2], Optional: optional})
	}
	sort.Slice(result, func(i, j int) bool {
		a, b := result[i], result[j]
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		return a.Key < b.Key
	})
	return result
}

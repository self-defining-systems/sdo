package sdk

import (
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

// ConfigMapKeyReference identifies an explicitly selected ConfigMap key.
// Optional follows Kubernetes semantics: a missing optional key is not a fault.
type ConfigMapKeyReference struct {
	Name     string `json:"name"`
	Key      string `json:"key"`
	Optional bool   `json:"optional"`
}

// ConfigMapKeyReferencesForDeployment returns explicit keys consumed through
// container/init-container env, ConfigMap volumes, and projected volumes.
// EnvFrom and volumes without Items consume all available keys and therefore
// impose no individual key requirement. Secret references are never returned.
// Results are sorted by name/key; required references win over optional ones.
// This is reference metadata only, not a credential-safety assertion about data.
func ConfigMapKeyReferencesForDeployment(deployment appsv1.Deployment) []ConfigMapKeyReference {
	type key struct{ name, field string }
	references := make(map[key]bool)
	add := func(name, field string, optional *bool) {
		if name == "" || field == "" {
			return
		}
		k := key{name, field}
		isOptional := optional != nil && *optional
		current, exists := references[k]
		if !exists || current && !isOptional {
			references[k] = isOptional
		}
	}
	containers := func(items []corev1.Container) {
		for _, container := range items {
			for _, env := range container.Env {
				if env.ValueFrom != nil && env.ValueFrom.ConfigMapKeyRef != nil {
					ref := env.ValueFrom.ConfigMapKeyRef
					add(ref.Name, ref.Key, ref.Optional)
				}
			}
		}
	}
	containers(deployment.Spec.Template.Spec.Containers)
	containers(deployment.Spec.Template.Spec.InitContainers)
	for _, volume := range deployment.Spec.Template.Spec.Volumes {
		if ref := volume.ConfigMap; ref != nil {
			for _, item := range ref.Items {
				add(ref.Name, item.Key, ref.Optional)
			}
		}
		if volume.Projected != nil {
			for _, source := range volume.Projected.Sources {
				if ref := source.ConfigMap; ref != nil {
					for _, item := range ref.Items {
						add(ref.Name, item.Key, ref.Optional)
					}
				}
			}
		}
	}
	result := make([]ConfigMapKeyReference, 0, len(references))
	for k, optional := range references {
		result = append(result, ConfigMapKeyReference{Name: k.name, Key: k.field, Optional: optional})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Name != result[j].Name {
			return result[i].Name < result[j].Name
		}
		return result[i].Key < result[j].Key
	})
	return result
}

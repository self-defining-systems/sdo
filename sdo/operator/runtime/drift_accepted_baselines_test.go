package runtime

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func scriptedBaselineDefaults(source *unstructured.Unstructured) *unstructured.Unstructured {
	live := source.DeepCopy()
	switch live.GetKind() {
	case "Deployment":
		spec, _, _ := unstructured.NestedMap(live.Object, "spec", "template", "spec")
		for key, value := range map[string]any{"dnsPolicy": "ClusterFirst", "restartPolicy": "Always", "schedulerName": "default-scheduler", "terminationGracePeriodSeconds": int64(30)} {
			if _, found := spec[key]; !found {
				spec[key] = value
			}
		}
		_ = unstructured.SetNestedMap(live.Object, spec, "spec", "template", "spec")
	case "Service":
		for key, value := range map[string]any{"type": "ClusterIP", "sessionAffinity": "None", "internalTrafficPolicy": "Cluster"} {
			if _, found, _ := unstructured.NestedFieldNoCopy(live.Object, "spec", key); !found {
				_ = unstructured.SetNestedField(live.Object, value, "spec", key)
			}
		}
	}
	return live
}

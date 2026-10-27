package runtime

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sdo.dev/operator/sdk"
)

// sourceBookkeepingAnnotations is the single annotation exclusion policy for
// comparisons, acceptance records and replay. Do not ignore whole annotation
// domains: user-authored scheduling and security annotations can be repairs.
var sourceBookkeepingAnnotations = []string{
	"sdo.dev/source-tree",
	acceptedPathsAnnotation,
	"deployment.kubernetes.io/revision",
	"kubectl.kubernetes.io/last-applied-configuration",
	"kubectl.kubernetes.io/restartedAt",
}

func sourceBookkeepingAnnotation(name string) bool {
	return slices.Contains(sourceBookkeepingAnnotations, name)
}

// HPA ownership comes from the captured target identity, not a manager name or
// a live-only API read; offline replay uses exactly the same evidence.
func (v LiveReadView) hpaOwnsReplicas(ref sdk.ObjectRef) bool {
	groupVersion, err := schema.ParseGroupVersion(ref.APIVersion)
	if err != nil {
		return false
	}
	for _, hpa := range v.HorizontalPodAutoscalers() {
		target := hpa.Spec.ScaleTargetRef
		targetVersion, err := schema.ParseGroupVersion(target.APIVersion)
		if err == nil && hpa.Namespace == ref.Namespace && target.Name == ref.Name && target.Kind == ref.Kind && targetVersion.Group == groupVersion.Group {
			return true
		}
	}
	return false
}

func (v LiveReadView) sourceComparableFields(object map[string]any, ref sdk.ObjectRef) map[string]any {
	result := comparableFields(object)
	if v.hpaOwnsReplicas(ref) {
		unstructured.RemoveNestedField(result, "spec", "replicas")
	}
	return result
}

func (v LiveReadView) sourceDriftPaths(paths []string, ref sdk.ObjectRef) []string {
	if v.hpaOwnsReplicas(ref) {
		return slices.DeleteFunc(paths, func(path string) bool { return path == "spec.replicas" })
	}
	return paths
}

// canonicalQuantity retains invalid values so they remain visible as drift.
func canonicalQuantity(value any) any {
	text, ok := value.(string)
	if !ok {
		return value
	}
	quantity, err := resource.ParseQuantity(text)
	if err != nil {
		return value
	}
	decimal := quantity.AsDec().String()
	if strings.Contains(decimal, ".") {
		decimal = strings.TrimRight(strings.TrimRight(decimal, "0"), ".")
	}
	return decimal
}

// Normalize only Kubernetes quantity fields. Arbitrary strings such as env
// values, image tags and ConfigMap payloads must retain their exact meaning.
func normalizeSourceQuantities(object map[string]any) {
	visitSourceQuantityLists(object, "", func(_ string, values map[string]any) {
		for key, value := range values {
			values[key] = canonicalQuantity(value)
		}
	})
}

// The schema walk is shared by object and historical acceptance-value
// normalization, so the two paths cannot disagree about which strings are quantities.
func visitSourceQuantityLists(object map[string]any, prefix string, visit func(string, map[string]any)) {
	resourceList := func(value any, path string) { values, _ := value.(map[string]any); visit(path, values) }
	resources := func(node map[string]any, path string) {
		r, _ := node["resources"].(map[string]any)
		resourceList(r["requests"], path+".resources.requests")
		resourceList(r["limits"], path+".resources.limits")
	}
	podSpec := func(spec map[string]any, path string) {
		resources(spec, path)
		resourceList(spec["overhead"], path+".overhead")
		for _, field := range []string{"containers", "initContainers", "ephemeralContainers"} {
			containers, _ := spec[field].([]any)
			for index, container := range containers {
				if c, ok := container.(map[string]any); ok {
					resources(c, fmt.Sprintf("%s.%s[%d]", path, field, index))
				}
			}
		}
	}
	spec, _ := object["spec"].(map[string]any)
	switch object["kind"] {
	case "Pod":
		podSpec(spec, prefix+"spec")
	case "Deployment", "StatefulSet", "DaemonSet", "ReplicaSet", "ReplicationController", "Job":
		template, _ := spec["template"].(map[string]any)
		templateSpec, _ := template["spec"].(map[string]any)
		podSpec(templateSpec, prefix+"spec.template.spec")
		claims, _ := spec["volumeClaimTemplates"].([]any)
		for index, claim := range claims {
			if c, ok := claim.(map[string]any); ok {
				s, _ := c["spec"].(map[string]any)
				resources(s, fmt.Sprintf("%sspec.volumeClaimTemplates[%d].spec", prefix, index))
			}
		}
	case "CronJob":
		job, _ := spec["jobTemplate"].(map[string]any)
		visitSourceQuantityLists(map[string]any{"kind": "Job", "spec": job["spec"]}, prefix+"spec.jobTemplate.", visit)
	case "PersistentVolumeClaim":
		resources(spec, prefix+"spec")
	case "PersistentVolume":
		resourceList(spec["capacity"], prefix+"spec.capacity")
	}
}

func normalizedAcceptanceValues(observed *unstructured.Unstructured, values map[string]any) map[string]any {
	result := maps.Clone(values)
	visitSourceQuantityLists(observed.Object, "", func(path string, quantities map[string]any) {
		for key := range quantities {
			if value, ok := result[path+"."+key]; ok {
				result[path+"."+key] = canonicalQuantity(value)
			}
		}
	})
	return result
}

package rendered

import (
	"errors"
	"fmt"
	"sort"
)

// podKinds is the one table of built-in kinds that carry a pod spec and where
// the pod spec lives. Every consumer that reads, stamps, pins, resolves,
// attributes or gates container images (Normalize, ImageSites,
// CheckExpectedWorkloads, deployment.Stamp, receipt pins, the shape digest,
// observed-workload parsing) goes through it; a kind missing here runs images
// that no check covers. TestPodTemplateTableIsTheOnlyLocator enforces that no
// other source file spells out a pod-spec location.
//
// Custom resources that embed pod specs are not in the table: they are
// outside the kinds SDO can substitute or observe.
var podKinds = map[string][]string{
	"Deployment":            {"spec", "template", "spec"},
	"StatefulSet":           {"spec", "template", "spec"},
	"DaemonSet":             {"spec", "template", "spec"},
	"ReplicaSet":            {"spec", "template", "spec"},
	"ReplicationController": {"spec", "template", "spec"},
	"Job":                   {"spec", "template", "spec"},
	"CronJob":               {"spec", "jobTemplate", "spec", "template", "spec"},
	"Pod":                   {"spec"},
}

// ContainerFields are the pod-spec lists that hold images, in apply order.
var ContainerFields = [...]string{InitContainers, Containers}

// The pod-spec container lists.
const (
	InitContainers = "initContainers"
	Containers     = "containers"
)

// WorkloadKind reports whether kind is a pod-bearing workload whose container
// images the deployment path substitutes and locks.
func WorkloadKind(kind string) bool {
	_, ok := podKinds[kind]
	return ok
}

// PodKinds lists the table's kinds, sorted.
func PodKinds() []string {
	kinds := make([]string, 0, len(podKinds))
	for kind := range podKinds {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	return kinds
}

// PodSpecPath returns the field path of the pod spec of kind (a copy).
func PodSpecPath(kind string) ([]string, bool) {
	path, ok := podKinds[kind]
	return append([]string(nil), path...), ok
}

// PodSpec returns the pod spec of a pod-bearing object (a reference into obj).
func PodSpec(obj map[string]any) (map[string]any, error) {
	kind, _ := obj["kind"].(string)
	path, ok := podKinds[kind]
	if !ok {
		return nil, fmt.Errorf("kind %q carries no pod spec", kind)
	}
	return walk(obj, path, kind)
}

// PodTemplateMetadata returns the metadata map of the pod the object creates:
// the template metadata of a controller, the object's own metadata for a Pod.
// With create, a missing template metadata map is added (stamping); without,
// it is returned nil when absent.
func PodTemplateMetadata(obj map[string]any, create bool) (map[string]any, error) {
	kind, _ := obj["kind"].(string)
	path, ok := podKinds[kind]
	if !ok {
		return nil, fmt.Errorf("kind %q carries no pod spec", kind)
	}
	if kind == "Pod" {
		metadata, ok := obj["metadata"].(map[string]any)
		if !ok {
			return nil, errors.New("Pod needs metadata")
		}
		return metadata, nil
	}
	template, err := walk(obj, path[:len(path)-1], kind)
	if err != nil {
		return nil, err
	}
	metadata, _ := template["metadata"].(map[string]any)
	if metadata == nil && create {
		metadata = map[string]any{}
		template["metadata"] = metadata
	}
	return metadata, nil
}

func walk(obj map[string]any, path []string, kind string) (map[string]any, error) {
	current := obj
	for _, field := range path {
		next, ok := current[field].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%s needs %s", kind, field)
		}
		current = next
	}
	return current, nil
}

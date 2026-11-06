package rendered

import (
	"errors"

	"sdo.dev/operator/sdk"
)

// Normalize applies the namespace default and cluster-scope clearing to
// obj in place and validates the workload shape. It returns the object's
// reference and whether it is a workload. It neither stamps the source tree
// nor substitutes images; those are deployment concerns (deployment.Stamp).
func Normalize(obj map[string]any, namespace string) (ref sdk.ObjectRef, workload bool, err error) {
	kind, _ := obj["kind"].(string)
	metadata, ok := obj["metadata"].(map[string]any)
	if !ok || kind == "" {
		return sdk.ObjectRef{}, false, errors.New("Kubernetes document needs kind and metadata")
	}
	name, _ := metadata["name"].(string)
	if name == "" {
		return sdk.ObjectRef{}, false, errors.New("Kubernetes resource needs metadata.name")
	}
	ns, _ := metadata["namespace"].(string)
	if ClusterScopedKind(kind) {
		ns = ""
		delete(metadata, "namespace")
	} else if ns == "" {
		ns = namespace
		metadata["namespace"] = ns
	}
	apiVersion, _ := obj["apiVersion"].(string)
	ref = sdk.ObjectRef{APIVersion: apiVersion, Kind: kind, Namespace: ns, Name: name}
	if !WorkloadKind(kind) {
		return ref, false, nil
	}
	podSpec, err := PodSpec(obj)
	if err != nil {
		return sdk.ObjectRef{}, false, err
	}
	count := 0
	for _, field := range ContainerFields {
		containers, _ := podSpec[field].([]any)
		for _, raw := range containers {
			if _, ok := raw.(map[string]any); !ok {
				return sdk.ObjectRef{}, false, errors.New("workload has invalid container")
			}
			count++
		}
	}
	if count == 0 {
		return sdk.ObjectRef{}, false, errors.New("workload has no immutable container images")
	}
	return ref, true, nil
}

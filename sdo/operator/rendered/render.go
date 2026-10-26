package rendered

import "errors"

// RenderManifest applies the namespace, source revision, and deployed image
// substitutions used by the raw deployment strategy.
func RenderManifest(document map[string]any, defaultNamespace, tree string, imageFor func(kind, name, container string, init bool, sourceImage string) string) (string, []string, error) {
	kind, _ := document["kind"].(string)
	metadata, ok := document["metadata"].(map[string]any)
	if !ok || kind == "" {
		return "", nil, errors.New("Kubernetes document needs kind and metadata")
	}
	name, _ := metadata["name"].(string)
	if name == "" {
		return "", nil, errors.New("Kubernetes resource needs metadata.name")
	}
	namespace, _ := metadata["namespace"].(string)
	if ClusterScopedKind(kind) {
		namespace = ""
		delete(metadata, "namespace")
	} else if namespace == "" {
		namespace = defaultNamespace
		metadata["namespace"] = namespace
	}
	annotations := nestedMap(metadata, "annotations")
	if tree != "" {
		annotations["sdo.dev/source-tree"] = tree
	}
	if kind != "Deployment" && kind != "StatefulSet" && kind != "DaemonSet" && kind != "Job" {
		return "", nil, nil
	}
	spec, ok := document["spec"].(map[string]any)
	if !ok {
		return "", nil, errors.New("workload needs spec")
	}
	template, ok := spec["template"].(map[string]any)
	if !ok {
		return "", nil, errors.New("workload needs pod template")
	}
	if tree != "" {
		nestedMap(nestedMap(template, "metadata"), "annotations")["sdo.dev/source-tree"] = tree
	}
	podSpec, ok := template["spec"].(map[string]any)
	if !ok {
		return "", nil, errors.New("workload needs pod spec")
	}
	images := []string{}
	for _, field := range []string{"initContainers", "containers"} {
		containers, _ := podSpec[field].([]any)
		for _, raw := range containers {
			container, ok := raw.(map[string]any)
			if !ok {
				return "", nil, errors.New("workload has invalid container")
			}
			image, _ := container["image"].(string)
			if imageFor != nil {
				containerName, _ := container["name"].(string)
				image = imageFor(kind, name, containerName, field == "initContainers", image)
				container["image"] = image
			}
			images = append(images, image)
		}
	}
	if len(images) == 0 {
		return "", nil, errors.New("workload has no immutable container images")
	}
	return kind + "/" + namespace + "/" + name, images, nil
}

func nestedMap(parent map[string]any, name string) map[string]any {
	value, ok := parent[name].(map[string]any)
	if !ok {
		value = map[string]any{}
		parent[name] = value
	}
	return value
}

package deployment

import (
	"errors"

	"sdo.dev/operator/rendered"
)

// ImageSite identifies one container image reference in a workload.
type ImageSite struct {
	Kind      string
	Name      string
	Container string
	Init      bool
	Image     string
}

// Stamp records the source tree on an already-normalized object (and on the
// pod template of a workload), substitutes container images through imageFor
// when it is non-nil, and returns the resulting images in init-then-regular
// order. Non-workloads return no images. Stamping is a deploy-only step:
// reads render the object unstamped.
func Stamp(obj map[string]any, tree rendered.BaseTree, imageFor func(ImageSite) string) ([]string, error) {
	kind, _ := obj["kind"].(string)
	metadata, ok := obj["metadata"].(map[string]any)
	if !ok {
		return nil, errors.New("Kubernetes document needs metadata")
	}
	name, _ := metadata["name"].(string)
	if tree != "" {
		nestedMap(metadata, "annotations")["sdo.dev/source-tree"] = string(tree)
	}
	if !rendered.WorkloadKind(kind) {
		return nil, nil
	}
	spec, ok := obj["spec"].(map[string]any)
	if !ok {
		return nil, errors.New("workload needs spec")
	}
	template, ok := spec["template"].(map[string]any)
	if !ok {
		return nil, errors.New("workload needs pod template")
	}
	if tree != "" {
		nestedMap(nestedMap(template, "metadata"), "annotations")["sdo.dev/source-tree"] = string(tree)
	}
	podSpec, ok := template["spec"].(map[string]any)
	if !ok {
		return nil, errors.New("workload needs pod spec")
	}
	images := []string{}
	for _, field := range []string{"initContainers", "containers"} {
		containers, _ := podSpec[field].([]any)
		for _, raw := range containers {
			container, ok := raw.(map[string]any)
			if !ok {
				return nil, errors.New("workload has invalid container")
			}
			image, _ := container["image"].(string)
			if imageFor != nil {
				containerName, _ := container["name"].(string)
				image = imageFor(ImageSite{Kind: kind, Name: name, Container: containerName, Init: field == "initContainers", Image: image})
				container["image"] = image
			}
			images = append(images, image)
		}
	}
	return images, nil
}

func nestedMap(parent map[string]any, name string) map[string]any {
	value, ok := parent[name].(map[string]any)
	if !ok {
		value = map[string]any{}
		parent[name] = value
	}
	return value
}

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

// Stamp records the effective source tree ID on an already-normalized object's
// metadata (annotations, except a Deployment's labels: the API server bumps
// a Deployment's generation for any annotation change; never in a pod template: the tree changes with every upstream
// commit, and a template stamp would roll every workload, datastores included,
// on every advance; a workload's pods change only with its own spec and image
// pins, and attribute to the stamped owner), substitutes container images through imageFor
// when it is non-nil, and returns the resulting images in init-then-regular
// order. Non-workloads return no images. Stamping is a deploy-only step:
// reads render the object unstamped.
func Stamp(obj map[string]any, tree string, imageFor func(ImageSite) string) ([]string, error) {
	kind, _ := obj["kind"].(string)
	metadata, ok := obj["metadata"].(map[string]any)
	if !ok {
		return nil, errors.New("Kubernetes document needs metadata")
	}
	name, _ := metadata["name"].(string)
	if tree != "" {
		if rendered.StampInLabels(kind) {
			labels := nestedMap(metadata, "labels")
			for key, value := range rendered.SourceTreeLabels(tree) {
				labels[key] = value
			}
		} else {
			nestedMap(metadata, "annotations")[rendered.SourceTreeKey] = tree
		}
	}
	if !rendered.WorkloadKind(kind) {
		return nil, nil
	}
	podSpec, err := rendered.PodSpec(obj)
	if err != nil {
		return nil, err
	}
	images := []string{}
	for _, field := range rendered.ContainerFields {
		containers, _ := podSpec[field].([]any)
		for _, raw := range containers {
			container, ok := raw.(map[string]any)
			if !ok {
				return nil, errors.New("workload has invalid container")
			}
			image, _ := container["image"].(string)
			if imageFor != nil {
				containerName, _ := container["name"].(string)
				image = imageFor(ImageSite{Kind: kind, Name: name, Container: containerName, Init: field == rendered.InitContainers, Image: image})
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

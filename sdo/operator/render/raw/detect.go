package raw

import (
	"bytes"
	"errors"
	"io"
	"path"

	"gopkg.in/yaml.v3"

	v1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/render"
)

// Detect is the fallback root: every YAML or JSON file of the tree whose
// documents are all Kubernetes objects (apiVersion, kind and metadata.name),
// as one raw source at the tree root. A tree without such a file has none.
func Detect(fsys render.FS) ([]render.Root, error) {
	files, err := render.TreeFiles(fsys)
	if err != nil {
		return nil, err
	}
	var manifests []string
	for _, name := range files {
		switch path.Ext(name) {
		case ".yaml", ".yml", ".json":
		default:
			continue
		}
		data, err := fsys.ReadFile(name)
		if err != nil {
			return nil, err
		}
		if kubernetesObjects(data) {
			manifests = append(manifests, name)
		}
	}
	if len(manifests) == 0 {
		return nil, nil
	}
	return []render.Root{{Dir: ".", Config: &v1alpha1.RawRenderConfig{Paths: manifests}}}, nil
}

// kubernetesObjects reports whether data holds at least one document and every
// non-empty document is a Kubernetes object.
func kubernetesObjects(data []byte) bool {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	found := false
	for {
		var document map[string]any
		err := decoder.Decode(&document)
		if errors.Is(err, io.EOF) {
			return found
		}
		if err != nil {
			return false
		}
		if document == nil {
			continue
		}
		metadata, _ := document["metadata"].(map[string]any)
		name, _ := metadata["name"].(string)
		apiVersion, _ := document["apiVersion"].(string)
		kind, _ := document["kind"].(string)
		if apiVersion == "" || kind == "" || name == "" {
			return false
		}
		found = true
	}
}

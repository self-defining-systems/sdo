// Package raw renders the manifests a deployment names by path. It is the
// pure form of the selection the accepted-source reader has always applied: a
// selected path is one manifest file or a directory deployed recursively, and
// every YAML or JSON document with an apiVersion, kind and name is an object.
// Namespace defaulting, deduplication and Secret classification beyond the
// SecretsOmit projection belong to core.
package raw

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	v1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/render"
)

// ID is the renderer ID under which Register adds the raw renderer.
const ID = "raw"

// Doc tells an agent what the raw renderer does and when to choose it.
const Doc = `Deploys plain Kubernetes manifests exactly as they are committed. Choose it when the source tree holds ready-to-apply YAML or JSON manifests and needs no templating or overlays.
Config: paths, a list of tree-relative manifest files or directories. A directory is deployed recursively; every YAML or JSON document with apiVersion, kind and metadata.name becomes an object. List only what must be deployed (for example the deploy manifests, not CI or docs files).
A path that is a kustomization file or root is rejected: use the kustomize renderer for kustomization directories and the helm renderer for charts.`

// Register adds the raw renderer to c.
func Register(c *render.Catalog) error {
	return render.Register[*v1alpha1.RawRenderConfig](c, ID, render.RendererFunc[*v1alpha1.RawRenderConfig](renderRaw), render.WithDoc(Doc))
}

func renderRaw(_ context.Context, fsys render.FS, env render.Env, config *v1alpha1.RawRenderConfig) (render.Result, error) {
	files, err := selectFiles(fsys, config.GetPaths())
	if err != nil {
		return render.Result{}, err
	}
	for _, file := range files {
		if isKustomization(file) {
			return render.Result{}, fmt.Errorf("path %s is a kustomize root; use renderer kustomize", file)
		}
	}
	var objects []*unstructured.Unstructured
	for _, file := range files {
		data, err := fsys.ReadFile(file)
		if err != nil {
			return render.Result{}, fmt.Errorf("read manifest %s: %w", file, err)
		}
		decoded, err := decode(file, data)
		if err != nil {
			return render.Result{}, err
		}
		for _, object := range decoded {
			if env.Secrets == render.SecretsOmit && strings.EqualFold(object.GetKind(), "Secret") {
				object.Object = secretReference(object)
			}
			objects = append(objects, object)
		}
	}
	return render.SinglePhase(objects...), nil
}

// selectFiles resolves the selected paths to the manifest files under them,
// sorted. A path that no manifest matches, or a manifest selected twice, is an
// error.
func selectFiles(fsys render.FS, selected []string) ([]string, error) {
	if len(selected) == 0 {
		return nil, errors.New("no manifest paths are selected")
	}
	seen := map[string]bool{}
	for _, selection := range selected {
		clean := path.Clean(selection)
		if selection == "" || clean != selection || clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || strings.HasPrefix(clean, "/") {
			return nil, fmt.Errorf("invalid path %q", selection)
		}
		candidates, err := filesUnder(fsys, selection)
		if err != nil {
			return nil, err
		}
		matched := false
		for _, candidate := range candidates {
			if path.Base(candidate) != "Kustomization" && !strings.HasSuffix(candidate, ".yaml") && !strings.HasSuffix(candidate, ".yml") && !strings.HasSuffix(candidate, ".json") {
				continue
			}
			matched = true
			if seen[candidate] {
				return nil, fmt.Errorf("manifest %s is selected twice", candidate)
			}
			seen[candidate] = true
		}
		if !matched {
			return nil, fmt.Errorf("path %s has no manifests", selection)
		}
	}
	files := make([]string, 0, len(seen))
	for file := range seen {
		files = append(files, file)
	}
	sort.Strings(files)
	return files, nil
}

// filesUnder returns name when it is a file, or every file beneath it when it
// is a directory. A missing path has no files.
func filesUnder(fsys render.FS, name string) ([]string, error) {
	info, err := fsys.Stat(name)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return []string{name}, nil
	}
	entries, err := fsys.ReadDir(name)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, entry := range entries {
		below, err := filesUnder(fsys, path.Join(name, entry.Name()))
		if err != nil {
			return nil, err
		}
		out = append(out, below...)
	}
	return out, nil
}

func decode(file string, data []byte) ([]*unstructured.Unstructured, error) {
	var objects []*unstructured.Unstructured
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	for {
		var document map[string]any
		if err := decoder.Decode(&document); err == io.EOF {
			return objects, nil
		} else if err != nil {
			return nil, fmt.Errorf("decode manifest %s failed", file)
		}
		// Unstructured values must use JSON numeric types before DeepCopy or
		// Kubernetes conversion; YAML's native int values are not supported.
		encoded, err := json.Marshal(document)
		if err != nil {
			return nil, fmt.Errorf("decode manifest %s: %w", file, err)
		}
		object := &unstructured.Unstructured{}
		if err := json.Unmarshal(encoded, &object.Object); err != nil {
			return nil, fmt.Errorf("decode manifest %s: %w", file, err)
		}
		if object.GetAPIVersion() == "" || object.GetKind() == "" || object.GetName() == "" {
			continue
		}
		objects = append(objects, object)
	}
}

func isKustomization(name string) bool {
	base := path.Base(name)
	return base == "kustomization.yaml" || base == "kustomization.yml" || base == "Kustomization"
}

// secretReference keeps a Secret's identity and drops everything else.
func secretReference(object *unstructured.Unstructured) map[string]any {
	metadata := map[string]any{"name": object.GetName()}
	if namespace := object.GetNamespace(); namespace != "" {
		metadata["namespace"] = namespace
	}
	return map[string]any{"apiVersion": object.GetAPIVersion(), "kind": "Secret", "metadata": metadata}
}

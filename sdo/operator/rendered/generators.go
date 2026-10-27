package rendered

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"strings"

	"gopkg.in/yaml.v3"
	"sigs.k8s.io/kustomize/api/krusty"
	"sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/kustomize/kyaml/filesys"
)

// renderKustomization never checks out or executes accepted code. Its filesystem
// contains only declared local inputs read at the accepted Git revision. Secret
// generators are removed before loading inputs, so their files are never read.
func renderKustomization(ctx context.Context, root string, read func(context.Context, string) ([]byte, error)) ([]byte, error) {
	fs := filesys.MakeFsInMemory()
	loaded := map[string]bool{}
	var load func(string) error
	safe := func(base, name string) (string, error) {
		if name == "" || path.IsAbs(name) || strings.ContainsAny(name, ":\\") {
			return "", fmt.Errorf("only tracked local generator inputs are supported")
		}
		joined := path.Join(base, name)
		if joined == ".." || strings.HasPrefix(joined, "../") {
			return "", fmt.Errorf("generator input escapes accepted source")
		}
		return joined, nil
	}
	write := func(name string, data []byte) error {
		if err := fs.MkdirAll(path.Dir("/source/" + name)); err != nil {
			return err
		}
		return fs.WriteFile("/source/"+name, data)
	}
	load = func(name string) error {
		if loaded[name] {
			return nil
		}
		loaded[name] = true
		data, err := read(ctx, name)
		if err != nil {
			return err
		}
		base := path.Dir(name)
		if path.Base(name) != "kustomization.yaml" && path.Base(name) != "kustomization.yml" && path.Base(name) != "Kustomization" {
			// Secret manifests remain identity-only, including multi-document inputs.
			var out bytes.Buffer
			decoder := yaml.NewDecoder(bytes.NewReader(data))
			encoder := yaml.NewEncoder(&out)
			for {
				var item map[string]any
				err := decoder.Decode(&item)
				if err == io.EOF {
					break
				}
				if err != nil {
					return fmt.Errorf("decode accepted resource %s", name)
				}
				if item["kind"] == "Secret" {
					item = secretReference(item)
				}
				if err := encoder.Encode(item); err != nil {
					return err
				}
			}
			return write(name, out.Bytes())
		}
		var doc map[string]any
		if err := yaml.Unmarshal(data, &doc); err != nil {
			return fmt.Errorf("decode generator declaration %s failed", name)
		}
		allowed := map[string]bool{"apiVersion": true, "kind": true, "resources": true, "configMapGenerator": true, "secretGenerator": true, "generatorOptions": true, "namespace": true, "namePrefix": true, "nameSuffix": true, "commonLabels": true, "commonAnnotations": true, "images": true, "replicas": true, "labels": true}
		for key := range doc {
			if !allowed[key] {
				return fmt.Errorf("unsupported accepted kustomization field %s in %s", key, name)
			}
		}
		resources, _ := doc["resources"].([]any)
		secrets, _ := doc["secretGenerator"].([]any)
		globalOptions, _ := doc["generatorOptions"].(map[string]any)
		for index, raw := range secrets {
			generator, ok := raw.(map[string]any)
			if !ok {
				return fmt.Errorf("invalid Secret generator reference")
			}
			options, _ := generator["options"].(map[string]any)
			if globalOptions["disableNameSuffixHash"] != true && options["disableNameSuffixHash"] != true {
				return fmt.Errorf("accepted Secret generator requires stable names for value-free source references")
			}
			reference := secretReference(map[string]any{"apiVersion": "v1", "metadata": generator})
			encoded, err := yaml.Marshal(reference)
			if err != nil {
				return err
			}
			// Virtual resources exist only in the private render filesystem, never
			// in Git or the accepted checkout. No Secret inputs are loaded.
			virtual := fmt.Sprintf(".sdo-secret-reference-%d.yaml", index)
			if err := write(path.Join(base, virtual), encoded); err != nil {
				return err
			}
			resources = append(resources, virtual)
		}
		delete(doc, "secretGenerator")
		declaredResources, _ := doc["resources"].([]any)
		doc["resources"] = resources
		for _, raw := range declaredResources {
			resource, ok := raw.(string)
			if !ok {
				return fmt.Errorf("invalid resource in %s", name)
			}
			target, err := safe(base, resource)
			if err != nil {
				return err
			}
			if err := load(target); err != nil {
				// Directories must declare their own kustomization; never scan arbitrary files.
				found := false
				for _, file := range []string{"kustomization.yaml", "kustomization.yml", "Kustomization"} {
					if err := load(path.Join(target, file)); err == nil {
						found = true
						break
					}
				}
				if !found {
					return fmt.Errorf("accepted resource %s is unavailable", target)
				}
			}
		}
		generators, _ := doc["configMapGenerator"].([]any)
		for _, raw := range generators {
			generator, ok := raw.(map[string]any)
			if !ok {
				return fmt.Errorf("invalid ConfigMap generator")
			}
			for key := range generator {
				if key != "name" && key != "namespace" && key != "files" && key != "literals" && key != "options" && key != "behavior" {
					return fmt.Errorf("unsupported ConfigMap generator field %s", key)
				}
			}
			inputs, _ := generator["files"].([]any)
			for _, input := range inputs {
				value, ok := input.(string)
				if !ok {
					return fmt.Errorf("invalid ConfigMap file input")
				}
				_, file, hasKey := strings.Cut(value, "=")
				if !hasKey {
					file = value
				}
				target, err := safe(base, file)
				if err != nil {
					return err
				}
				content, err := read(ctx, target)
				if err != nil {
					return fmt.Errorf("tracked ConfigMap input %s unavailable", target)
				}
				if err := write(target, content); err != nil {
					return err
				}
			}
		}
		sanitized, err := yaml.Marshal(doc)
		if err != nil {
			return err
		}
		return write(name, sanitized)
	}
	if err := load(root); err != nil {
		return nil, err
	}
	options := krusty.MakeDefaultOptions()
	options.LoadRestrictions = types.LoadRestrictionsNone
	result, err := krusty.MakeKustomizer(options).Run(fs, "/source/"+path.Dir(root))
	if err != nil {
		return nil, fmt.Errorf("render accepted kustomization %s failed", root)
	}
	return result.AsYaml()
}

// Secret source is an identity reference, including metadata: last-applied
// annotations and arbitrary extension fields can contain credential bytes too.
func secretReference(object map[string]any) map[string]any {
	metadata, _ := object["metadata"].(map[string]any)
	return map[string]any{"apiVersion": object["apiVersion"], "kind": "Secret", "metadata": map[string]any{"name": metadata["name"], "namespace": metadata["namespace"]}}
}

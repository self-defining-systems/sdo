// Package kustomize is the render plugin for kustomize roots. It builds a
// kustomization with krusty over a private in-memory filesystem that holds
// only the declared, tracked, local inputs read through render.FS. Nothing is
// checked out, fetched or executed: remote bases, plugins, helm charts and
// every field outside a small allowlist are rejected before krusty sees them.
package kustomize

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strings"

	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/kustomize/api/krusty"
	"sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/kustomize/kyaml/filesys"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/render"
)

// ID is the renderer ID of this plugin.
const ID = "kustomize"

// Register adds the kustomize renderer to c under ID.
func Register(c *render.Catalog) error {
	return render.Register[*contracts.KustomizeRenderConfig](c, ID, render.RendererFunc[*contracts.KustomizeRenderConfig](renderRoot))
}

var kustomizationFiles = []string{"kustomization.yaml", "kustomization.yml", "Kustomization"}

var (
	kustomizationFields = map[string]bool{
		"apiVersion": true, "kind": true, "resources": true, "configMapGenerator": true,
		"secretGenerator": true, "generatorOptions": true, "namespace": true, "namePrefix": true,
		"nameSuffix": true, "commonLabels": true, "commonAnnotations": true, "images": true,
		"replicas": true, "labels": true,
	}
	generatorFields = map[string]bool{
		"name": true, "namespace": true, "files": true, "envs": true, "literals": true,
		"options": true, "behavior": true,
	}
	secretGeneratorFields = map[string]bool{
		"name": true, "namespace": true, "files": true, "envs": true, "literals": true,
		"options": true, "behavior": true, "type": true,
	}
)

func isKustomizationFile(name string) bool {
	base := path.Base(name)
	for _, f := range kustomizationFiles {
		if base == f {
			return true
		}
	}
	return false
}

func renderRoot(ctx context.Context, fsys render.FS, env render.Env, cfg *contracts.KustomizeRenderConfig) (render.Result, error) {
	root := path.Clean(cfg.GetPath())
	if cfg.GetPath() == "" {
		root = "."
	}
	if !fs.ValidPath(root) {
		return render.Result{}, fmt.Errorf("kustomize path %q must be a relative path inside the source tree", cfg.GetPath())
	}
	l := &loader{ctx: ctx, fsys: fsys, omit: env.Secrets == render.SecretsOmit, mem: filesys.MakeFsInMemory(), loaded: map[string]bool{}}
	if err := l.loadDir(root); err != nil {
		return render.Result{}, err
	}
	options := krusty.MakeDefaultOptions()
	// Everything krusty can load is already in the private filesystem.
	options.LoadRestrictions = types.LoadRestrictionsNone
	resmap, err := krusty.MakeKustomizer(options).Run(l.mem, path.Join("/source", root))
	if err != nil {
		if !l.omit {
			// Build errors can quote input lines, and SecretsInclude loads Secret inputs.
			return render.Result{}, fmt.Errorf("kustomize build of %s failed", root)
		}
		return render.Result{}, fmt.Errorf("kustomize build of %s failed: %w", root, err)
	}
	var objs []*unstructured.Unstructured
	for _, r := range resmap.Resources() {
		data, err := r.MarshalJSON()
		if err != nil {
			return render.Result{}, fmt.Errorf("kustomize build of %s: encode resource: %w", root, err)
		}
		obj := &unstructured.Unstructured{}
		if err := obj.UnmarshalJSON(data); err != nil {
			return render.Result{}, fmt.Errorf("kustomize build of %s: decode resource: %w", root, err)
		}
		if l.omit && obj.GetKind() == "Secret" && obj.GetAPIVersion() == "v1" {
			// A merge or replace generator with no inputs leaves the default
			// type and an empty data map; the value-free form is identity only.
			delete(obj.Object, "type")
			for _, field := range []string{"data", "stringData"} {
				if m, ok := obj.Object[field].(map[string]any); ok && len(m) == 0 {
					delete(obj.Object, field)
				}
			}
		}
		objs = append(objs, obj)
	}
	return render.SinglePhase(objs...), nil
}

type loader struct {
	ctx    context.Context
	fsys   render.FS
	omit   bool
	mem    filesys.FileSystem
	loaded map[string]bool
}

// safe joins a kustomization-relative input and rejects anything that is not a
// tracked local path inside the source tree.
func safe(base, name string) (string, error) {
	if name == "" || path.IsAbs(name) || strings.ContainsAny(name, ":\\") {
		return "", fmt.Errorf("only tracked local inputs are supported (%q)", name)
	}
	joined := path.Join(base, name)
	if !fs.ValidPath(joined) {
		return "", fmt.Errorf("input %q escapes the source tree", name)
	}
	return joined, nil
}

func (l *loader) write(name string, data []byte) error {
	target := "/source/" + name
	if err := l.mem.MkdirAll(path.Dir(target)); err != nil {
		return err
	}
	return l.mem.WriteFile(target, data)
}

func (l *loader) loadDir(dir string) error {
	if err := l.ctx.Err(); err != nil {
		return err
	}
	info, err := l.fsys.Stat(dir)
	if err != nil {
		return fmt.Errorf("kustomization directory %s: %w", dir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is a file; name the directory holding its kustomization", dir)
	}
	for _, f := range kustomizationFiles {
		name := path.Join(dir, f)
		if _, err := l.fsys.Stat(name); err == nil {
			return l.loadKustomization(name)
		} else if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	// Directories must declare their own kustomization; never scan arbitrary files.
	return fmt.Errorf("directory %s has no kustomization (%s)", dir, strings.Join(kustomizationFiles, ", "))
}

func (l *loader) loadResource(target string) error {
	info, err := l.fsys.Stat(target)
	if err != nil {
		return fmt.Errorf("resource %s unavailable: %w", target, err)
	}
	if info.IsDir() {
		return l.loadDir(target)
	}
	if isKustomizationFile(target) {
		return fmt.Errorf("resource %s is a kustomization file; name its directory", target)
	}
	return l.loadManifestFile(target)
}

// loadManifestFile copies a plain resource file. Under SecretsOmit Secret
// documents shrink to identity references, including in multi-document files.
func (l *loader) loadManifestFile(name string) error {
	if l.loaded[name] {
		return nil
	}
	l.loaded[name] = true
	data, err := l.fsys.ReadFile(name)
	if err != nil {
		return fmt.Errorf("resource %s: %w", name, err)
	}
	if !l.omit {
		return l.write(name, data)
	}
	var out strings.Builder
	decoder := yaml.NewDecoder(strings.NewReader(string(data)))
	encoder := yaml.NewEncoder(&out)
	for {
		var item map[string]any
		err := decoder.Decode(&item)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("decode resource %s: %w", name, err)
		}
		if item["kind"] == "Secret" {
			item = secretReference(item)
		}
		if err := encoder.Encode(item); err != nil {
			return err
		}
	}
	if err := encoder.Close(); err != nil {
		return err
	}
	return l.write(name, []byte(out.String()))
}

func (l *loader) loadKustomization(name string) error {
	if l.loaded[name] {
		return nil
	}
	l.loaded[name] = true
	data, err := l.fsys.ReadFile(name)
	if err != nil {
		return fmt.Errorf("kustomization %s: %w", name, err)
	}
	base := path.Dir(name)
	var doc map[string]any
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return fmt.Errorf("decode kustomization %s: %w", name, err)
	}
	for key := range doc {
		if !kustomizationFields[key] {
			return fmt.Errorf("unsupported kustomization field %s in %s", key, name)
		}
	}
	resources, err := stringList(doc["resources"], "resource", name)
	if err != nil {
		return err
	}
	declared := make([]any, 0, len(resources))
	for _, r := range resources {
		declared = append(declared, r)
	}
	secrets, err := l.loadSecretGenerators(doc, base, name)
	if err != nil {
		return err
	}
	for _, r := range resources {
		target, err := safe(base, r)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		if err := l.loadResource(target); err != nil {
			return err
		}
	}
	if err := l.loadConfigMapGenerators(doc, base, name); err != nil {
		return err
	}
	if l.omit {
		// A create generator becomes a value-free virtual resource that exists
		// only in the private filesystem, never in Git or the source tree. A
		// merge or replace generator acts on a Secret an earlier layer already
		// registered (reduced to identity), so it stays a generator without
		// inputs and keeps its behavior; a virtual resource would collide.
		var kept []any
		for index, generator := range secrets {
			if behavior, _ := generator["behavior"].(string); behavior != "" && behavior != "create" {
				kept = append(kept, secretGeneratorReference(generator, behavior))
				continue
			}
			encoded, err := yaml.Marshal(secretReference(map[string]any{"apiVersion": "v1", "metadata": generator}))
			if err != nil {
				return err
			}
			virtual := fmt.Sprintf(".sdo-secret-reference-%d.yaml", index)
			if err := l.write(path.Join(base, virtual), encoded); err != nil {
				return err
			}
			declared = append(declared, virtual)
		}
		if len(kept) > 0 {
			doc["secretGenerator"] = kept
		} else {
			delete(doc, "secretGenerator")
		}
	}
	doc["resources"] = declared
	sanitized, err := yaml.Marshal(doc)
	if err != nil {
		return err
	}
	return l.write(name, sanitized)
}

func stringList(raw any, what, file string) ([]string, error) {
	items, _ := raw.([]any)
	out := make([]string, 0, len(items))
	for _, item := range items {
		s, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("invalid %s in %s", what, file)
		}
		out = append(out, s)
	}
	return out, nil
}

// generatorInputs lists the files a generator's `files` and `envs` name.
func generatorInputs(generator map[string]any, base, file string) ([]string, error) {
	var out []string
	for _, field := range []string{"files", "envs"} {
		inputs, err := stringList(generator[field], field+" input", file)
		if err != nil {
			return nil, err
		}
		for _, value := range inputs {
			input := value
			if field == "files" {
				if _, p, keyed := strings.Cut(value, "="); keyed {
					input = p
				}
			}
			target, err := safe(base, input)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", file, err)
			}
			out = append(out, target)
		}
	}
	return out, nil
}

func (l *loader) loadConfigMapGenerators(doc map[string]any, base, name string) error {
	generators, _ := doc["configMapGenerator"].([]any)
	for _, raw := range generators {
		generator, ok := raw.(map[string]any)
		if !ok {
			return fmt.Errorf("invalid ConfigMap generator in %s", name)
		}
		for key := range generator {
			if !generatorFields[key] {
				return fmt.Errorf("unsupported ConfigMap generator field %s in %s", key, name)
			}
		}
		inputs, err := generatorInputs(generator, base, name)
		if err != nil {
			return err
		}
		for _, target := range inputs {
			content, err := l.fsys.ReadFile(target)
			if err != nil {
				return fmt.Errorf("tracked ConfigMap input %s: %w", target, err)
			}
			if err := l.write(target, content); err != nil {
				return err
			}
		}
	}
	return nil
}

// loadSecretGenerators validates every Secret generator the same way in both
// modes and returns their metadata. Under SecretsOmit the input files are only
// declared with Stat, so they stay attributable inputs without being read; a
// missing one is tolerated there and fails the SecretsInclude render.
func (l *loader) loadSecretGenerators(doc map[string]any, base, name string) ([]map[string]any, error) {
	generators, _ := doc["secretGenerator"].([]any)
	globalOptions, _ := doc["generatorOptions"].(map[string]any)
	var out []map[string]any
	for _, raw := range generators {
		generator, ok := raw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("invalid Secret generator in %s", name)
		}
		for key := range generator {
			if !secretGeneratorFields[key] {
				return nil, fmt.Errorf("unsupported Secret generator field %s in %s", key, name)
			}
		}
		options, _ := generator["options"].(map[string]any)
		if globalOptions["disableNameSuffixHash"] != true && options["disableNameSuffixHash"] != true {
			return nil, fmt.Errorf("Secret generator %v in %s needs disableNameSuffixHash: a hash of the value would differ between SecretsOmit and SecretsInclude", generator["name"], name)
		}
		inputs, err := generatorInputs(generator, base, name)
		if err != nil {
			return nil, err
		}
		for _, target := range inputs {
			if l.omit {
				if _, err := l.fsys.Stat(target); err != nil && !errors.Is(err, fs.ErrNotExist) {
					return nil, fmt.Errorf("Secret input %s: %w", target, err)
				}
				continue
			}
			content, err := l.fsys.ReadFile(target)
			if err != nil {
				return nil, fmt.Errorf("tracked Secret input %s: %w", target, err)
			}
			if err := l.write(target, content); err != nil {
				return nil, err
			}
		}
		out = append(out, generator)
	}
	return out, nil
}

// secretReference reduces a Secret to identity: last-applied annotations and
// extension fields can contain credential bytes too.
func secretReference(object map[string]any) map[string]any {
	metadata, _ := object["metadata"].(map[string]any)
	return map[string]any{"apiVersion": object["apiVersion"], "kind": "Secret", "metadata": map[string]any{"name": metadata["name"], "namespace": metadata["namespace"]}}
}

// secretGeneratorReference is a Secret generator reduced to identity and
// behavior. The name suffix hash is already required off, and no input stays.
func secretGeneratorReference(generator map[string]any, behavior string) map[string]any {
	ref := map[string]any{"name": generator["name"], "behavior": behavior, "options": map[string]any{"disableNameSuffixHash": true}}
	if ns, ok := generator["namespace"]; ok {
		ref["namespace"] = ns
	}
	return ref
}

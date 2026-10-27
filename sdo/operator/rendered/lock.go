// Package rendered owns the controller's durable, per-object deployment evidence.
// Only the Go broker publishes this sixth operational artifact class.
package rendered

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk"
)

const Directory = layout.RenderedDir
const LockPath = layout.RenderedLockFile

var errLockObject = errors.New("rendered lock object unavailable")

type ContainerImage struct {
	Name   string `json:"name"`
	Init   bool   `json:"init"`
	Digest string `json:"digest"`
}

type Object struct {
	Ref    sdk.ObjectRef    `json:"ref"`
	File   string           `json:"file,omitempty"`
	SHA256 string           `json:"sha256,omitempty"`
	Images []ContainerImage `json:"images,omitempty"`
	Error  string           `json:"error,omitempty"`
}

type Lock struct {
	Version       int      `json:"version"`
	SourceCommit  string   `json:"source_commit"`
	Namespace     string   `json:"namespace"`
	ManifestPaths []string `json:"manifest_paths"`
	Objects       []Object `json:"objects"`
}

type Snapshot struct {
	Lock  Lock
	Files map[string][]byte
}

func objectRef(o *unstructured.Unstructured) sdk.ObjectRef {
	return sdk.ObjectRef{APIVersion: o.GetAPIVersion(), Kind: o.GetKind(), Namespace: o.GetNamespace(), Name: o.GetName()}
}

func objectFile(ref sdk.ObjectRef) string {
	data, _ := json.Marshal(ref)
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x.yaml", sum)
}

// BuildLock renders every accepted object as source declares it. The lock is
// an informational snapshot of the render; nothing reads it to decide a diff,
// a reconcile or a closure.
func (s Source) BuildLock(ctx context.Context, revision, namespace string) (Snapshot, error) {
	result := Snapshot{Lock: Lock{Version: 1, SourceCommit: revision, Namespace: namespace, ManifestPaths: append([]string{}, s.ManifestPaths...), Objects: []Object{}}, Files: map[string][]byte{}}
	raw, err := s.RawManifests(ctx, revision, namespace)
	if err != nil {
		return result, err
	}
	for _, o := range raw {
		ref := objectRef(o)
		entry := Object{Ref: ref}
		object, err := s.render(o)
		if err == nil {
			entry.Images = containerImages(object)
			data, marshalErr := yaml.Marshal(object.Object)
			if marshalErr != nil {
				return result, marshalErr
			}
			entry.File = objectFile(ref)
			sum := sha256.Sum256(data)
			entry.SHA256 = fmt.Sprintf("%x", sum)
			result.Files[entry.File] = data
		} else {
			entry.Error = err.Error()
		}
		result.Lock.Objects = append(result.Lock.Objects, entry)
	}
	sort.Slice(result.Lock.Objects, func(i, j int) bool {
		return objectFile(result.Lock.Objects[i].Ref) < objectFile(result.Lock.Objects[j].Ref)
	})
	return result, nil
}

func containerImages(o *unstructured.Unstructured) []ContainerImage {
	spec, _, _ := unstructured.NestedMap(o.Object, "spec", "template", "spec")
	images := []ContainerImage{}
	for _, field := range []string{"initContainers", "containers"} {
		containers, _ := spec[field].([]any)
		for _, raw := range containers {
			container, _ := raw.(map[string]any)
			image, _ := container["image"].(string)
			name, _ := container["name"].(string)
			images = append(images, ContainerImage{Name: name, Init: field == "initContainers", Digest: image})
		}
	}
	return images
}

// Validate independently renders source at revision and requires the snapshot
// to match it exactly.
func (s Snapshot) Validate(ctx context.Context, repository, revision string) error {
	l := s.Lock
	if l.Version != 1 || l.Namespace == "" || !gitRevision.MatchString(l.SourceCommit) {
		return errors.New("invalid rendered lock header")
	}
	source := Source{Repository: repository, ManifestPaths: l.ManifestPaths}
	same, err := source.SameInputs(ctx, l.SourceCommit, revision)
	if err != nil || !same {
		return fmt.Errorf("stale rendered lock: source inputs differ: %v", err)
	}
	raw, err := source.RawManifests(ctx, revision, l.Namespace)
	if err != nil {
		return err
	}
	entries := map[sdk.ObjectRef]Object{}
	for _, entry := range l.Objects {
		if _, ok := entries[entry.Ref]; ok {
			return errors.New("duplicate rendered lock object")
		}
		entries[entry.Ref] = entry
	}
	if len(entries) != len(raw) {
		return errors.New("rendered lock inventory differs from fresh render")
	}
	used := map[string]bool{}
	for _, expected := range raw {
		entry, ok := entries[objectRef(expected)]
		if !ok {
			return errors.New("rendered lock omits source object")
		}
		if entry.Error != "" {
			if entry.File != "" || entry.SHA256 != "" || len(entry.Images) > 0 {
				return errors.New("gap entry contains rendered evidence")
			}
			continue
		}
		actual, err := s.object(entry)
		if err != nil {
			return err
		}
		used[entry.File] = true
		fresh, err := source.render(expected)
		if err != nil {
			return err
		}
		a, _ := json.Marshal(actual.Object)
		b, _ := json.Marshal(fresh.Object)
		if string(a) != string(b) {
			return fmt.Errorf("rendered lock differs from fresh render for %s/%s", entry.Ref.Kind, entry.Ref.Name)
		}
		recorded, current := entry.Images, containerImages(fresh)
		if len(recorded) != len(current) {
			return errors.New("lock container inventory differs from object")
		}
		for i := range current {
			if recorded[i] != current[i] {
				return errors.New("lock container image differs from object")
			}
		}
	}
	if len(used) != len(s.Files) {
		return errors.New("rendered lock contains unindexed files")
	}
	return nil
}

func (s Snapshot) object(entry Object) (*unstructured.Unstructured, error) {
	if entry.Error != "" {
		return nil, fmt.Errorf("%w: %s/%s: %s", errLockObject, entry.Ref.Kind, entry.Ref.Name, entry.Error)
	}
	if entry.File != objectFile(entry.Ref) {
		return nil, fmt.Errorf("%w: invalid object filename", errLockObject)
	}
	data, ok := s.Files[entry.File]
	if !ok {
		return nil, fmt.Errorf("%w: missing object %s/%s", errLockObject, entry.Ref.Kind, entry.Ref.Name)
	}
	sum := sha256.Sum256(data)
	if fmt.Sprintf("%x", sum) != entry.SHA256 {
		return nil, fmt.Errorf("%w: object hash mismatch", errLockObject)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	// Convert YAML integer shapes to the JSON-compatible unstructured contract.
	encoded, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}
	o := &unstructured.Unstructured{}
	if err := json.Unmarshal(encoded, &o.Object); err != nil {
		return nil, err
	}
	if objectRef(o) != entry.Ref {
		return nil, fmt.Errorf("%w: object identity mismatch", errLockObject)
	}
	return o, nil
}

// Write writes only inside a broker-private checkout; publication is atomic Git.
func (s Snapshot) Write(root string) error {
	dir := filepath.Join(root, Directory)
	// Reject symlink parents before removing or creating any files.
	for _, p := range []string{filepath.Join(root, ".sdo"), dir} {
		if info, err := os.Lstat(p); err == nil && !info.IsDir() {
			return fmt.Errorf("rendered path must be a directory: %s", p)
		} else if err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	for name, data := range s.Files {
		if filepath.Base(name) != name || !strings.HasSuffix(name, ".yaml") {
			return errors.New("unsafe rendered object filename")
		}
		if err := os.WriteFile(filepath.Join(dir, name), data, 0644); err != nil {
			return err
		}
	}
	data, err := json.MarshalIndent(s.Lock, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(root, LockPath), append(data, '\n'), 0644)
}

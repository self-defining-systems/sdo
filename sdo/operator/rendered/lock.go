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
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk"
)

const Directory = layout.RenderedDir
const LockPath = layout.RenderedLockFile

var ErrEvidenceGap = errors.New("rendered deployment evidence unavailable")
var immutableImage = regexp.MustCompile(`^.+@sha256:[a-f0-9]{64}$`)

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

// BuildLock reuses FX1's per-object renderer and named regular/init image evidence.
// Failed objects have explicit gap entries and cannot erase the successful ones.
func (s Source) BuildLock(ctx context.Context, revision, namespace string) (Snapshot, error) {
	result := Snapshot{Lock: Lock{Version: 1, SourceCommit: revision, Namespace: namespace, ManifestPaths: append([]string{}, s.ManifestPaths...), Objects: []Object{}}, Files: map[string][]byte{}}
	raw, err := s.RawManifests(ctx, revision, namespace)
	if err != nil {
		return result, err
	}
	tree, err := s.sourceTree(ctx, revision)
	if err != nil {
		return result, err
	}
	for _, o := range raw {
		ref := objectRef(o)
		entry := Object{Ref: ref}
		if gap := s.Gaps[ref]; gap != "" {
			entry.Error = gap
			result.Lock.Objects = append(result.Lock.Objects, entry)
			continue
		}
		object, err := s.renderDocument(ctx, o.Object, tree, ref, ref.Kind+"/"+ref.Name)
		if err == nil {
			entry.Images, err = containerImages(object)
			for i := range entry.Images {
				image := &entry.Images[i]
				if s.Digests != nil {
					if digest, ok := s.Digests.BaselineImage(ref, image.Name, image.Init); ok {
						image.Digest = digest
					}
				}
				if !immutableImage.MatchString(image.Digest) {
					err = fmt.Errorf("%w: %s/%s container %s needs a deployed digest", ErrEvidenceGap, ref.Kind, ref.Name, image.Name)
				}
			}
		}
		if err != nil {
			entry.Error = err.Error()
			entry.Images = nil
		} else {
			data, err := yaml.Marshal(object.Object)
			if err != nil {
				return result, err
			}
			entry.File = objectFile(ref)
			sum := sha256.Sum256(data)
			entry.SHA256 = fmt.Sprintf("%x", sum)
			result.Files[entry.File] = data
		}
		result.Lock.Objects = append(result.Lock.Objects, entry)
	}
	sort.Slice(result.Lock.Objects, func(i, j int) bool {
		return objectFile(result.Lock.Objects[i].Ref) < objectFile(result.Lock.Objects[j].Ref)
	})
	return result, nil
}

func containerImages(o *unstructured.Unstructured) ([]ContainerImage, error) {
	spec, _, _ := unstructured.NestedMap(o.Object, "spec", "template", "spec")
	images := []ContainerImage{}
	for _, field := range []string{"initContainers", "containers"} {
		containers, _ := spec[field].([]any)
		for _, raw := range containers {
			container, _ := raw.(map[string]any)
			image, _ := container["image"].(string)
			name, _ := container["name"].(string)
			if name == "" || image == "" {
				return nil, fmt.Errorf("%w: %s/%s container %s needs a deployed digest", ErrEvidenceGap, o.GetKind(), o.GetName(), name)
			}
			images = append(images, ContainerImage{Name: name, Init: field == "initContainers", Digest: image})
		}
	}
	return images, nil
}

// Validate accepts source-pinned identities without deployment substitutions.
func (s Snapshot) Validate(ctx context.Context, repository, revision string) error {
	return s.ValidateWithEvidence(ctx, repository, revision, nil)
}

// ValidateWithEvidence independently renders source and authorizes substitutions
// only with separately supplied, broker-owned deployment evidence.
func (s Snapshot) ValidateWithEvidence(ctx context.Context, repository, revision string, evidence []DeploymentEvidence) error {
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
		images, err := containerImages(actual)
		if err != nil {
			return err
		}
		if len(images) != len(entry.Images) {
			return errors.New("lock container inventory differs from object")
		}
		for i, image := range images {
			recorded := entry.Images[i]
			if image.Name != recorded.Name || image.Init != recorded.Init || !immutableImage.MatchString(recorded.Digest) {
				return errors.New("invalid named container digest in lock")
			}
			if immutableImage.MatchString(image.Digest) && image.Digest != recorded.Digest {
				return errors.New("immutable object image differs from lock digest")
			}
		}
		tree := actual.GetAnnotations()["sdo.dev/source-tree"]
		if tree != "" {
			same, err := source.SameWorkloadInputs(ctx, tree, revision, expected, evidence)
			if err != nil || !same {
				return errors.New("lock object has stale deployed source tree")
			}
		}
		authorized := map[ContainerImage]DeployedImage{}
		for _, proof := range evidence {
			for _, image := range proof.Images {
				same, err := source.SameImageInputs(ctx, proof.SourceTree, revision, image.Inputs)
				if err != nil || !same {
					continue
				}
				if image.Ref != entry.Ref {
					continue
				}
				key := ContainerImage{Name: image.Name, Init: image.Init}
				if prior, ok := authorized[key]; ok && prior != image {
					return errors.New("conflicting deployment image evidence")
				}
				authorized[key] = image
			}
		}
		sourceImages, err := containerImages(expected)
		if err != nil {
			return err
		}
		for i, image := range sourceImages {
			if i >= len(images) || image.Name != images[i].Name || image.Init != images[i].Init {
				return errors.New("source container inventory differs from lock")
			}
			if immutableImage.MatchString(image.Digest) {
				if images[i].Digest != image.Digest || entry.Images[i].Digest != image.Digest {
					return errors.New("lock replaces source-pinned image")
				}
			} else {
				proof, ok := authorized[ContainerImage{Name: image.Name, Init: image.Init}]
				if !ok || (proof.Inputs.SourceImage != "" && proof.Inputs.SourceImage != image.Digest) || proof.Applied != images[i].Digest || proof.Digest != entry.Images[i].Digest {
					return errors.New("lock image lacks trusted named deployment evidence")
				}
			}
		}
		if len(sourceImages) != len(images) {
			return errors.New("source container inventory differs from lock")
		}
		if _, _, err := RenderManifest(expected.Object, l.Namespace, tree, func(_, _, name string, init bool, image string) string {
			if immutableImage.MatchString(image) {
				return image
			}
			return authorized[ContainerImage{Name: name, Init: init}].Applied
		}); err != nil {
			return err
		}
		a, _ := json.Marshal(actual.Object)
		b, _ := json.Marshal(expected.Object)
		if string(a) != string(b) {
			return fmt.Errorf("rendered lock differs from fresh render for %s/%s", entry.Ref.Kind, entry.Ref.Name)
		}
	}
	if len(used) != len(s.Files) {
		return errors.New("rendered lock contains unindexed files")
	}
	return nil
}

func (s Snapshot) object(entry Object) (*unstructured.Unstructured, error) {
	if entry.Error != "" {
		return nil, fmt.Errorf("%w: %s/%s: %s", ErrEvidenceGap, entry.Ref.Kind, entry.Ref.Name, entry.Error)
	}
	if entry.File != objectFile(entry.Ref) {
		return nil, fmt.Errorf("%w: invalid object filename", ErrEvidenceGap)
	}
	data, ok := s.Files[entry.File]
	if !ok {
		return nil, fmt.Errorf("%w: missing object %s/%s", ErrEvidenceGap, entry.Ref.Kind, entry.Ref.Name)
	}
	sum := sha256.Sum256(data)
	if fmt.Sprintf("%x", sum) != entry.SHA256 {
		return nil, fmt.Errorf("%w: object hash mismatch", ErrEvidenceGap)
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
		return nil, fmt.Errorf("%w: object identity mismatch", ErrEvidenceGap)
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

func (s Source) readLock(ctx context.Context, revision, namespace string) (Snapshot, error) {
	result := Snapshot{Files: map[string][]byte{}}
	if !gitRevision.MatchString(revision) || namespace == "" || s.Repository == "" {
		return result, fmt.Errorf("%w: repository, revision, namespace required", ErrEvidenceGap)
	}
	read := func(path string) ([]byte, error) {
		return exec.CommandContext(ctx, "git", "-C", s.Repository, "show", revision+":"+path).Output()
	}
	data, err := read(LockPath)
	if err != nil {
		return result, fmt.Errorf("%w: missing lock at %s", ErrEvidenceGap, revision)
	}
	if err := json.Unmarshal(data, &result.Lock); err != nil {
		return result, fmt.Errorf("%w: invalid lock: %v", ErrEvidenceGap, err)
	}
	l := result.Lock
	if l.Version != 1 || l.Namespace != namespace || !reflect.DeepEqual(l.ManifestPaths, s.ManifestPaths) {
		return result, fmt.Errorf("%w: lock deployment selection mismatch", ErrEvidenceGap)
	}
	same, err := s.SameInputs(ctx, l.SourceCommit, revision)
	if err != nil || !same {
		return result, fmt.Errorf("%w: stale lock", ErrEvidenceGap)
	}
	return result, nil
}

// Load only the requested object's bytes. Unrelated missing YAML or read
// failures cannot consume or suppress a single-object source comparison.
func (s Source) lockedObject(ctx context.Context, revision string, snapshot Snapshot, entry Object) (*unstructured.Unstructured, error) {
	if entry.Error == "" && entry.File == objectFile(entry.Ref) {
		data, err := exec.CommandContext(ctx, "git", "-C", s.Repository, "show", revision+":"+Directory+"/"+entry.File).Output()
		if err != nil {
			return nil, fmt.Errorf("%w: read lock object %s/%s: %v", ErrEvidenceGap, entry.Ref.Kind, entry.Ref.Name, err)
		}
		snapshot.Files[entry.File] = data
	}
	return snapshot.object(entry)
}

func (s Source) lockManifests(ctx context.Context, revision, namespace string) ([]*unstructured.Unstructured, error) {
	snapshot, err := s.readLock(ctx, revision, namespace)
	if err != nil {
		return nil, err
	}
	raw, err := s.RawManifests(ctx, revision, namespace)
	if err != nil {
		return nil, err
	}
	entries := map[sdk.ObjectRef]Object{}
	for _, entry := range snapshot.Lock.Objects {
		entries[entry.Ref] = entry
	}
	objects := []*unstructured.Unstructured{}
	var gaps []error
	for _, expected := range raw {
		entry, ok := entries[objectRef(expected)]
		if !ok {
			gaps = append(gaps, fmt.Errorf("%w: missing lock object %s/%s", ErrEvidenceGap, expected.GetKind(), expected.GetName()))
			continue
		}
		o, err := s.lockedObject(ctx, revision, snapshot, entry)
		if err != nil {
			gaps = append(gaps, err)
			continue
		}
		objects = append(objects, o)
	}
	return objects, errors.Join(gaps...)
}

func (s Source) lockManifest(ctx context.Context, revision string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	snapshot, err := s.readLock(ctx, revision, ref.Namespace)
	if err != nil {
		return nil, err
	}
	for _, entry := range snapshot.Lock.Objects {
		if entry.Ref == ref {
			return s.lockedObject(ctx, revision, snapshot, entry)
		}
	}
	raw, err := s.RawManifests(ctx, revision, ref.Namespace)
	if err != nil {
		return nil, err
	}
	for _, o := range raw {
		if objectRef(o) == ref {
			return nil, fmt.Errorf("%w: lock missing %s/%s", ErrEvidenceGap, ref.Kind, ref.Name)
		}
	}
	return nil, fmt.Errorf("%w: %s/%s", ErrAcceptedManifestAbsent, ref.Kind, ref.Name)
}

// LockCurrent checks only identity, so a memory-only commit never rewrites a
// lock or attempts to bless the current live state after an incident.
func (s Source) LockCurrent(ctx context.Context, revision, namespace string) bool {
	_, err := s.readLock(ctx, revision, namespace)
	return err == nil
}

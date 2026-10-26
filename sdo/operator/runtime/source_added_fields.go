package runtime

import (
	"encoding/json"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// acceptedPathsAnnotation records, without values, the configuration paths an
// object had beyond accepted source when SDO last applied or adopted it. It
// travels with the object into informer snapshots and corpus captures, so live
// and replayed DiffAgainstSource report the same additions.
const acceptedPathsAnnotation = "sdo.dev/accepted-paths"

// stampAcceptedPaths records on apply, the object SDO is about to write, the
// undeclared paths of applied, the server's dry-run result of that write.
// Server defaults and fields present at first acceptance are thereby part of
// the accepted object. Paths that were already additions under an earlier
// acceptance stay additions: re-acceptance must not absorb drift that accepted
// source never declared.
func stampAcceptedPaths(apply, applied, live *unstructured.Unstructured) error {
	declared := flattenDriftObject(apply)
	added := map[string]bool{}
	for _, path := range addedSourceFields(live, comparableFields(apply.Object)) {
		added[path] = true
	}
	paths := []string{}
	for path, value := range flattenDriftObject(applied) {
		if value != nil && !added[path] && !reservedMetadataPath(path) && !declaredPath(declared, path) {
			paths = append(paths, path)
		}
	}
	sort.Strings(paths)
	encoded, err := json.Marshal(paths)
	if err != nil {
		return err
	}
	annotations := apply.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[acceptedPathsAnnotation] = string(encoded)
	apply.SetAnnotations(annotations)
	return nil
}

func acceptedPaths(object *unstructured.Unstructured) (map[string]bool, bool) {
	if object == nil {
		return nil, false
	}
	encoded, ok := object.GetAnnotations()[acceptedPathsAnnotation]
	if !ok {
		return nil, false
	}
	var paths []string
	if json.Unmarshal([]byte(encoded), &paths) != nil {
		return nil, false
	}
	result := make(map[string]bool, len(paths))
	for _, path := range paths {
		result[path] = true
	}
	return result, true
}

// addedSourceFields returns observed configuration paths that were absent when
// SDO accepted the object and that accepted source does not declare. Apply
// normalization keeps such fields, so they need their own check. Without an
// acceptance record there is no acceptance point and nothing is added.
func addedSourceFields(observed *unstructured.Unstructured, accepted map[string]any) []string {
	present, ok := acceptedPaths(observed)
	if !ok {
		return nil
	}
	declared := flattenDriftObject(&unstructured.Unstructured{Object: accepted})
	paths := []string{}
	for path, value := range flattenDriftObject(observed) {
		// Typed reads serialize unset fields as null; null is absence.
		if value != nil && !present[path] && !reservedMetadataPath(path) && !declaredPath(declared, path) {
			paths = append(paths, path)
		}
	}
	sort.Strings(paths)
	return paths
}

func observedLeafUnder(leaves map[string]any, path string) bool {
	for leaf := range leaves {
		if leaf == path || strings.HasPrefix(leaf, path+".") || strings.HasPrefix(leaf, path+"[") {
			return true
		}
	}
	return false
}

func declaredPath(leaves map[string]any, path string) bool {
	if observedLeafUnder(leaves, path) {
		return true
	}
	// A declared scalar or empty value covers anything written beneath it.
	for leaf := range leaves {
		if strings.HasPrefix(path, leaf+".") || strings.HasPrefix(path, leaf+"[") {
			return true
		}
	}
	return false
}

// Kubernetes reserves kubernetes.io and k8s.io label and annotation keys for its
// own components (revision stamps, rollout restarts); they are bookkeeping,
// not configuration that accepted source could declare.
func reservedMetadataPath(path string) bool {
	for _, prefix := range []string{"metadata.labels.", "metadata.annotations.", "spec.template.metadata.labels.", "spec.template.metadata.annotations."} {
		if !strings.HasPrefix(path, prefix) {
			continue
		}
		key := strings.TrimPrefix(path, prefix)
		domain, _, qualified := strings.Cut(key, "/")
		if !qualified {
			return false
		}
		for _, reserved := range []string{"kubernetes.io", "k8s.io"} {
			if domain == reserved || strings.HasSuffix(domain, "."+reserved) {
				return true
			}
		}
	}
	return false
}

package runtime

import (
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// addedSourceFields returns observed configuration paths that accepted source
// does not declare and that another field manager wrote after the deployer's
// latest accepted Apply. Apply normalization keeps such fields, so they need
// their own check. Server defaults are either unowned (objects SDO applied) or
// owned by a writer that predates acceptance (adopted objects), so write order
// separates defaulting from added drift without a second server request.
// Without a deployer Apply there is no acceptance point and nothing is added.
func addedSourceFields(observed *unstructured.Unstructured, accepted map[string]any) []string {
	if observed == nil {
		return nil
	}
	var acceptedAt *metav1.Time
	for _, entry := range observed.GetManagedFields() {
		if entry.Manager == "sdo-deployer" && entry.Operation == metav1.ManagedFieldsOperationApply && entry.Subresource == "" && entry.Time != nil {
			if acceptedAt == nil || entry.Time.After(acceptedAt.Time) {
				acceptedAt = entry.Time
			}
		}
	}
	if acceptedAt == nil {
		return nil
	}
	declared := flattenDriftObject(&unstructured.Unstructured{Object: accepted})
	present := flattenDriftObject(observed)
	added := map[string]bool{}
	for _, entry := range observed.GetManagedFields() {
		if entry.Manager == "sdo-deployer" || entry.Subresource != "" || entry.Time == nil || !entry.Time.After(acceptedAt.Time) {
			continue
		}
		for path := range ownedDriftPathsForEntry(observed, entry) {
			if !reservedMetadataPath(path) && observedLeafUnder(present, path) && !declaredPath(declared, path) {
				added[path] = true
			}
		}
	}
	paths := make([]string, 0, len(added))
	for path := range added {
		paths = append(paths, path)
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

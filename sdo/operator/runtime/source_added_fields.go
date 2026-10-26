package runtime

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// acceptedPathsAnnotation records, without values, every configuration path an
// object carried when SDO last accepted it, bound to the accepted source it was
// taken against. It travels with the object into informer snapshots and corpus
// captures, so live and replayed DiffAgainstSource report the same additions.
// SDO writes it under its own field manager, never the deployer's: a deployer
// apply that does not carry the key must not delete it.
const acceptedPathsAnnotation = "sdo.dev/accepted-paths"

const acceptanceFieldManager = "sdo-acceptance"

// A record larger than this is not written; the object then has no
// acceptance point and its source comparison fails closed.
const maxAcceptanceRecordBytes = 32 << 10

// ErrAcceptanceRecordUnavailable means an object has no acceptance record for
// its current accepted source, so fields added since acceptance are unknown.
var ErrAcceptanceRecordUnavailable = errors.New("acceptance record is missing or was taken against other accepted source")

type acceptanceRecord struct {
	Source string   `json:"source"`
	Paths  []string `json:"paths"`
}

// acceptedSourceShape identifies which paths accepted source declares. Values
// stay out so that redacted captures of the same source bind to the record.
func acceptedSourceShape(source *unstructured.Unstructured) string {
	paths := make([]string, 0)
	for path := range flattenDriftObject(source) {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	encoded, _ := json.Marshal(paths)
	return fmt.Sprintf("%x", sha256.Sum256(encoded))
}

func readAcceptanceRecord(object *unstructured.Unstructured) (acceptanceRecord, bool) {
	if object == nil {
		return acceptanceRecord{}, false
	}
	encoded, ok := object.GetAnnotations()[acceptedPathsAnnotation]
	if !ok {
		return acceptanceRecord{}, false
	}
	var record acceptanceRecord
	if json.Unmarshal([]byte(encoded), &record) != nil || record.Source == "" {
		return acceptanceRecord{}, false
	}
	return record, true
}

// acceptanceRecordCurrent reports whether observed carries a record taken
// against source.
func acceptanceRecordCurrent(observed, source *unstructured.Unstructured) bool {
	record, ok := readAcceptanceRecord(observed)
	return ok && record.Source == acceptedSourceShape(source)
}

// buildAcceptanceRecord records every path of applied, the server's dry-run
// result of accepting source over live. Server defaults and fields present at
// first acceptance thereby become part of the accepted object. Paths that were
// additions under live's earlier record stay additions: re-acceptance must not
// absorb drift that accepted source never declared. ok is false when the record
// would exceed the size limit.
func buildAcceptanceRecord(source, applied, live *unstructured.Unstructured) (string, bool, error) {
	carried := map[string]bool{}
	if previous, ok := readAcceptanceRecord(live); ok {
		for _, path := range additionsAgainst(live, previous, source) {
			carried[path] = true
		}
	}
	paths := []string{}
	for path, value := range flattenDriftObject(applied) {
		if value != nil && !carried[path] && !bookkeepingPath(path, value) {
			paths = append(paths, path)
		}
	}
	sort.Strings(paths)
	encoded, err := json.Marshal(acceptanceRecord{Source: acceptedSourceShape(source), Paths: paths})
	if err != nil {
		return "", false, err
	}
	return string(encoded), len(encoded) <= maxAcceptanceRecordBytes, nil
}

// addedSourceFields returns observed configuration paths that were absent when
// SDO accepted the object and that accepted source does not declare. Apply
// normalization keeps such fields, so they need their own check. Without a
// record taken against this source the additions are unknown, and the
// comparison fails closed. Identity-only declarations are never stamped and
// track no additions.
func addedSourceFields(observed, source *unstructured.Unstructured) ([]string, error) {
	if len(flattenDriftObject(source)) == 0 {
		return nil, nil
	}
	record, ok := readAcceptanceRecord(observed)
	if !ok || record.Source != acceptedSourceShape(source) {
		return nil, ErrAcceptanceRecordUnavailable
	}
	return additionsAgainst(observed, record, source), nil
}

func additionsAgainst(observed *unstructured.Unstructured, record acceptanceRecord, source *unstructured.Unstructured) []string {
	present := make(map[string]bool, len(record.Paths))
	for _, path := range record.Paths {
		present[path] = true
	}
	declared := flattenDriftObject(source)
	paths := []string{}
	for path, value := range flattenDriftObject(observed) {
		// Typed reads serialize unset fields as null; null is absence.
		if value != nil && !present[path] && !bookkeepingPath(path, value) && !declaredPath(declared, path) {
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

// Kubernetes reserves kubernetes.io and k8s.io label, annotation and finalizer
// names for its own components (revision stamps, rollout restarts, load
// balancer cleanup); they are bookkeeping, not configuration that accepted
// source could declare. Controllers write them after acceptance.
func bookkeepingPath(path string, value any) bool {
	if strings.HasPrefix(path, "metadata.finalizers[") {
		name, _ := value.(string)
		return reservedDomainName(name)
	}
	for _, prefix := range []string{"metadata.labels.", "metadata.annotations.", "spec.template.metadata.labels.", "spec.template.metadata.annotations."} {
		if strings.HasPrefix(path, prefix) {
			return reservedDomainName(strings.TrimPrefix(path, prefix))
		}
	}
	return false
}

func reservedDomainName(name string) bool {
	domain, _, qualified := strings.Cut(name, "/")
	if !qualified {
		return false
	}
	for _, reserved := range []string{"kubernetes.io", "k8s.io"} {
		if domain == reserved || strings.HasSuffix(domain, "."+reserved) {
			return true
		}
	}
	return false
}

// acceptanceSettled reports whether controllers have caught up with the
// object's current spec, so that a record taken now does not miss fields they
// are about to write (a scheduled Pod's node, a rollout's observed spec).
func acceptanceSettled(live *unstructured.Unstructured) bool {
	if observed, found, _ := unstructured.NestedInt64(live.Object, "status", "observedGeneration"); found && observed != live.GetGeneration() {
		return false
	}
	if live.GetAPIVersion() == "v1" && live.GetKind() == "Pod" {
		node, _, _ := unstructured.NestedString(live.Object, "spec", "nodeName")
		return node != ""
	}
	return true
}

// removeAddedFields deletes from next every map entry whose observed fields
// were all added after acceptance. Reconciliation restores declared keys and
// whole declared lists, so without this an added map key (a nodeSelector, a
// label) would survive the repair that DiffAgainstSource asks for. Without a
// current record nothing is removed.
func removeAddedFields(next, observed, source *unstructured.Unstructured) {
	added, err := addedSourceFields(observed, source)
	if err != nil || len(added) == 0 {
		return
	}
	addedSet := make(map[string]bool, len(added))
	for _, path := range added {
		addedSet[path] = true
	}
	leaves := flattenDriftObject(observed)
	onlyAdded := func(path string) bool {
		found := false
		for leaf, value := range leaves {
			if value == nil || (leaf != path && !strings.HasPrefix(leaf, path+".") && !strings.HasPrefix(leaf, path+"[")) {
				continue
			}
			if !addedSet[leaf] {
				return false
			}
			found = true
		}
		return found
	}
	var prune func(string, map[string]any)
	prune = func(prefix string, node map[string]any) {
		for key, child := range node {
			path := prefix + "." + key
			if onlyAdded(path) {
				delete(node, key)
			} else if nested, ok := child.(map[string]any); ok {
				prune(path, nested)
			}
		}
	}
	for key, child := range next.Object {
		nested, ok := child.(map[string]any)
		switch {
		case !ok || key == "apiVersion" || key == "kind" || key == "status":
		case key == "metadata":
			for _, field := range []string{"labels", "annotations"} {
				if values, ok := nested[field].(map[string]any); ok {
					prune("metadata."+field, values)
				}
			}
		default:
			if onlyAdded(key) {
				delete(next.Object, key)
			} else {
				prune(key, nested)
			}
		}
	}
}

// RecordAcceptance sets on object the acceptance record that adoption would
// take against source over the object as it is now.
func RecordAcceptance(object, source *unstructured.Unstructured) error {
	record, fits, err := buildAcceptanceRecord(source, object, object)
	if err != nil {
		return err
	}
	if !fits {
		return fmt.Errorf("acceptance record of %s/%s exceeds %d bytes", object.GetKind(), object.GetName(), maxAcceptanceRecordBytes)
	}
	annotations := object.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[acceptedPathsAnnotation] = record
	object.SetAnnotations(annotations)
	return nil
}

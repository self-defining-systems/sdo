package runtime

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
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

// AcceptanceUnknownError reports a source comparison whose declared fields
// agree but whose additions are unknown. Detectors and programs see an error,
// so nothing is learned from it; the controller's own drift handling uses
// Declared, which compares declared fields only.
type AcceptanceUnknownError struct {
	Ref      sdk.ObjectRef
	Declared program.Diff
}

func (e *AcceptanceUnknownError) Error() string {
	return fmt.Sprintf("compare %s/%s with accepted source: %v", e.Ref.Kind, e.Ref.Name, ErrAcceptanceRecordUnavailable)
}

func (e *AcceptanceUnknownError) Unwrap() error { return ErrAcceptanceRecordUnavailable }

// declaredSourceDiff is the comparison the controller's drift admission,
// verification and closure need: whether declared fields match accepted
// source. An unknown acceptance record must not block them (an object faulted
// before it was ever recorded still closes once restored).
func declaredSourceDiff(view program.ReadView, ref sdk.ObjectRef) (program.Diff, error) {
	diff, err := view.DiffAgainstSource(ref)
	var unknown *AcceptanceUnknownError
	if errors.As(err, &unknown) {
		return unknown.Declared, nil
	}
	return diff, err
}

// An acceptance record belongs to one object instance (its UID): copied onto
// a recreated object it is not that object's acceptance point. A record
// without a UID predates the binding; it stays readable until adoption
// re-stamps it (with its additions carried forward).
type acceptanceRecord struct {
	Source string   `json:"source"`
	UID    string   `json:"uid"`
	Paths  []string `json:"paths"`
	// Values holds the accepted value of every undeclared configuration
	// scalar. A server-defaulted field is present at acceptance, so its path
	// alone cannot tell a later change from the default.
	Values map[string]any `json:"values,omitempty"`
}

// payloadFields hold content, not configuration: Secret and ConfigMap data can
// be large and secret, and other writers fill them in. Their keys are tracked
// as paths only.
var payloadFields = map[string]bool{"data": true, "binaryData": true, "stringData": true}

// valueTracked reports whether the accepted value of an undeclared scalar at
// path is recorded: configuration only, never metadata (labels, annotations
// and finalizers have their own rules), status or payloads.
func valueTracked(path string) bool {
	if strings.HasPrefix(path, "metadata.") || strings.Contains(path, ".metadata.") {
		return false
	}
	top, _, _ := strings.Cut(path, ".")
	return !payloadFields[top]
}

// acceptedSourceShape identifies which paths accepted source declares. Values
// stay out so that redacted captures of the same source bind to the record.
func acceptedSourceShape(source *unstructured.Unstructured) string {
	paths := make([]string, 0)
	for path := range flattenDriftObject(source) {
		paths = append(paths, path)
	}
	return sourceShapeDigest(paths)
}

func sourceShapeDigest(paths []string) string {
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

// currentAcceptanceRecord returns observed's record when it was taken on this
// object instance against source.
func currentAcceptanceRecord(observed, source *unstructured.Unstructured) (acceptanceRecord, bool) {
	record, ok := readAcceptanceRecord(observed)
	if !ok || (record.UID != "" && record.UID != string(observed.GetUID())) || (record.Source != acceptedSourceShape(source) && record.Source != legacyAcceptedSourceShape(source)) {
		return acceptanceRecord{}, false
	}
	return record, true
}

// acceptanceRecordCurrent reports whether observed carries a record taken on
// it against source.
func acceptanceRecordCurrent(observed, source *unstructured.Unstructured) bool {
	_, ok := currentAcceptanceRecord(observed, source)
	return ok
}

// acceptanceRecordBound reports whether observed's current record is also
// bound to its UID, so adoption need not re-stamp it.
func acceptanceRecordBound(observed, source *unstructured.Unstructured) bool {
	record, ok := currentAcceptanceRecord(observed, source)
	return ok && record.UID == string(observed.GetUID())
}

// buildAcceptanceRecord records every path of applied, the server's dry-run
// result of accepting source over live. Server defaults and fields present at
// first acceptance thereby become part of the accepted object. Paths that were
// additions under live's earlier record stay additions: re-acceptance must not
// absorb drift that accepted source never declared.
//
// reaccepted marks an object SDO accepted before whose record was lost (the
// object was recreated, or the record stripped): what changed meanwhile is
// unknown. Its undeclared fields that another writer owns then stay additions;
// only declared fields, the deployer's fields and unowned server defaults are
// accepted. ok is false when the record would exceed the size limit.
func buildAcceptanceRecord(source, applied, live *unstructured.Unstructured, reaccepted bool) (string, bool, error) {
	carried := map[string]bool{}
	if previous, ok := readAcceptanceRecord(live); ok {
		for _, path := range additionsAgainst(live, previous, source) {
			carried[path] = true
		}
	}
	if reaccepted {
		declared := flattenDriftObject(source)
		owned := ownedByOtherWriters(live)
		for path := range flattenDriftObject(applied) {
			if ownsDriftPath(owned, path) && !declaredPath(declared, path) {
				carried[path] = true
			}
		}
	}
	paths := []string{}
	appliedLeaves, declared := flattenDriftObject(applied), flattenDriftObject(source)
	for path, value := range appliedLeaves {
		if value != nil && !carried[path] && !bookkeepingPath(path, value) {
			paths = append(paths, path)
		}
	}
	sort.Strings(paths)
	values := map[string]any{}
	for path, value := range appliedLeaves {
		if value != nil && !carried[path] && !bookkeepingPath(path, value) && valueTracked(path) && !declaredPath(declared, path) {
			values[path] = value
		}
	}
	record := acceptanceRecord{Source: acceptedSourceShape(source), UID: string(live.GetUID()), Paths: paths}
	// Values double the record. An object too large to carry them still keeps
	// its paths rather than losing the acceptance point altogether.
	if len(values) > 0 {
		record.Values = values
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return "", false, err
	}
	if len(encoded) > maxAcceptanceRecordBytes && record.Values != nil {
		record.Values = nil
		if encoded, err = json.Marshal(record); err != nil {
			return "", false, err
		}
	}
	return string(encoded), len(encoded) <= maxAcceptanceRecordBytes, nil
}

// addedSourceFields returns observed configuration paths that were absent when
// SDO accepted the object and that accepted source does not declare, plus the
// recorded scalars whose value has since changed. Apply
// normalization keeps such fields, so they need their own check. Without a
// record taken against this source the additions are unknown, and the
// comparison fails closed. Identity-only declarations are never stamped and
// track no additions.
func addedSourceFields(observed, source *unstructured.Unstructured) ([]string, error) {
	if len(flattenDriftObject(source)) == 0 {
		return nil, nil
	}
	record, ok := currentAcceptanceRecord(observed, source)
	if !ok {
		return nil, ErrAcceptanceRecordUnavailable
	}
	paths := append(additionsAgainst(observed, record, source), changedAcceptedValues(observed, record)...)
	sort.Strings(paths)
	return paths, nil
}

// changedAcceptedValues lists the recorded scalars whose observed value differs
// from the one accepted. A field another controller owns through a subresource
// (an autoscaler's replicas) is that controller's to change; one written
// through the object itself, by kubectl or anything else, is drift.
func changedAcceptedValues(observed *unstructured.Unstructured, record acceptanceRecord) []string {
	if len(record.Values) == 0 {
		return nil
	}
	leaves := flattenDriftObject(observed)
	var controllers map[string]bool
	changed := []string{}
	for path, accepted := range normalizedAcceptanceValues(observed, record.Values) {
		value, ok := leaves[path]
		if !ok || value == nil || sameScalar(value, accepted) {
			continue
		}
		if controllers == nil {
			controllers = ownedBySubresourceWriters(observed)
		}
		if !ownsDriftPath(controllers, path) {
			changed = append(changed, path)
		}
	}
	sort.Strings(changed)
	return changed
}

// sameScalar compares an observed scalar with its decoded record (JSON turns
// integers into floats).
func sameScalar(observed, recorded any) bool {
	left, _ := json.Marshal(observed)
	right, _ := json.Marshal(recorded)
	return string(left) == string(right)
}

// ownedByOtherWriters returns the paths that field managers other than SDO's
// deployer and acceptance record own.
func ownedByOtherWriters(object *unstructured.Unstructured) map[string]bool {
	return ownedByWriters(object, func(metav1.ManagedFieldsEntry) bool { return true })
}

// ownedBySubresourceWriters returns the paths other field managers own through
// a subresource, such as the scale subresource an autoscaler writes.
func ownedBySubresourceWriters(object *unstructured.Unstructured) map[string]bool {
	return ownedByWriters(object, func(entry metav1.ManagedFieldsEntry) bool { return entry.Subresource != "" })
}

func ownedByWriters(object *unstructured.Unstructured, include func(metav1.ManagedFieldsEntry) bool) map[string]bool {
	owned := map[string]bool{}
	for _, entry := range object.GetManagedFields() {
		if entry.Manager == "sdo-deployer" || entry.Manager == acceptanceFieldManager || entry.FieldsV1 == nil || !include(entry) {
			continue
		}
		var fields map[string]any
		if json.Unmarshal(entry.FieldsV1.Raw, &fields) == nil {
			walkOwnedFields(fields, object.Object, "", owned)
		}
	}
	return owned
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

// Annotation exclusions share the comparison policy; Kubernetes-reserved
// labels and finalizers retain their existing controller bookkeeping policy.
func bookkeepingPath(path string, value any) bool {
	if strings.HasPrefix(path, "metadata.finalizers[") {
		name, _ := value.(string)
		return reservedDomainName(name)
	}
	for _, prefix := range []string{"metadata.annotations.", "spec.template.metadata.annotations."} {
		if strings.HasPrefix(path, prefix) {
			return sourceBookkeepingAnnotation(strings.TrimPrefix(path, prefix))
		}
	}
	for _, prefix := range []string{"metadata.labels.", "spec.template.metadata.labels."} {
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
	record, _ := currentAcceptanceRecord(observed, source)
	restored := changedAcceptedValues(observed, record)
	for _, path := range restored {
		// A changed scalar goes back to its accepted value, not to absence.
		setLeaf(next.Object, path, record.Values[path])
	}
	addedSet := make(map[string]bool, len(added))
	for _, path := range added {
		addedSet[path] = true
	}
	for _, path := range restored {
		delete(addedSet, path)
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
	record, fits, err := buildAcceptanceRecord(source, object, object, false)
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

// setLeaf writes value at a flattened path ("a.b[0].c") when every parent
// exists. Reconciliation has already restored the declared lists, so an index
// the source does not have is simply skipped.
func setLeaf(object map[string]any, path string, value any) {
	if number, ok := value.(float64); ok && number == math.Trunc(number) && math.Abs(number) < 1<<53 {
		value = int64(number)
	}
	var node any = object
	segments := strings.Split(path, ".")
	for position, segment := range segments {
		name, indexes := segment, []int(nil)
		if open := strings.IndexByte(segment, '['); open >= 0 {
			name = segment[:open]
			for _, part := range strings.Split(strings.ReplaceAll(segment[open:], "]", ""), "[")[1:] {
				index, err := strconv.Atoi(part)
				if err != nil {
					return
				}
				indexes = append(indexes, index)
			}
		}
		parent, ok := node.(map[string]any)
		if !ok {
			return
		}
		if position == len(segments)-1 && len(indexes) == 0 {
			parent[name] = value
			return
		}
		node = parent[name]
		for step, index := range indexes {
			items, ok := node.([]any)
			if !ok || index >= len(items) {
				return
			}
			if position == len(segments)-1 && step == len(indexes)-1 {
				items[index] = value
				return
			}
			node = items[index]
		}
	}
}

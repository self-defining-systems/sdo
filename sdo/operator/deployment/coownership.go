package deployment

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/structured-merge-diff/v4/fieldpath"

	"sdo.dev/operator/sdk"
)

// installFieldManagers are the field managers of the tools that install an
// application SDO later adopts: helm, and kubectl's apply, create, replace,
// edit and patch. The API server's own and the controllers' managers are
// never in this set.
var installFieldManagers = map[string]bool{
	"helm":                      true,
	"kubectl":                   true,
	"kubectl-client-side-apply": true,
	"kubectl-create":            true,
	"kubectl-replace":           true,
	"kubectl-edit":              true,
	"kubectl-patch":             true,
}

// releaseCoOwnership gives the deployer the install's claim on every field
// the deployer applies, and on the structs around them. An object the install
// created is co-owned with the deployer for each field the deployer applied
// with the install's value, and a server-side apply deletes a field only when
// its applier is the last owner. A head that swaps a volume's type (emptyDir
// for projected) was refused by admission ("may not specify more than 1 volume
// type") because helm still owned the old source; one that drops a lifecycle
// hook was refused ("must specify a handler type") because helm owned the
// hook's struct, which the deployer's leaf left behind empty. The claims of
// the install managers (helm, kubectl) on a path equal to, above or below a
// path the deployer owns move to the deployer, so its later apply removes
// them with the rest of what it stops sending. Fields on no path the deployer
// applies stay the install's: nothing the deployer never sent is removed, so
// no workload rolls. The state is the record: once the install managers claim
// nothing the deployer owns the call changes nothing, so it runs before every
// apply (ownership from earlier applies) and after it (ownership from this
// one) and is idempotent.
//
// A change that removes a field in the very first apply of an object, before
// the deployer has owned anything on it, finds nothing to take over: the field
// is the install's alone and stays.
//
// It only moves ownership metadata, so a failure never blocks a deployment: it
// is logged, and the next apply tries again.
func (b CommandBackend) releaseCoOwnership(ctx context.Context, documents []byte) {
	output, err := b.run(ctx, b.kubectl(), b.kubectlArgs("get", "--filename=-", "--output=json", "--show-managed-fields", "--ignore-not-found"), documents)
	if err != nil {
		slog.Warn("coownership_read_failed", "error", err, "output", strings.TrimSpace(string(output)))
		return
	}
	objects, err := liveObjects(output)
	if err != nil {
		slog.Warn("coownership_read_failed", "error", err)
		return
	}
	for _, object := range objects {
		patch, err := coOwnershipPatch(object)
		if err != nil {
			slog.Warn("coownership_patch_failed", "kind", object.GetKind(), "name", object.GetName(), "error", err)
			continue
		}
		if patch == nil {
			continue
		}
		args := []string{"patch", resourceOf(object), object.GetName(), "--type=json", "--patch=" + string(patch)}
		if namespace := object.GetNamespace(); namespace != "" {
			args = append(args, "--namespace", namespace)
		}
		if output, err := b.run(ctx, b.kubectl(), b.kubectlArgs(args...), nil); err != nil {
			slog.Warn("coownership_patch_failed", "kind", object.GetKind(), "name", object.GetName(), "error", err, "output", strings.TrimSpace(string(output)))
			continue
		}
		slog.Info("coownership_moved", "kind", object.GetKind(), "name", object.GetName(), "namespace", object.GetNamespace())
	}
}

// liveObjects parses `kubectl get -o json`: a List of the objects found, or the
// single object, or nothing when none exists.
func liveObjects(output []byte) ([]unstructured.Unstructured, error) {
	if len(bytes.TrimSpace(output)) == 0 {
		return nil, nil
	}
	var document unstructured.Unstructured
	if err := json.Unmarshal(output, &document.Object); err != nil {
		return nil, fmt.Errorf("parse live objects: %w", err)
	}
	if document.IsList() {
		var objects []unstructured.Unstructured
		err := document.EachListItem(func(item runtime.Object) error {
			objects = append(objects, *item.(*unstructured.Unstructured))
			return nil
		})
		return objects, err
	}
	return []unstructured.Unstructured{document}, nil
}

// resourceOf is the kind-and-group name kubectl resolves without a REST map
// of its own ("Deployment.apps", "Service").
func resourceOf(object unstructured.Unstructured) string {
	group, _, _ := strings.Cut(object.GetAPIVersion(), "/")
	if !strings.Contains(object.GetAPIVersion(), "/") {
		return object.GetKind()
	}
	return object.GetKind() + "." + group
}

// coOwnershipPatch is the JSON patch that moves, from each install manager's
// entry to the deployer's apply entry, the fields on paths the deployer also
// owns something on (see releaseCoOwnership), or nil when there is none. It
// tests the resourceVersion, so it never overwrites a concurrent change of the
// managed fields.
func coOwnershipPatch(object unstructured.Unstructured) ([]byte, error) {
	entries := object.GetManagedFields()
	deployerAt := -1
	deployer := new(fieldpath.Set)
	for i, entry := range entries {
		if entry.Manager == sdk.DeployerFieldManager && entry.Operation == metav1.ManagedFieldsOperationApply && entry.FieldsV1 != nil {
			if err := deployer.FromJSON(bytes.NewReader(entry.FieldsV1.Raw)); err != nil {
				return nil, fmt.Errorf("read the deployer's managed fields: %w", err)
			}
			deployerAt = i
		}
	}
	if deployerAt < 0 || deployer.Empty() {
		return nil, nil
	}
	// The paths the deployer owns something on: each of its fields and every
	// struct above one.
	around := new(fieldpath.Set)
	deployer.Iterate(func(path fieldpath.Path) {
		for n := 1; n <= len(path); n++ {
			around.Insert(path[:n])
		}
	})
	moved := new(fieldpath.Set)
	kept := make([]metav1.ManagedFieldsEntry, 0, len(entries))
	for i, entry := range entries {
		if i == deployerAt || !installFieldManagers[entry.Manager] || entry.FieldsV1 == nil {
			kept = append(kept, entry)
			continue
		}
		owned := new(fieldpath.Set)
		if err := owned.FromJSON(bytes.NewReader(entry.FieldsV1.Raw)); err != nil {
			return nil, fmt.Errorf("read %s's managed fields: %w", entry.Manager, err)
		}
		take := new(fieldpath.Set)
		owned.Iterate(func(path fieldpath.Path) {
			if around.Has(path) || underAny(deployer, path) {
				take.Insert(path)
			}
		})
		if take.Empty() {
			kept = append(kept, entry)
			continue
		}
		moved = moved.Union(take)
		rest := owned.Difference(take)
		if rest.Empty() {
			continue
		}
		raw, err := rest.ToJSON()
		if err != nil {
			return nil, err
		}
		entry.FieldsV1 = &metav1.FieldsV1{Raw: raw}
		kept = append(kept, entry)
	}
	if moved.Empty() {
		return nil, nil
	}
	raw, err := deployer.Union(moved).ToJSON()
	if err != nil {
		return nil, err
	}
	for i := range kept {
		if kept[i].Manager == sdk.DeployerFieldManager && kept[i].Operation == metav1.ManagedFieldsOperationApply {
			kept[i].FieldsV1 = &metav1.FieldsV1{Raw: raw}
		}
	}
	return json.Marshal([]map[string]any{
		{"op": "test", "path": "/metadata/resourceVersion", "value": object.GetResourceVersion()},
		{"op": "replace", "path": "/metadata/managedFields", "value": kept},
	})
}

// underAny reports whether a proper prefix of path is a field of set.
func underAny(set *fieldpath.Set, path fieldpath.Path) bool {
	for n := 1; n < len(path); n++ {
		if set.Has(path[:n]) {
			return true
		}
	}
	return false
}

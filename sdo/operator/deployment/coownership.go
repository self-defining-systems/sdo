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

// releaseCoOwnership leaves every field the deployer applies to the deployer
// alone. An object the install created is co-owned with the deployer for each
// field the deployer applied with the install's value, and a server-side apply
// that stops sending a co-owned field leaves it in place: a head that swaps a
// volume's type (emptyDir for projected) is then refused by admission ("may
// not specify more than 1 volume type") and SDO holds a change an engineer
// would have deployed. The install managers' claim on exactly those fields is
// dropped; fields only the install owns stay its own, so nothing the deployer
// never applied is removed and no workload rolls. The state is the record:
// once no install manager shares a field with the deployer the call changes
// nothing, so it runs before every apply (ownership from earlier applies) and
// after it (ownership from this one) and is idempotent.
//
// A change that removes a field in the very first apply, before the deployer
// has applied the object even once, finds no shared field to release: the
// field is the install's alone and stays.
//
// It only trims ownership metadata, so a failure never blocks a deployment: it
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
		slog.Info("coownership_released", "kind", object.GetKind(), "name", object.GetName(), "namespace", object.GetNamespace())
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

// coOwnershipPatch is the JSON patch that removes from each install manager's
// entry the fields the deployer's apply entry also owns, or nil when none is
// shared. It tests the resourceVersion, so it never overwrites a concurrent
// change of the managed fields.
func coOwnershipPatch(object unstructured.Unstructured) ([]byte, error) {
	entries := object.GetManagedFields()
	deployer := new(fieldpath.Set)
	for _, entry := range entries {
		if entry.Manager == sdk.DeployerFieldManager && entry.Operation == metav1.ManagedFieldsOperationApply && entry.FieldsV1 != nil {
			if err := deployer.FromJSON(bytes.NewReader(entry.FieldsV1.Raw)); err != nil {
				return nil, fmt.Errorf("read the deployer's managed fields: %w", err)
			}
		}
	}
	if deployer.Empty() {
		return nil, nil
	}
	changed := false
	kept := make([]metav1.ManagedFieldsEntry, 0, len(entries))
	for _, entry := range entries {
		if !installFieldManagers[entry.Manager] || entry.FieldsV1 == nil {
			kept = append(kept, entry)
			continue
		}
		owned := new(fieldpath.Set)
		if err := owned.FromJSON(bytes.NewReader(entry.FieldsV1.Raw)); err != nil {
			return nil, fmt.Errorf("read %s's managed fields: %w", entry.Manager, err)
		}
		alone := owned.Difference(deployer)
		if alone.Equals(owned) {
			kept = append(kept, entry)
			continue
		}
		changed = true
		if alone.Empty() {
			continue
		}
		raw, err := alone.ToJSON()
		if err != nil {
			return nil, err
		}
		entry.FieldsV1 = &metav1.FieldsV1{Raw: raw}
		kept = append(kept, entry)
	}
	if !changed {
		return nil, nil
	}
	return json.Marshal([]map[string]any{
		{"op": "test", "path": "/metadata/resourceVersion", "value": object.GetResourceVersion()},
		{"op": "replace", "path": "/metadata/managedFields", "value": kept},
	})
}

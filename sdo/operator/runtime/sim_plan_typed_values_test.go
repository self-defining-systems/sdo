package runtime

import (
	"context"
	"errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk"
)

// simTypedKube extends the existing deterministic API-server simulation with
// named Pod effects, a refused exec subresource, and explicit failed/stale writes.
type simTypedKube struct {
	*simKube
	mode       string
	once       bool
	deletedUID types.UID
	execCalls  int
	podReads   int
}

func (k *simTypedKube) Get(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if ref.Kind == "Pod" && (k.mode == "exec-version-bumped" || k.mode == "exec-pod-recreated") {
		k.podReads++
		if k.podReads == 2 {
			if k.mode == "exec-pod-recreated" {
				k.objects[ref].SetUID("replacement-pod")
			}
			k.bump(k.objects[ref])
		}
	}
	if k.objects[ref] == nil {
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, ref.Name)
	}
	return k.simKube.Get(ctx, ref)
}

func (k *simTypedKube) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, raw []byte) error {
	if !k.once {
		k.once = true
		switch k.mode {
		case "write-error":
			return apierrors.NewInternalError(errors.New("seeded write failure"))
		case "stale-write":
			return nil // an acknowledgement without the requested effect
		}
	}
	return k.simKube.Patch(ctx, ref, kind, raw)
}

func (k *simTypedKube) Delete(_ context.Context, ref sdk.ObjectRef, uid types.UID, rv string) error {
	object := k.objects[ref]
	if object == nil {
		return apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, ref.Name)
	}
	if !k.once {
		k.once = true
		switch k.mode {
		case "conflict":
			k.bump(object)
		case "pod-recreated":
			object.SetUID("replacement-pod")
			k.bump(object)
		case "write-error":
			return apierrors.NewInternalError(errors.New("seeded delete failure"))
		case "stale-write":
			return nil
		}
	}
	if object.GetUID() != uid || object.GetResourceVersion() != rv {
		return apierrors.NewConflict(schema.GroupResource{Resource: "pods"}, ref.Name, errors.New("identity changed"))
	}
	k.deletedUID = uid
	delete(k.objects, ref)
	if k.mode == "replacement-after-delete" {
		replacement := object.DeepCopy()
		replacement.SetUID("replacement-pod")
		k.objects[ref] = replacement
	}
	return nil
}

func (k *simTypedKube) Exec(_ context.Context, ref sdk.ObjectRef, _ string, _ []string) (int, error) {
	k.execCalls++
	return 0, apierrors.NewForbidden(schema.GroupResource{Resource: "pods/exec"}, ref.Name, errors.New("no controller grant"))
}

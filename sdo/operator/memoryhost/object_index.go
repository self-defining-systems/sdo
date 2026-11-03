package memoryhost

import (
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
)

// ObjectIndex projects an immutable snapshot once, rather than converting every
// typed list again for each object read. Missing identities still belong to the
// caller's scoped/generic reader; Secrets never enter this read surface.
type ObjectIndex struct {
	once     sync.Once
	view     sdk.DetectionContext
	captured []unstructured.Unstructured
	objects  map[sdk.ObjectRef]unstructured.Unstructured
}

func NewObjectIndex(view sdk.DetectionContext, captured []unstructured.Unstructured) *ObjectIndex {
	return &ObjectIndex{view: view, captured: captured}
}

// Lookup returns a private copy. Explicitly captured objects take precedence
// over the typed compatibility projection, as they do in replay snapshots.
func (i *ObjectIndex) Lookup(ref sdk.ObjectRef) (*unstructured.Unstructured, bool) {
	if strings.EqualFold(ref.Kind, "Secret") {
		return nil, false
	}
	i.once.Do(func() {
		i.objects = map[sdk.ObjectRef]unstructured.Unstructured{}
		for _, objects := range [][]unstructured.Unstructured{i.captured, sdk.TypedObjects(i.view)} {
			for _, object := range objects {
				key := sdk.ObjectIdentity(object)
				if _, exists := i.objects[key]; !exists {
					i.objects[key] = *object.DeepCopy()
				}
			}
		}
		i.view = nil
		i.captured = nil
	})
	object, ok := i.objects[ref]
	if !ok {
		return nil, false
	}
	return object.DeepCopy(), true
}

package sdk

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// ObjectReader reads an exact, versioned identity. Returned objects are private copies.
// An absent or uncaptured identity is an error, never an empty healthy object.
type ObjectReader interface {
	Object(ObjectRef) (*unstructured.Unstructured, error)
}

// ObjectCaptureSource freezes discovery-backed objects at an incident boundary.
// References extend the application namespace to incident-touched scopes.
type ObjectCaptureSource interface {
	CaptureObjects([]ObjectRef) ([]unstructured.Unstructured, error)
}

var objectScheme = func() *runtime.Scheme {
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, appsv1.AddToScheme, autoscalingv2.AddToScheme, discoveryv1.AddToScheme, networkingv1.AddToScheme, policyv1.AddToScheme, admissionv1.AddToScheme} {
		if err := add(s); err != nil {
			panic(err)
		}
	}
	return s
}()

// TypedObjects is the compatibility edge for existing typed detector views and
// old fixtures. It discovers zero-argument Kubernetes slice getters by type;
// capture/replay have no separate kind inventories.
func TypedObjects(view any) []unstructured.Unstructured {
	var out []unstructured.Unstructured
	v := reflect.ValueOf(view)
	if !v.IsValid() {
		return out
	}
	for i := 0; i < v.NumMethod(); i++ {
		m := v.Method(i)
		t := m.Type()
		if t.NumIn() != 0 || t.NumOut() != 1 || t.Out(0).Kind() != reflect.Slice {
			continue
		}
		elem := t.Out(0).Elem()
		if elem.Kind() != reflect.Struct || !reflect.PointerTo(elem).Implements(reflect.TypeOf((*runtime.Object)(nil)).Elem()) {
			continue
		}
		values := m.Call(nil)[0]
		for j := 0; j < values.Len(); j++ {
			ptr := reflect.New(elem)
			ptr.Elem().Set(values.Index(j))
			obj := ptr.Interface().(runtime.Object)
			fields, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
			if err != nil {
				continue
			}
			u := unstructured.Unstructured{Object: fields}
			if u.GetAPIVersion() == "" || u.GetKind() == "" {
				gvks, _, err := objectScheme.ObjectKinds(obj)
				if err != nil || len(gvks) == 0 {
					continue
				}
				u.SetGroupVersionKind(gvks[0])
			}
			out = append(out, u)
		}
	}
	SortObjects(out)
	return out
}
func SortObjects(objects []unstructured.Unstructured) {
	sort.Slice(objects, func(i, j int) bool {
		return fmt.Sprint(ObjectIdentity(objects[i])) < fmt.Sprint(ObjectIdentity(objects[j]))
	})
}
func ObjectIdentity(o unstructured.Unstructured) ObjectRef {
	return ObjectRef{APIVersion: o.GetAPIVersion(), Kind: o.GetKind(), Namespace: o.GetNamespace(), Name: o.GetName()}
}

var ErrObjectNotCaptured = errors.New("object not captured")

func FindObject(objects []unstructured.Unstructured, ref ObjectRef) (*unstructured.Unstructured, error) {
	if strings.EqualFold(ref.Kind, "Secret") {
		return nil, fmt.Errorf("Secret reads forbidden")
	}
	for _, o := range objects {
		if ObjectIdentity(o) == ref {
			return o.DeepCopy(), nil
		}
	}
	return nil, fmt.Errorf("%w: %s/%s %s/%s", ErrObjectNotCaptured, ref.APIVersion, ref.Kind, ref.Namespace, ref.Name)
}

// TypedObjectFields returns target's typed Kubernetes list fields, the ones
// DecodeTypedObjects fills. They are a projection of the generic objects.
func TypedObjectFields(target any) []reflect.Value {
	var fields []reflect.Value
	v := reflect.ValueOf(target).Elem()
	for i := 0; i < v.NumField(); i++ {
		if _, ok := typedObjectKind(v.Field(i)); ok {
			fields = append(fields, v.Field(i))
		}
	}
	return fields
}

func typedObjectKind(field reflect.Value) (schema.GroupVersionKind, bool) {
	if !field.CanSet() || field.Kind() != reflect.Slice {
		return schema.GroupVersionKind{}, false
	}
	elem := field.Type().Elem()
	if elem == reflect.TypeOf(unstructured.Unstructured{}) || elem.Kind() != reflect.Struct || !reflect.PointerTo(elem).Implements(reflect.TypeOf((*runtime.Object)(nil)).Elem()) {
		return schema.GroupVersionKind{}, false
	}
	probe := reflect.New(elem).Interface().(runtime.Object)
	gvks, _, err := objectScheme.ObjectKinds(probe)
	if err != nil || len(gvks) == 0 {
		return schema.GroupVersionKind{}, false
	}
	return gvks[0], true
}

// DecodeTypedObjects maintains the legacy typed fixture edge without a kind map.
// New kinds remain in Objects and are available through ObjectReader.
func DecodeTypedObjects(target any, objects []unstructured.Unstructured) error {
	for _, field := range TypedObjectFields(target) {
		gvk, _ := typedObjectKind(field)
		elem := field.Type().Elem()
		for _, object := range objects {
			if object.GroupVersionKind() != gvk {
				continue
			}
			value := reflect.New(elem)
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.Object, value.Interface()); err != nil {
				return err
			}
			field.Set(reflect.Append(field, value.Elem()))
		}
	}
	return nil
}

package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	ktesting "k8s.io/client-go/testing"
	operator "sdo.dev/operator/runtime"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func objectRef(o *unstructured.Unstructured) sdk.ObjectRef { return sdk.ObjectIdentity(*o) }
func fixtureResource(ref sdk.ObjectRef) schema.GroupVersionResource {
	gvr, _ := meta.UnsafeGuessKindToResource(schema.FromAPIVersionAndKind(ref.APIVersion, ref.Kind))
	return gvr
}
func snapshotObjects(s sdktest.Snapshot) (map[sdk.ObjectRef]*unstructured.Unstructured, error) {
	out := map[sdk.ObjectRef]*unstructured.Unstructured{}
	add := func(object unstructured.Unstructured) error {
		ref := sdk.ObjectIdentity(object)
		if strings.EqualFold(ref.Kind, "Secret") {
			return fmt.Errorf("Secret fixture objects are forbidden")
		}
		if ref.Kind == "" || ref.APIVersion == "" || ref.Name == "" {
			return fmt.Errorf("fixture object identity is incomplete")
		}
		out[ref] = object.DeepCopy()
		return nil
	}
	for _, object := range sdk.TypedObjects(s) {
		if object.GetNamespace() == "" {
			object.SetNamespace(s.NamespaceName)
		}
		if err := add(object); err != nil {
			return nil, err
		}
	}
	// Compatibility for old scoped fixtures uses field types, not kind tables.
	v := reflect.ValueOf(s.ScopedSnapshot)
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		if field.Kind() != reflect.Slice {
			continue
		}
		elem := field.Type().Elem()
		if elem.Kind() != reflect.Struct || !reflect.PointerTo(elem).Implements(reflect.TypeOf((*kruntime.Object)(nil)).Elem()) {
			continue
		}
		for j := 0; j < field.Len(); j++ {
			ptr := reflect.New(elem)
			ptr.Elem().Set(field.Index(j))
			obj := ptr.Interface().(kruntime.Object)
			fields, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(obj)
			if err != nil {
				return nil, err
			}
			gvks, _, err := scheme.Scheme.ObjectKinds(obj)
			if err != nil || len(gvks) == 0 {
				return nil, fmt.Errorf("scoped fixture GVK unavailable")
			}
			object := unstructured.Unstructured{Object: fields}
			object.SetGroupVersionKind(gvks[0])
			if err := add(object); err != nil {
				return nil, err
			}
		}
	}
	// Canonical generic objects override their legacy compatibility projections.
	for _, object := range s.Objects {
		if err := add(object); err != nil {
			return nil, err
		}
	}
	return out, nil
}
func newCluster(s sdktest.Snapshot) (*cluster, error) {
	objects, err := snapshotObjects(s)
	if err != nil {
		return nil, err
	}
	fake := kubefake.NewSimpleClientset()
	c := &cluster{TypedPlanClient: operator.TypedPlanClient{Client: fake}, fake: fake, initial: objects, original: s}
	for ref, object := range objects {
		if object.GetUID() == "" {
			object.SetUID(types.UID("offline:" + ref.Kind + ":" + ref.Name))
		}
		if object.GetResourceVersion() == "" {
			object.SetResourceVersion("1")
		}
		if err := fake.Tracker().Add(object); err != nil {
			return nil, err
		}
	}
	fake.PrependReactor("create", "*", func(action ktesting.Action) (bool, kruntime.Object, error) {
		object := action.(ktesting.CreateAction).GetObject()
		access, err := meta.Accessor(object)
		if err != nil {
			return true, nil, err
		}
		access.SetUID(types.UID("offline:created:" + access.GetName()))
		access.SetResourceVersion("1")
		return false, nil, nil
	})
	fake.PrependReactor("delete", "*", func(action ktesting.Action) (bool, kruntime.Object, error) {
		deletion := action.(ktesting.DeleteAction)
		object, err := fake.Tracker().Get(action.GetResource(), action.GetNamespace(), deletion.GetName())
		if err != nil {
			return true, nil, err
		}
		access, err := meta.Accessor(object)
		if err != nil {
			return true, nil, err
		}
		pre := deletion.GetDeleteOptions().Preconditions
		if pre == nil || pre.UID == nil || pre.ResourceVersion == nil || *pre.UID != access.GetUID() || *pre.ResourceVersion != access.GetResourceVersion() {
			return true, nil, apierrors.NewConflict(action.GetResource().GroupResource(), deletion.GetName(), fmt.Errorf("identity preconditions differ"))
		}
		return false, nil, nil
	})
	return c, nil
}
func fixtureUnstructured(object kruntime.Object, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if object == nil {
		return nil, fmt.Errorf("fixture returned no object")
	}
	fields, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(object)
	if err != nil {
		return nil, err
	}
	out := &unstructured.Unstructured{Object: fields}
	out.SetAPIVersion(ref.APIVersion)
	out.SetKind(ref.Kind)
	return out, nil
}
func (c *cluster) Get(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.EqualFold(ref.Kind, "Secret") {
		return nil, fmt.Errorf("Secret reads forbidden")
	}
	object, err := c.fake.Invokes(ktesting.NewGetAction(fixtureResource(ref), ref.Namespace, ref.Name), &unstructured.Unstructured{})
	if err != nil {
		return nil, err
	}
	return fixtureUnstructured(object, ref)
}
func (c *cluster) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.EqualFold(ref.Kind, "Secret") {
		return fmt.Errorf("Secret writes forbidden")
	}
	_, err := c.fake.Invokes(ktesting.NewPatchAction(fixtureResource(ref), ref.Namespace, ref.Name, kind, patch), &unstructured.Unstructured{})
	return err
}
func (c *cluster) Create(ctx context.Context, ref sdk.ObjectRef, object *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if object == nil || sdk.ObjectIdentity(*object) != ref || strings.EqualFold(ref.Kind, "Secret") {
		return nil, fmt.Errorf("create requires matching nonsecret identity")
	}
	created, err := c.fake.Invokes(ktesting.NewCreateAction(fixtureResource(ref), ref.Namespace, object.DeepCopy()), &unstructured.Unstructured{})
	if err != nil {
		return nil, err
	}
	return fixtureUnstructured(created, ref)
}
func (c *cluster) Delete(ctx context.Context, ref sdk.ObjectRef, uid types.UID, version string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.EqualFold(ref.Kind, "Secret") {
		return fmt.Errorf("Secret writes forbidden")
	}
	_, err := c.fake.Invokes(ktesting.NewDeleteActionWithOptions(fixtureResource(ref), ref.Namespace, ref.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &version}}), &unstructured.Unstructured{})
	return err
}
func (c *cluster) objects(ctx context.Context) (map[sdk.ObjectRef]*unstructured.Unstructured, error) {
	refs := map[sdk.ObjectRef]bool{}
	for ref := range c.initial {
		refs[ref] = true
	}
	// Client-go records creates, including controller reactions and subsequently
	// deleted objects. This is the fixture inventory; there is no resource allowlist.
	for _, action := range c.fake.Actions() {
		if action.GetVerb() != "create" {
			continue
		}
		creation, ok := action.(ktesting.CreateAction)
		if !ok {
			continue
		}
		object := creation.GetObject()
		fields, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(object)
		if err != nil {
			return nil, err
		}
		o := unstructured.Unstructured{Object: fields}
		if o.GetKind() == "" {
			gvks, _, err := scheme.Scheme.ObjectKinds(object)
			if err != nil || len(gvks) == 0 {
				return nil, fmt.Errorf("created fixture GVK unavailable")
			}
			o.SetGroupVersionKind(gvks[0])
		}
		refs[sdk.ObjectIdentity(o)] = true
	}
	out := map[sdk.ObjectRef]*unstructured.Unstructured{}
	for ref := range refs {
		object, err := c.Get(ctx, ref)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out[ref] = object
	}
	return out, nil
}
func (c *cluster) snapshot(ctx context.Context) (sdktest.Snapshot, error) {
	objects, err := c.objects(ctx)
	if err != nil {
		return sdktest.Snapshot{}, err
	}
	original := c.original.ScopedSnapshot
	s := sdktest.Snapshot{NamespaceName: c.original.NamespaceName, ObjectSnapshotVersion: 1, ScopedSnapshot: sdk.ScopedSnapshot{Version: original.Version, Scope: original.Scope, Generation: original.Generation, Statuses: original.Statuses}}
	var local, scoped []unstructured.Unstructured
	for ref, object := range objects {
		s.Objects = append(s.Objects, *object.DeepCopy())
		if ref.Namespace == s.NamespaceName {
			local = append(local, *object.DeepCopy())
		}
		for _, target := range original.Scope.Targets {
			if target.APIVersion == ref.APIVersion && target.Kind == ref.Kind && target.Namespace == ref.Namespace && (target.Name == "" || target.Name == ref.Name) {
				scoped = append(scoped, *object.DeepCopy())
				break
			}
		}
	}
	for _, target := range c.catalogConfigurations {
		catalog := c.catalogs[databaseKey(target)]
		args, _ := json.Marshal([]any{target})
		result, _ := json.Marshal(catalog.users)
		s.Readings = append(s.Readings, sdktest.PrimitiveCall{Method: "DatabaseUsers", Arguments: args, Result: result})
	}
	sort.Slice(s.Readings, func(i, j int) bool { return string(s.Readings[i].Arguments) < string(s.Readings[j].Arguments) })
	sdk.SortObjects(s.Objects)
	if err := sdk.DecodeTypedObjects(&s, local); err != nil {
		return sdktest.Snapshot{}, err
	}
	if err := sdk.DecodeTypedObjects(&s.ScopedSnapshot, scoped); err != nil {
		return sdktest.Snapshot{}, err
	}
	return s, nil
}

type source struct {
	accepted  map[sdk.ObjectRef]*unstructured.Unstructured
	revisions map[string]map[sdk.ObjectRef]*unstructured.Unstructured
}

func newSource(c Case) (*source, error) {
	s := &source{accepted: map[sdk.ObjectRef]*unstructured.Unstructured{}, revisions: map[string]map[sdk.ObjectRef]*unstructured.Unstructured{}}
	add := func(dst map[sdk.ObjectRef]*unstructured.Unstructured, objects []*unstructured.Unstructured) error {
		for _, o := range objects {
			if o == nil || o.GetKind() == "Secret" {
				return fmt.Errorf("accepted source must contain only nonsecret objects")
			}
			ref := objectRef(o)
			if ref.Kind == "" || ref.APIVersion == "" || ref.Name == "" {
				return fmt.Errorf("accepted source object identity is incomplete")
			}
			if _, ok := dst[ref]; ok {
				return fmt.Errorf("duplicate accepted source object")
			}
			dst[ref] = o.DeepCopy()
		}
		return nil
	}
	if err := add(s.accepted, c.Accepted); err != nil {
		return nil, err
	}
	for rev, objects := range c.Revisions {
		dst := map[sdk.ObjectRef]*unstructured.Unstructured{}
		if err := add(dst, objects); err != nil {
			return nil, err
		}
		s.revisions[rev] = dst
	}
	return s, nil
}
func (s *source) AcceptedManifest(_ context.Context, _ string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if o := s.accepted[ref]; o != nil {
		return o.DeepCopy(), nil
	}
	return nil, operator.ErrAcceptedManifestAbsent
}
func (s *source) AcceptedScopedManifest(ctx context.Context, commit, namespace string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	return s.AcceptedManifest(ctx, commit, ref)
}
func (s *source) Revision(_ context.Context, ref sdk.ObjectRef, revision string) (*unstructured.Unstructured, error) {
	if o := s.revisions[revision][ref]; o != nil {
		return o.DeepCopy(), nil
	}
	return nil, fmt.Errorf("explicit revision fixture unavailable")
}
func (s *source) AcceptedManifestRefs(context.Context, string, string) ([]sdk.ObjectRef, error) {
	out := []sdk.ObjectRef{}
	for ref := range s.accepted {
		out = append(out, ref)
	}
	return out, nil
}

// reaction never copies the fixed fixture. Rollout status and controller-owned
// pod recreation/readiness are explicit simulation assumptions; external
// network and exec observations are never invented.
type reaction struct {
	cluster *cluster
	source  *source
	plan    program.Plan
	effects *[]string
	done    map[int]bool
}

func (w *reaction) Wait(ctx context.Context, refs []sdk.ObjectRef) error {
	for _, ref := range refs {
		if ref.APIVersion != "apps/v1" || (ref.Kind != "Deployment" && ref.Kind != "StatefulSet") {
			continue
		}
		o, err := w.cluster.Get(ctx, ref)
		if err != nil {
			return err
		}
		template, _, _ := unstructured.NestedMap(o.Object, "spec", "template")
		if !w.acceptedTemplate(ref, template) {
			*w.effects = append(*w.effects, "simulated:rollout-unresolved:"+ref.Kind+"/"+ref.Name)
			continue
		}
		replicas, _, _ := unstructured.NestedInt64(o.Object, "spec", "replicas")

		status := map[string]any{"observedGeneration": o.GetGeneration(), "replicas": replicas, "updatedReplicas": replicas, "readyReplicas": replicas, "availableReplicas": replicas}
		raw, _ := json.Marshal(map[string]any{"status": status})
		if err = w.cluster.Patch(ctx, ref, types.MergePatchType, raw); err != nil {
			return err
		}
		if w.cluster.simStatus == nil {
			w.cluster.simStatus = map[sdk.ObjectRef]map[string]any{}
		}
		observed, err := w.cluster.Get(ctx, ref)
		if err != nil {
			return err
		}
		recorded, _, err := unstructured.NestedMap(observed.Object, "status")
		if err != nil {
			return err
		}
		w.cluster.simStatus[ref] = recorded
		*w.effects = append(*w.effects, "simulated:rollout-status:"+ref.Kind+"/"+ref.Name)
	}
	return nil
}

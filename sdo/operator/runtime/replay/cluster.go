package replay

import (
	"context"
	"encoding/json"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

type resource struct{ field, version, kind, plural string }

var resources = []resource{
	{"configMaps", "v1", "ConfigMap", "configmaps"}, {"services", "v1", "Service", "services"}, {"pods", "v1", "Pod", "pods"}, {"deployments", "apps/v1", "Deployment", "deployments"}, {"statefulSets", "apps/v1", "StatefulSet", "statefulsets"}, {"replicaSets", "apps/v1", "ReplicaSet", "replicasets"}, {"horizontalPodAutoscalers", "autoscaling/v2", "HorizontalPodAutoscaler", "horizontalpodautoscalers"}, {"podDisruptionBudgets", "policy/v1", "PodDisruptionBudget", "poddisruptionbudgets"}, {"endpoints", "v1", "Endpoints", "endpoints"}, {"endpointSlices", "discovery.k8s.io/v1", "EndpointSlice", "endpointslices"}, {"networkPolicies", "networking.k8s.io/v1", "NetworkPolicy", "networkpolicies"}, {"events", "v1", "Event", "events"},
}

var scopedResources = []resource{
	{"deployments", "apps/v1", "Deployment", "deployments"}, {"services", "v1", "Service", "services"}, {"endpoints", "v1", "Endpoints", "endpoints"},
	{"mutating_webhooks", "admissionregistration.k8s.io/v1", "MutatingWebhookConfiguration", "mutatingwebhookconfigurations"},
	{"validating_webhooks", "admissionregistration.k8s.io/v1", "ValidatingWebhookConfiguration", "validatingwebhookconfigurations"},
}

func allResources() []resource {
	out := append([]resource{}, resources...)
	return append(out, scopedResources[3:]...)
}
func (r resource) gvk() schema.GroupVersionKind {
	return schema.FromAPIVersionAndKind(r.version, r.kind)
}
func (r resource) gvr() schema.GroupVersionResource {
	return r.gvk().GroupVersion().WithResource(r.plural)
}
func objectRef(o *unstructured.Unstructured) sdk.ObjectRef {
	return sdk.ObjectRef{APIVersion: o.GetAPIVersion(), Kind: o.GetKind(), Namespace: o.GetNamespace(), Name: o.GetName()}
}
func snapshotObjects(s sdktest.Snapshot) (map[sdk.ObjectRef]*unstructured.Unstructured, error) {
	raw, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	var scoped map[string]json.RawMessage
	if err = json.Unmarshal(fields["scoped_observation"], &scoped); err != nil {
		return nil, err
	}
	out := map[sdk.ObjectRef]*unstructured.Unstructured{}
	for index, r := range append(append([]resource{}, resources...), scopedResources...) {
		var items []map[string]any
		rawField := fields[r.field]
		if index >= len(resources) {
			rawField = scoped[r.field]
		}
		if len(rawField) == 0 {
			continue
		}
		if err = json.Unmarshal(rawField, &items); err != nil {
			return nil, err
		}
		for _, item := range items {
			item["apiVersion"] = r.version
			item["kind"] = r.kind
			raw, e := json.Marshal(item)
			if e != nil {
				return nil, e
			}
			o := &unstructured.Unstructured{}
			if e = o.UnmarshalJSON(raw); e != nil {
				return nil, e
			}
			o.SetAPIVersion(r.version)
			o.SetKind(r.kind)
			if o.GetNamespace() == "" && index < len(resources) {
				o.SetNamespace(s.NamespaceName)
			}
			ref := objectRef(o)
			if ref.Name == "" {
				return nil, fmt.Errorf("fixture object has no name")
			}
			out[ref] = o
		}
	}
	return out, nil
}
func newCluster(s sdktest.Snapshot) (*cluster, error) {
	objects, err := snapshotObjects(s)
	if err != nil {
		return nil, err
	}
	f := kubefake.NewSimpleClientset()
	c := &cluster{TypedPlanClient: operator.TypedPlanClient{Client: f}, fake: f, initial: objects, original: s}
	for ref, o := range objects {
		if o.GetUID() == "" {
			o.SetUID(types.UID("offline:" + ref.Kind + ":" + ref.Name))
		}
		if o.GetResourceVersion() == "" {
			o.SetResourceVersion("1")
		}
		typed, e := scheme.Scheme.New(schema.FromAPIVersionAndKind(ref.APIVersion, ref.Kind))
		if e != nil {
			return nil, e
		}
		if e = kruntime.DefaultUnstructuredConverter.FromUnstructured(o.Object, typed); e != nil {
			return nil, e
		}
		if e = f.Tracker().Add(typed); e != nil {
			return nil, e
		}
	}
	// The tracker has no API-server admission/defaulting. Synthesize identity
	// metadata only, preserving all authored and mutated application fields.
	f.PrependReactor("create", "*", func(a ktesting.Action) (bool, kruntime.Object, error) {
		action := a.(ktesting.CreateAction)
		o := action.GetObject()
		access, ok := o.(metav1.Object)
		if !ok {
			return true, nil, fmt.Errorf("created object has no metadata")
		}
		access.SetUID(types.UID("offline:created:" + access.GetName()))
		access.SetResourceVersion("1")
		return false, nil, nil
	})
	// client-go does not enforce delete preconditions itself.
	f.PrependReactor("delete", "*", func(a ktesting.Action) (bool, kruntime.Object, error) {
		action := a.(ktesting.DeleteAction)
		o, e := f.Tracker().Get(a.GetResource(), a.GetNamespace(), action.GetName())
		if e != nil {
			return true, nil, e
		}
		meta := o.(metav1.Object)
		pre := action.GetDeleteOptions().Preconditions
		if pre == nil || pre.UID == nil || pre.ResourceVersion == nil || *pre.UID != meta.GetUID() || *pre.ResourceVersion != meta.GetResourceVersion() {
			return true, nil, apierrors.NewConflict(a.GetResource().GroupResource(), action.GetName(), fmt.Errorf("identity preconditions differ"))
		}
		return false, nil, nil
	})
	return c, nil
}
func (c *cluster) objects(ctx context.Context) (map[sdk.ObjectRef]*unstructured.Unstructured, error) {
	out := map[sdk.ObjectRef]*unstructured.Unstructured{}
	for _, r := range allResources() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		list, e := c.fake.Tracker().List(r.gvr(), r.gvk(), "")
		if e != nil {
			return nil, e
		}
		raw, e := json.Marshal(list)
		if e != nil {
			return nil, e
		}
		var data struct {
			Items []json.RawMessage `json:"items"`
		}
		if e = json.Unmarshal(raw, &data); e != nil {
			return nil, e
		}
		for _, item := range data.Items {
			o := &unstructured.Unstructured{}
			var fields map[string]any
			if e = json.Unmarshal(item, &fields); e != nil {
				return nil, e
			}
			fields["apiVersion"] = r.version
			fields["kind"] = r.kind
			item, e = json.Marshal(fields)
			if e != nil {
				return nil, e
			}
			if e = o.UnmarshalJSON(item); e != nil {
				return nil, e
			}
			o.SetAPIVersion(r.version)
			o.SetKind(r.kind)
			out[objectRef(o)] = o
		}
	}
	return out, nil
}
func (c *cluster) snapshot(ctx context.Context) (sdktest.Snapshot, error) {
	objects, err := c.objects(ctx)
	if err != nil {
		return sdktest.Snapshot{}, err
	}
	scopedRaw, _ := json.Marshal(c.original.ScopedSnapshot)
	var scoped map[string]any
	if err = json.Unmarshal(scopedRaw, &scoped); err != nil {
		return sdktest.Snapshot{}, err
	}
	fields := map[string]any{"namespace": c.original.NamespaceName, "scoped_observation": scoped}
	for _, r := range resources {
		items := []any{}
		for ref, o := range objects {
			if ref.APIVersion == r.version && ref.Kind == r.kind && ref.Namespace == c.original.NamespaceName {
				items = append(items, o.Object)
			}
		}
		fields[r.field] = items
	}
	for _, r := range scopedResources {
		items := []any{}
		for ref, o := range objects {
			if ref.APIVersion != r.version || ref.Kind != r.kind {
				continue
			}
			for _, target := range c.original.ScopedSnapshot.Scope.Targets {
				if target.APIVersion == ref.APIVersion && target.Kind == ref.Kind && target.Namespace == ref.Namespace && (target.Name == "" || target.Name == ref.Name) {
					items = append(items, o.Object)
					break
				}
			}
		}
		scoped[r.field] = items
	}
	raw, err := json.Marshal(fields)
	if err != nil {
		return sdktest.Snapshot{}, err
	}
	var s sdktest.Snapshot
	err = json.Unmarshal(raw, &s)
	return s, err
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

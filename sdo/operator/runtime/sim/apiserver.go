package sim

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	jsonpatch "github.com/evanphx/json-patch"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/managedfields"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/kube-openapi/pkg/validation/spec"
	"sdo.dev/operator/sdk"
	"sigs.k8s.io/yaml"
)

//go:embed testdata/openapi-subset.json
var openapiSubset []byte

var (
	typeConverterOnce sync.Once
	typeConverter     managedfields.TypeConverter
)

// serverTypes is the real server's OpenAPI schema for the served kinds, cut from
// the pinned kube-apiserver by TestRegenerateSimSchema. Server-side apply uses
// it for list-map keys, atomic lists and field ownership, as the server does.
func serverTypes() managedfields.TypeConverter {
	typeConverterOnce.Do(func() {
		var doc struct {
			Definitions map[string]*spec.Schema `json:"definitions"`
		}
		if err := json.Unmarshal(openapiSubset, &doc); err != nil {
			panic(err)
		}
		tc, err := managedfields.NewTypeConverter(doc.Definitions, false)
		if err != nil {
			panic(err)
		}
		typeConverter = tc
	})
	return typeConverter
}

// kindInfo describes one served resource.
type kindInfo struct {
	plural, kind, group, version string
	namespaced                   bool
	// hasStatus: status is a subresource, so main-resource writes ignore status
	// and the status endpoint ignores everything else.
	hasStatus bool
	// generation: spec changes bump metadata.generation.
	generation bool
	shortNames []string
	categories []string
	scale      bool
}

var allVerbs = []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"}

var servedKinds = []kindInfo{
	{plural: "configmaps", kind: "ConfigMap", version: "v1", namespaced: true, shortNames: []string{"cm"}},
	{plural: "endpoints", kind: "Endpoints", version: "v1", namespaced: true, shortNames: []string{"ep"}},
	{plural: "events", kind: "Event", version: "v1", namespaced: true, shortNames: []string{"ev"}},
	{plural: "nodes", kind: "Node", version: "v1", hasStatus: true, shortNames: []string{"no"}},
	{plural: "persistentvolumeclaims", kind: "PersistentVolumeClaim", version: "v1", namespaced: true, hasStatus: true, shortNames: []string{"pvc"}},
	{plural: "pods", categories: []string{"all"}, kind: "Pod", version: "v1", namespaced: true, hasStatus: true, generation: true, shortNames: []string{"po"}},
	{plural: "secrets", kind: "Secret", version: "v1", namespaced: true},
	{plural: "serviceaccounts", kind: "ServiceAccount", version: "v1", namespaced: true, shortNames: []string{"sa"}},
	{plural: "services", categories: []string{"all"}, kind: "Service", version: "v1", namespaced: true, hasStatus: true, shortNames: []string{"svc"}},
	{plural: "daemonsets", categories: []string{"all"}, kind: "DaemonSet", group: "apps", version: "v1", namespaced: true, hasStatus: true, generation: true, shortNames: []string{"ds"}},
	{plural: "deployments", scale: true, categories: []string{"all"}, kind: "Deployment", group: "apps", version: "v1", namespaced: true, hasStatus: true, generation: true, shortNames: []string{"deploy"}},
	{plural: "replicasets", scale: true, categories: []string{"all"}, kind: "ReplicaSet", group: "apps", version: "v1", namespaced: true, hasStatus: true, generation: true, shortNames: []string{"rs"}},
	{plural: "statefulsets", scale: true, categories: []string{"all"}, kind: "StatefulSet", group: "apps", version: "v1", namespaced: true, hasStatus: true, generation: true, shortNames: []string{"sts"}},
	{plural: "horizontalpodautoscalers", categories: []string{"all"}, kind: "HorizontalPodAutoscaler", group: "autoscaling", version: "v2", namespaced: true, hasStatus: true, shortNames: []string{"hpa"}},
	{plural: "cronjobs", categories: []string{"all"}, kind: "CronJob", group: "batch", version: "v1", namespaced: true, hasStatus: true, shortNames: []string{"cj"}},
	{plural: "jobs", categories: []string{"all"}, kind: "Job", group: "batch", version: "v1", namespaced: true, hasStatus: true, generation: true},
	{plural: "roles", kind: "Role", group: "rbac.authorization.k8s.io", version: "v1", namespaced: true},
	{plural: "rolebindings", kind: "RoleBinding", group: "rbac.authorization.k8s.io", version: "v1", namespaced: true},
	{plural: "clusterroles", kind: "ClusterRole", group: "rbac.authorization.k8s.io", version: "v1"},
	{plural: "clusterrolebindings", kind: "ClusterRoleBinding", group: "rbac.authorization.k8s.io", version: "v1"},
	{plural: "leases", kind: "Lease", group: "coordination.k8s.io", version: "v1", namespaced: true},
	{plural: "endpointslices", kind: "EndpointSlice", group: "discovery.k8s.io", version: "v1", namespaced: true},
	{plural: "ingresses", kind: "Ingress", group: "networking.k8s.io", version: "v1", namespaced: true, hasStatus: true, generation: true, shortNames: []string{"ing"}},
	{plural: "networkpolicies", kind: "NetworkPolicy", group: "networking.k8s.io", version: "v1", namespaced: true, shortNames: []string{"netpol"}},
	{plural: "poddisruptionbudgets", kind: "PodDisruptionBudget", group: "policy", version: "v1", namespaced: true, hasStatus: true, generation: true, shortNames: []string{"pdb"}},
}

func (k kindInfo) groupVersion() string {
	if k.group == "" {
		return k.version
	}
	return k.group + "/" + k.version
}
func (k kindInfo) gvk() schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: k.group, Version: k.version, Kind: k.kind}
}
func (k kindInfo) groupResource() schema.GroupResource {
	return schema.GroupResource{Group: k.group, Resource: k.plural}
}

func lookupKind(groupVersion, plural string) (kindInfo, bool) {
	for _, k := range servedKinds {
		if k.groupVersion() == groupVersion && k.plural == plural {
			return k, true
		}
	}
	return kindInfo{}, false
}

// apiServer is the simulated API server's request logic. It reads and writes the
// world's Kube store, so faults, caches and scheduled transitions see every write.
type apiServer struct {
	kube *Kube
	now  func() time.Time
	fms  map[fmKey]*managedfields.FieldManager
	uids int
	log  watchLog
	// nodePorts and clusterIPs allocate the way the service registry does.
	nodePorts, clusterIPs int
}

func newAPIServer(k *Kube) *apiServer {
	return &apiServer{kube: k, now: func() time.Time {
		if k.Clock != nil {
			return k.Clock()
		}
		return time.Now()
	}, fms: map[fmKey]*managedfields.FieldManager{}}
}

type fmKey struct {
	gvk         schema.GroupVersionKind
	subresource string
}

func (s *apiServer) fieldManager(r request) *managedfields.FieldManager {
	key := fmKey{r.info.gvk(), r.subresource}
	if fm, ok := s.fms[key]; ok {
		return fm
	}
	fm, err := managedfields.NewDefaultFieldManager(serverTypes(), identityConvertor{}, defaulter{}, creater{}, key.gvk, key.gvk.GroupVersion(), key.subresource, nil)
	if err != nil {
		panic(err)
	}
	s.fms[key] = fm
	return fm
}

// scopeToEndpoint keeps a write inside the part of the object its endpoint owns:
// the main resource never changes status, the status subresource changes only
// status. The server strips the rest (its reset fields) before recording ownership.
func scopeToEndpoint(r request, live, obj *unstructured.Unstructured) {
	if !r.info.hasStatus || live == nil {
		return
	}
	if r.subresource == "status" {
		kept := live.DeepCopy()
		if status, ok := obj.Object["status"]; ok {
			kept.Object["status"] = status
		} else {
			delete(kept.Object, "status")
		}
		kept.SetManagedFields(obj.GetManagedFields())
		obj.Object = kept.Object
		return
	}
	if status, ok := live.Object["status"]; ok {
		obj.Object["status"] = runtime.DeepCopyJSONValue(status)
	} else {
		delete(obj.Object, "status")
	}
}

// request is one parsed resource request.
type request struct {
	info        kindInfo
	namespace   string
	name        string
	subresource string
	method      string
	query       map[string][]string
	contentType string
	body        []byte
	userAgent   string
	accept      string
}

func (r request) q(key string) string {
	if v := r.query[key]; len(v) > 0 {
		return v[0]
	}
	return ""
}

func (r request) ref() sdk.ObjectRef {
	return sdk.ObjectRef{APIVersion: r.info.groupVersion(), Kind: r.info.kind, Namespace: r.namespace, Name: r.name}
}

func (r request) dryRun() bool { return r.q("dryRun") == metav1.DryRunAll }

// manager is the field manager name: the fieldManager parameter, else the user
// agent's program name, as the server derives it.
func (r request) manager() string {
	if m := r.q("fieldManager"); m != "" {
		return m
	}
	ua := r.userAgent
	if i := strings.Index(ua, "/"); i > 0 {
		ua = ua[:i]
	}
	if ua == "" {
		return "unknown"
	}
	return ua
}

func statusError(err error) (int, []byte) {
	if se, ok := err.(apierrors.APIStatus); ok {
		st := se.Status()
		st.TypeMeta = metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}
		return int(st.Code), mustJSON(st)
	}
	st := apierrors.NewInternalError(err).Status()
	st.TypeMeta = metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}
	return int(st.Code), mustJSON(st)
}

// serveResource handles get, list, create, update, patch and delete of the
// served kinds and their status subresource. ok is false for paths it does not own.
func (tr Transport) serveResource(req *http.Request, path []string, reply func(int, []byte) *http.Response) (*http.Response, bool) {
	groupVersion, rest := "", []string(nil)
	switch {
	case len(path) >= 2 && path[0] == "api":
		groupVersion, rest = path[1], path[2:]
	case len(path) >= 3 && path[0] == "apis":
		groupVersion, rest = path[1]+"/"+path[2], path[3:]
	default:
		return nil, false
	}
	namespace := ""
	if len(rest) >= 2 && rest[0] == "namespaces" && len(rest) > 2 {
		namespace, rest = rest[1], rest[2:]
	}
	if len(rest) == 0 {
		return nil, false
	}
	info, ok := lookupKind(groupVersion, rest[0])
	if !ok {
		return nil, false
	}
	r := request{info: info, namespace: namespace, method: req.Method, query: req.URL.Query(), contentType: req.Header.Get("Content-Type"), userAgent: req.UserAgent(), accept: req.Header.Get("Accept")}
	if len(rest) > 1 {
		r.name = rest[1]
	}
	if len(rest) > 2 {
		r.subresource = rest[2]
	}
	if req.Body != nil {
		r.body = readAll(req.Body)
	}
	server := tr.World.Kube.server()
	if req.Method == http.MethodGet && r.name == "" && (r.q("watch") == "true" || r.q("watch") == "1") {
		return server.serveWatch(r, req), true
	}
	// Requests arrive from many goroutines (informers, the controller loop, the
	// agent); the store serves one at a time, as one etcd write order does.
	server.kube.mu.Lock()
	code, body := server.handle(r)
	server.kube.mu.Unlock()
	return reply(code, body), true
}

// server returns the Kube's API server logic, created on first use.
func (k *Kube) server() *apiServer {
	k.apiOnce.Do(func() { k.api = newAPIServer(k) })
	return k.api
}

func (s *apiServer) handle(r request) (code int, body []byte) {
	obj, status, err := s.dispatch(r)
	if err != nil {
		return statusError(err)
	}
	if status == 0 {
		status = http.StatusOK
	}
	return status, mustJSON(obj)
}

func (s *apiServer) dispatch(r request) (any, int, error) {
	if r.subresource == "scale" && r.info.scale {
		return s.scaleSubresource(r)
	}
	if r.subresource != "" && r.subresource != "status" {
		return nil, 0, apierrors.NewNotFound(r.info.groupResource(), r.name)
	}
	if r.subresource == "status" && !r.info.hasStatus {
		return nil, 0, apierrors.NewNotFound(r.info.groupResource(), r.name)
	}
	switch r.method {
	case http.MethodGet:
		if r.name == "" {
			return s.list(r)
		}
		live := s.kube.objects[r.ref()]
		if live == nil {
			return nil, 0, apierrors.NewNotFound(r.info.groupResource(), r.name)
		}
		if r.metadataOnly() {
			return partialMetadata(live.Object), 0, nil
		}
		return live.Object, 0, nil
	case http.MethodPost:
		return s.create(r)
	case http.MethodPut:
		return s.update(r)
	case http.MethodPatch:
		return s.patch(r)
	case http.MethodDelete:
		return s.delete(r)
	}
	return nil, 0, apierrors.NewMethodNotSupported(r.info.groupResource(), r.method)
}

func (s *apiServer) list(r request) (any, int, error) {
	selector, err := labels.Parse(r.q("labelSelector"))
	if err != nil {
		return nil, 0, apierrors.NewBadRequest(err.Error())
	}
	fieldSel, err := fields.ParseSelector(r.q("fieldSelector"))
	if err != nil {
		return nil, 0, apierrors.NewBadRequest(err.Error())
	}
	items := []any{}
	for _, o := range s.kube.objectsLocked() {
		if o.GetKind() != r.info.kind || o.GetAPIVersion() != r.info.groupVersion() || (r.namespace != "" && o.GetNamespace() != r.namespace) {
			continue
		}
		if !selector.Matches(labels.Set(o.GetLabels())) {
			continue
		}
		set := fields.Set{"metadata.name": o.GetName(), "metadata.namespace": o.GetNamespace()}
		if phase, ok, _ := unstructured.NestedString(o.Object, "status", "phase"); ok {
			set["status.phase"] = phase
		}
		if node, ok, _ := unstructured.NestedString(o.Object, "spec", "nodeName"); ok {
			set["spec.nodeName"] = node
		}
		if !fieldSel.Matches(set) {
			continue
		}
		if r.metadataOnly() {
			items = append(items, partialMetadata(o.Object))
			continue
		}
		item := o.Object
		// A list serves its items without type information.
		delete(item, "apiVersion")
		delete(item, "kind")
		items = append(items, item)
	}
	if r.metadataOnly() {
		return map[string]any{"apiVersion": "meta.k8s.io/v1", "kind": "PartialObjectMetadataList", "metadata": map[string]any{"resourceVersion": strconv.Itoa(s.kube.version)}, "items": items}, 0, nil
	}
	return map[string]any{"apiVersion": r.info.groupVersion(), "kind": r.info.kind + "List", "metadata": map[string]any{"resourceVersion": strconv.Itoa(s.kube.version)}, "items": items}, 0, nil
}

// metadataOnly: the client asked for PartialObjectMetadata (Accept ...;as=PartialObjectMetadata),
// which is how the controller reads Secrets without their payload.
func (r request) metadataOnly() bool { return strings.Contains(r.accept, "as=PartialObjectMetadata") }

func partialMetadata(object map[string]any) map[string]any {
	return map[string]any{"apiVersion": "meta.k8s.io/v1", "kind": "PartialObjectMetadata", "metadata": runtime.DeepCopyJSONValue(object["metadata"])}
}

func (s *apiServer) decode(r request) (*unstructured.Unstructured, error) {
	var content map[string]any
	if err := json.Unmarshal(r.body, &content); err != nil {
		return nil, apierrors.NewBadRequest("the body of the request was in an unknown format - accepted media types include: application/json, application/yaml")
	}
	obj := &unstructured.Unstructured{Object: content}
	if obj.GetAPIVersion() != r.info.groupVersion() || obj.GetKind() != r.info.kind {
		return nil, apierrors.NewBadRequest(fmt.Sprintf("the API version in the data (%s) does not match the expected API version (%s)", obj.GetAPIVersion(), r.info.groupVersion()))
	}
	return obj, nil
}

func (s *apiServer) nextUID() string {
	s.uids++
	return fmt.Sprintf("00000000-0000-4000-8000-%012d", s.uids)
}

// prepare is the decode path: typed canonical form, then defaults.
func (s *apiServer) prepare(r request, obj *unstructured.Unstructured) error {
	s.defaults(r.info, obj)
	return nil
}

func (s *apiServer) create(r request) (any, int, error) {
	if r.subresource != "" {
		return nil, 0, apierrors.NewMethodNotSupported(r.info.groupResource(), r.method)
	}
	obj, err := s.decode(r)
	if err != nil {
		return nil, 0, err
	}
	if obj.GetName() == "" && obj.GetGenerateName() != "" {
		obj.SetName(obj.GetGenerateName() + fmt.Sprintf("%05x", s.uids+1))
	}
	if obj.GetName() == "" {
		return nil, 0, apierrors.NewInvalid(r.info.gvk().GroupKind(), "", nil)
	}
	if r.info.namespaced {
		if obj.GetNamespace() != "" && obj.GetNamespace() != r.namespace {
			return nil, 0, apierrors.NewBadRequest("the namespace of the provided object does not match the namespace sent on the request")
		}
		obj.SetNamespace(r.namespace)
	}
	r.name = obj.GetName()
	if s.kube.objects[r.ref()] != nil {
		return nil, 0, apierrors.NewAlreadyExists(r.info.groupResource(), r.name)
	}
	created, err := s.build(r, obj)
	if err != nil {
		return nil, 0, err
	}
	if !r.dryRun() {
		s.commit(created, nil)
		return s.kube.objects[r.ref()].Object, http.StatusCreated, nil
	}
	return created.Object, http.StatusCreated, nil
}

// build turns a created or applied object into what the registry stores.
func (s *apiServer) build(r request, obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	if err := s.prepare(r, obj); err != nil {
		return nil, err
	}
	obj.SetUID(types.UID(s.nextUID()))
	obj.SetCreationTimestamp(metav1.NewTime(s.now().Truncate(time.Second)))
	obj.SetResourceVersion("")
	obj.SetManagedFields(nil)
	if r.info.generation {
		obj.SetGeneration(1)
	}
	managed, err := s.fieldManager(r).Update(emptyObject(r.info), obj, r.manager())
	if err != nil {
		return nil, apierrors.NewBadRequest(err.Error())
	}
	// The registry's create strategy runs after ownership is recorded, so the
	// addresses and identity-derived fields it fills in belong to no manager.
	created := managed.(*unstructured.Unstructured)
	s.initialize(r.info, created)
	return created, nil
}

func (s *apiServer) update(r request) (any, int, error) {
	live := s.kube.objects[r.ref()]
	if live == nil {
		return nil, 0, apierrors.NewNotFound(r.info.groupResource(), r.name)
	}
	obj, err := s.decode(r)
	if err != nil {
		return nil, 0, err
	}
	return s.store(r, live, obj, false)
}

func (s *apiServer) patch(r request) (any, int, error) {
	live := s.kube.objects[r.ref()]
	patchType := types.PatchType(strings.Split(r.contentType, ";")[0])
	if patchType == types.ApplyPatchType {
		return s.apply(r, live)
	}
	if live == nil {
		return nil, 0, apierrors.NewNotFound(r.info.groupResource(), r.name)
	}
	before, _ := json.Marshal(live.Object)
	var merged []byte
	var err error
	switch patchType {
	case types.MergePatchType:
		merged, err = jsonpatch.MergePatch(before, r.body)
	case types.JSONPatchType:
		var p jsonpatch.Patch
		if p, err = jsonpatch.DecodePatch(r.body); err == nil {
			if merged, err = p.Apply(before); err != nil {
				return nil, 0, apierrors.NewGenericServerResponse(http.StatusUnprocessableEntity, "patch", r.info.groupResource(), r.name, err.Error(), 0, false)
			}
		}
	case types.StrategicMergePatchType:
		var typed runtime.Object
		if typed, err = scheme.Scheme.New(r.info.gvk()); err == nil {
			merged, err = strategicpatch.StrategicMergePatch(before, r.body, typed)
		}
	default:
		return nil, 0, apierrors.NewGenericServerResponse(http.StatusUnsupportedMediaType, "patch", r.info.groupResource(), r.name, fmt.Sprintf("the body of the request was in an unknown format - unsupported patch type %q", patchType), 0, false)
	}
	if err != nil {
		return nil, 0, apierrors.NewBadRequest(err.Error())
	}
	obj := &unstructured.Unstructured{}
	if err := json.Unmarshal(merged, &obj.Object); err != nil {
		return nil, 0, apierrors.NewBadRequest(err.Error())
	}
	return s.store(r, live, obj, false)
}

// apply is server-side apply: merge into the live object with ownership, or
// create when absent.
func (s *apiServer) apply(r request, live *unstructured.Unstructured) (any, int, error) {
	if r.q("fieldManager") == "" {
		return nil, 0, apierrors.NewInvalid(schema.GroupKind{Group: "meta.k8s.io", Kind: "PatchOptions"}, "", nil)
	}
	var content map[string]any
	if err := yaml.Unmarshal(r.body, &content); err != nil {
		return nil, 0, apierrors.NewBadRequest(err.Error())
	}
	applied := &unstructured.Unstructured{Object: content}
	if applied.GetAPIVersion() != r.info.groupVersion() || applied.GetKind() != r.info.kind {
		return nil, 0, apierrors.NewBadRequest(fmt.Sprintf("the API version in the data (%s) does not match the expected API version (%s)", applied.GetAPIVersion(), r.info.groupVersion()))
	}
	if applied.GetName() != r.name {
		return nil, 0, apierrors.NewBadRequest("the name of the object (" + applied.GetName() + ") does not match the name on the URL (" + r.name + ")")
	}
	if r.info.namespaced {
		applied.SetNamespace(r.namespace)
	}
	force := r.q("force") == "true"
	if r.info.hasStatus && r.subresource == "" {
		delete(applied.Object, "status")
	}
	if live == nil {
		// Apply to an absent object creates it, owned by the applier.
		managed, err := s.fieldManager(r).Apply(emptyObject(r.info), applied, r.manager(), force)
		if err != nil {
			return nil, 0, err
		}
		created := managed.(*unstructured.Unstructured)
		s.defaults(r.info, created)
		created.SetUID(types.UID(s.nextUID()))
		created.SetCreationTimestamp(metav1.NewTime(s.now().Truncate(time.Second)))
		if r.info.generation {
			created.SetGeneration(1)
		}
		s.initialize(r.info, created)
		if !r.dryRun() {
			s.commit(created, nil)
			return s.kube.objects[r.ref()].Object, http.StatusCreated, nil
		}
		return created.Object, http.StatusCreated, nil
	}
	managed, err := s.fieldManager(r).Apply(live.DeepCopy(), applied, r.manager(), force)
	if err != nil {
		return nil, 0, err
	}
	return s.finish(r, live, managed.(*unstructured.Unstructured))
}

// store finishes a non-apply update: defaults, ownership, status split, generation.
func (s *apiServer) store(r request, live, obj *unstructured.Unstructured, _ bool) (any, int, error) {
	if rv := obj.GetResourceVersion(); rv != "" && rv != live.GetResourceVersion() {
		return nil, 0, apierrors.NewConflict(r.info.groupResource(), r.name, fmt.Errorf("the object has been modified; please apply your changes to the latest version and try again"))
	}
	if err := uidImmutable(r, live, obj); err != nil {
		return nil, 0, err
	}
	// The registry owns identity.
	obj.SetUID(live.GetUID())
	obj.SetCreationTimestamp(live.GetCreationTimestamp())
	obj.SetName(live.GetName())
	obj.SetNamespace(live.GetNamespace())
	if err := s.prepare(r, obj); err != nil {
		return nil, 0, err
	}
	scopeToEndpoint(r, live, obj)
	if live.GetDeletionTimestamp() != nil {
		if added := newFinalizers(live.GetFinalizers(), obj.GetFinalizers()); len(added) > 0 {
			return nil, 0, apierrors.NewInvalid(r.info.gvk().GroupKind(), r.name, field.ErrorList{field.Forbidden(field.NewPath("metadata", "finalizers"), "no new finalizers can be added if the object is being deleted")})
		}
	}
	managed, err := s.fieldManager(r).Update(live.DeepCopy(), obj, r.manager())
	if err != nil {
		return nil, 0, apierrors.NewBadRequest(err.Error())
	}
	return s.finish(r, live, managed.(*unstructured.Unstructured))
}

// finish applies the status split and generation rule, then commits unless the
// request is a dry run or changes nothing.
func (s *apiServer) finish(r request, live, next *unstructured.Unstructured) (any, int, error) {
	s.defaults(r.info, next)
	if r.info.generation {
		next.SetGeneration(live.GetGeneration())
		// The Deployment strategy also counts annotation changes as spec changes.
		if specChanged(live, next) || (r.info.kind == "Deployment" && !reflect.DeepEqual(live.GetAnnotations(), next.GetAnnotations())) {
			next.SetGeneration(live.GetGeneration() + 1)
		}
	}
	if deletable(next) {
		return s.remove(r, live, next)
	}
	if sameStored(live, next) {
		return live.Object, 0, nil
	}
	if r.dryRun() {
		next.SetResourceVersion(live.GetResourceVersion())
		return next.Object, 0, nil
	}
	s.commit(next, live)
	return s.kube.objects[r.ref()].Object, 0, nil
}

// deletable: an object that is being deleted and has no finalizers left goes away.
func deletable(o *unstructured.Unstructured) bool {
	return o.GetDeletionTimestamp() != nil && len(o.GetFinalizers()) == 0
}

// remove deletes an object whose last finalizer went away. The DELETED event
// carries the last stored state (finalizers included), as the store's delete does.
func (s *apiServer) remove(r request, stored, o *unstructured.Unstructured) (any, int, error) {
	if !r.dryRun() {
		s.commitDelete(stored)
	}
	return o.Object, 0, nil
}

// commit stores an object under a new resourceVersion, running the cache hooks
// the same way every write through the Kube does.
func (s *apiServer) commit(o, previous *unstructured.Unstructured) {
	for _, hook := range s.kube.beforeWrite {
		hook()
	}
	s.kube.version++
	o.SetResourceVersion(strconv.Itoa(s.kube.version))
	s.kube.objects[sdk.ObjectRef{APIVersion: o.GetAPIVersion(), Kind: o.GetKind(), Namespace: o.GetNamespace(), Name: o.GetName()}] = o
	kind := "MODIFIED"
	if previous == nil {
		kind = "ADDED"
	}
	s.log.publish(kind, o, s.kube.version)
}

func (s *apiServer) commitDelete(o *unstructured.Unstructured) {
	for _, hook := range s.kube.beforeWrite {
		hook()
	}
	s.kube.version++
	delete(s.kube.objects, sdk.ObjectRef{APIVersion: o.GetAPIVersion(), Kind: o.GetKind(), Namespace: o.GetNamespace(), Name: o.GetName()})
	final := o.DeepCopy()
	final.SetResourceVersion(strconv.Itoa(s.kube.version))
	s.log.publish("DELETED", final, s.kube.version)
}

func specChanged(a, b *unstructured.Unstructured) bool {
	x, _ := json.Marshal(a.Object["spec"])
	y, _ := json.Marshal(b.Object["spec"])
	return string(x) != string(y)
}

// sameStored compares what would be written, ignoring the version stamp.
func sameStored(a, b *unstructured.Unstructured) bool {
	x, y := a.DeepCopy(), b.DeepCopy()
	x.SetResourceVersion("")
	y.SetResourceVersion("")
	left, _ := json.Marshal(x.Object)
	right, _ := json.Marshal(y.Object)
	return string(left) == string(right)
}

func (s *apiServer) delete(r request) (any, int, error) {
	live := s.kube.objects[r.ref()]
	if live == nil {
		return nil, 0, apierrors.NewNotFound(r.info.groupResource(), r.name)
	}
	var options metav1.DeleteOptions
	if len(r.body) > 0 {
		if err := json.Unmarshal(r.body, &options); err != nil {
			return nil, 0, apierrors.NewBadRequest(err.Error())
		}
	}
	if pre := options.Preconditions; pre != nil {
		if pre.UID != nil && *pre.UID != live.GetUID() {
			return nil, 0, apierrors.NewConflict(r.info.groupResource(), r.name, fmt.Errorf("Precondition failed: UID in precondition: %s, UID in object meta: %s", *pre.UID, live.GetUID()))
		}
		if pre.ResourceVersion != nil && *pre.ResourceVersion != live.GetResourceVersion() {
			return nil, 0, apierrors.NewConflict(r.info.groupResource(), r.name, fmt.Errorf("Precondition failed: ResourceVersion in precondition: %s, ResourceVersion in object meta: %s", *pre.ResourceVersion, live.GetResourceVersion()))
		}
	}
	if r.info.kind == "Pod" && len(live.GetFinalizers()) == 0 {
		// Pods report the deleted object, marked as terminating; one that no node
		// runs is removed at once.
		gone := live.DeepCopy()
		gone.SetDeletionTimestamp(&metav1.Time{Time: s.now().Truncate(time.Second)})
		gone.SetDeletionGracePeriodSeconds(new(int64))
		gone.SetGeneration(live.GetGeneration() + 1)
		if node, _, _ := unstructured.NestedString(live.Object, "spec", "nodeName"); node == "" {
			if !r.dryRun() {
				s.commitDelete(live)
				gone.SetResourceVersion(strconv.Itoa(s.kube.version))
			}
			return gone.Object, 0, nil
		}
		if !r.dryRun() {
			s.commit(gone, live)
			return s.kube.objects[r.ref()].Object, 0, nil
		}
		return gone.Object, 0, nil
	}
	if len(live.GetFinalizers()) > 0 {
		if live.GetDeletionTimestamp() != nil {
			return live.Object, 0, nil
		}
		next := live.DeepCopy()
		next.SetDeletionTimestamp(&metav1.Time{Time: s.now().Truncate(time.Second)})
		next.SetDeletionGracePeriodSeconds(new(int64))
		if r.info.generation {
			next.SetGeneration(live.GetGeneration() + 1)
		}
		if !r.dryRun() {
			s.commit(next, live)
			return s.kube.objects[r.ref()].Object, 0, nil
		}
		return next.Object, 0, nil
	}
	if !r.dryRun() {
		s.commitDelete(live)
	}
	return metav1.Status{TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}, Status: metav1.StatusSuccess,
		Details: &metav1.StatusDetails{Name: r.name, Group: r.info.group, Kind: r.info.plural, UID: live.GetUID()}}, 0, nil
}

// discovery serves /api, /apis and the per-version resource lists.
func serveDiscovery(path []string) ([]byte, bool) {
	switch {
	case len(path) == 1 && path[0] == "api":
		return mustJSON(metav1.APIVersions{TypeMeta: metav1.TypeMeta{Kind: "APIVersions"}, Versions: []string{"v1"}}), true
	case len(path) == 1 && path[0] == "apis":
		groups := map[string]*metav1.APIGroup{}
		var order []string
		for _, k := range servedKinds {
			if k.group == "" {
				continue
			}
			g := groups[k.group]
			if g == nil {
				g = &metav1.APIGroup{Name: k.group}
				groups[k.group] = g
				order = append(order, k.group)
			}
			fv := metav1.GroupVersionForDiscovery{GroupVersion: k.groupVersion(), Version: k.version}
			seen := false
			for _, v := range g.Versions {
				seen = seen || v == fv
			}
			if !seen {
				g.Versions = append(g.Versions, fv)
				g.PreferredVersion = g.Versions[0]
			}
		}
		sort.SliceStable(order, func(i, j int) bool { return groupRank(order[i]) < groupRank(order[j]) })
		list := metav1.APIGroupList{TypeMeta: metav1.TypeMeta{Kind: "APIGroupList", APIVersion: "v1"}}
		for _, name := range order {
			list.Groups = append(list.Groups, *groups[name])
		}
		return mustJSON(list), true
	case len(path) == 2 && path[0] == "api", len(path) == 3 && path[0] == "apis":
		gv := path[1]
		if path[0] == "apis" {
			gv = path[1] + "/" + path[2]
		}
		list := metav1.APIResourceList{TypeMeta: metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"}, GroupVersion: gv}
		if path[0] == "api" {
			// The core group's list carries no apiVersion.
			list.APIVersion = ""
		}
		for _, k := range servedKinds {
			if k.groupVersion() != gv {
				continue
			}
			res := metav1.APIResource{Name: k.plural, SingularName: strings.ToLower(k.kind), Namespaced: k.namespaced, Kind: k.kind, Verbs: allVerbs, ShortNames: k.shortNames, Categories: k.categories}
			list.APIResources = append(list.APIResources, res)
			if k.scale {
				list.APIResources = append(list.APIResources, metav1.APIResource{Name: k.plural + "/scale", Namespaced: k.namespaced, Group: "autoscaling", Version: "v1", Kind: "Scale", Verbs: []string{"get", "patch", "update"}})
			}
			if k.hasStatus {
				list.APIResources = append(list.APIResources, metav1.APIResource{Name: k.plural + "/status", Namespaced: k.namespaced, Kind: k.kind, Verbs: []string{"get", "patch", "update"}})
			}
		}
		if len(list.APIResources) == 0 {
			return nil, false
		}
		return mustJSON(list), true
	}
	return nil, false
}

// emptyObject is the live object a create starts from: the server's Creater makes
// a zero typed object, whose always-present empty structs (spec, status, template)
// already exist, so ownership of a created object excludes them.
func emptyObject(info kindInfo) *unstructured.Unstructured {
	empty := &unstructured.Unstructured{Object: map[string]any{}}
	if typed, err := scheme.Scheme.New(info.gvk()); err == nil {
		if content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(typed); err == nil {
			empty.Object = content
		}
	}
	empty.SetGroupVersionKind(info.gvk())
	return empty
}

// identityConvertor: the simulation serves one version of each kind, so there is
// nothing to convert between.
type identityConvertor struct{}

func (identityConvertor) Convert(in, out, context any) error { return nil }
func (identityConvertor) ConvertToVersion(in runtime.Object, _ runtime.GroupVersioner) (runtime.Object, error) {
	return in, nil
}
func (identityConvertor) ConvertFieldLabel(_ schema.GroupVersionKind, _, _ string) (string, string, error) {
	return "", "", fmt.Errorf("not implemented")
}

// creater makes the zero object a create starts from.
type creater struct{}

func (creater) New(gvk schema.GroupVersionKind) (runtime.Object, error) {
	for _, k := range servedKinds {
		if k.gvk() == gvk {
			return emptyObject(k), nil
		}
	}
	return &unstructured.Unstructured{Object: map[string]any{"apiVersion": gvk.GroupVersion().String(), "kind": gvk.Kind}}, nil
}

// defaulter applies the same defaulting functions as writes do.
type defaulter struct{}

func (defaulter) Default(in runtime.Object) {
	if u, ok := in.(*unstructured.Unstructured); ok {
		(&apiServer{}).defaults(kindOf(u), u)
	}
}

func newFinalizers(old, next []string) []string {
	have := map[string]bool{}
	for _, f := range old {
		have[f] = true
	}
	var added []string
	for _, f := range next {
		if !have[f] {
			added = append(added, f)
		}
	}
	return added
}

// groupRank is the order the server lists the groups it serves in.
func groupRank(group string) int {
	for i, g := range []string{"apps", "autoscaling", "batch", "networking.k8s.io", "policy", "discovery.k8s.io"} {
		if g == group {
			return i
		}
	}
	return 100
}

// uidImmutable: a write that names another uid changes an immutable field, which
// the server rejects as invalid (only delete preconditions report a conflict).
func uidImmutable(r request, live, obj *unstructured.Unstructured) error {
	if uid := obj.GetUID(); uid != "" && uid != live.GetUID() {
		return apierrors.NewInvalid(r.info.gvk().GroupKind(), r.name, field.ErrorList{field.Invalid(field.NewPath("metadata", "uid"), uid, "field is immutable")})
	}
	return nil
}

// scaleSubresource serves autoscaling/v1 Scale over the replicas of a workload.
// A write sets spec.replicas and moves ownership of that field to the writer
// under a "scale" subresource entry, as the server's scale handler does.
func (s *apiServer) scaleSubresource(r request) (any, int, error) {
	live := s.kube.objects[r.ref()]
	if live == nil {
		return nil, 0, apierrors.NewNotFound(r.info.groupResource(), r.name)
	}
	toScale := func(o *unstructured.Unstructured) map[string]any {
		replicas, _, _ := unstructured.NestedInt64(o.Object, "spec", "replicas")
		observed, _, _ := unstructured.NestedInt64(o.Object, "status", "replicas")
		selector := ""
		if match, _, _ := unstructured.NestedStringMap(o.Object, "spec", "selector", "matchLabels"); len(match) > 0 {
			selector = labels.SelectorFromSet(match).String()
		}
		meta := map[string]any{"name": o.GetName(), "namespace": o.GetNamespace(), "uid": string(o.GetUID()), "resourceVersion": o.GetResourceVersion(), "creationTimestamp": o.Object["metadata"].(map[string]any)["creationTimestamp"]}
		return map[string]any{"apiVersion": "autoscaling/v1", "kind": "Scale", "metadata": meta, "spec": map[string]any{"replicas": replicas}, "status": map[string]any{"replicas": observed, "selector": selector}}
	}
	if r.method == http.MethodGet {
		return toScale(live), 0, nil
	}
	if r.method != http.MethodPatch && r.method != http.MethodPut {
		return nil, 0, apierrors.NewMethodNotSupported(r.info.groupResource(), r.method)
	}
	before, _ := json.Marshal(toScale(live))
	var merged []byte
	var err error
	if r.method == http.MethodPut {
		merged = r.body
	} else {
		switch types.PatchType(strings.Split(r.contentType, ";")[0]) {
		case types.MergePatchType:
			merged, err = jsonpatch.MergePatch(before, r.body)
		case types.StrategicMergePatchType:
			merged, err = jsonpatch.MergePatch(before, r.body)
		default:
			return nil, 0, apierrors.NewGenericServerResponse(http.StatusUnsupportedMediaType, "patch", r.info.groupResource(), r.name, "unsupported patch type for scale", 0, false)
		}
	}
	var scale struct {
		Spec struct {
			Replicas int64 `json:"replicas"`
		} `json:"spec"`
	}
	if err == nil {
		err = json.Unmarshal(merged, &scale)
	}
	if err != nil {
		return nil, 0, apierrors.NewBadRequest(err.Error())
	}
	next := live.DeepCopy()
	_ = unstructured.SetNestedField(next.Object, scale.Spec.Replicas, "spec", "replicas")
	if live.Object["spec"].(map[string]any)["replicas"] == next.Object["spec"].(map[string]any)["replicas"] {
		return toScale(live), 0, nil
	}
	next.SetGeneration(live.GetGeneration() + 1)
	next.SetManagedFields(transferReplicas(live.GetManagedFields(), r.manager(), r.info.groupVersion()))
	if !r.dryRun() {
		s.commit(next, live)
		return toScale(s.kube.objects[r.ref()]), 0, nil
	}
	return toScale(next), 0, nil
}

// transferReplicas gives spec.replicas to manager through the scale subresource.
func transferReplicas(entries []metav1.ManagedFieldsEntry, manager, apiVersion string) []metav1.ManagedFieldsEntry {
	var out []metav1.ManagedFieldsEntry
	for _, e := range entries {
		if e.FieldsV1 == nil || (e.Subresource == "scale" && e.Manager == manager) {
			continue
		}
		var fields map[string]any
		if json.Unmarshal(e.FieldsV1.Raw, &fields) == nil {
			if spec, ok := fields["f:spec"].(map[string]any); ok {
				delete(spec, "f:replicas")
				if len(spec) == 0 || (len(spec) == 1 && spec["."] != nil) {
					delete(fields, "f:spec")
				}
			}
			if raw, err := json.Marshal(fields); err == nil {
				e.FieldsV1 = &metav1.FieldsV1{Raw: raw}
			}
		}
		out = append(out, e)
	}
	entry := metav1.ManagedFieldsEntry{Manager: manager, Operation: metav1.ManagedFieldsOperationUpdate, APIVersion: apiVersion, FieldsType: "FieldsV1", Subresource: "scale",
		FieldsV1: &metav1.FieldsV1{Raw: []byte(`{"f:spec":{"f:replicas":{}}}`)}}
	return append([]metav1.ManagedFieldsEntry{entry}, out...)
}
// ci-c4 measurement 1

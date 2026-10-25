package runtime

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	rbacinformers "k8s.io/client-go/informers/rbac/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	controllercore "sdo.dev/operator/core"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/traffic"
)

type KubernetesCacheConfig struct {
	GenericCapture     *controllercore.GenericCapture
	Now                func() time.Time
	AcceptedScope      func(context.Context) (sdk.ObservationScope, string, error)
	Scope, Ceiling     sdk.ObservationScope
	Generation         string
	Namespace          string
	Client             kubernetes.Interface
	StateConfigMapName string
	TrackDrift         bool
}

type ObjectChange struct {
	Old      *unstructured.Unstructured
	New      *unstructured.Unstructured
	Initial  bool
	Adoption bool
}

// KubernetesCache coalesces informer notifications into a set of pending watch
// kinds. A watch kind carries no object identity, so N queued events of one
// kind trigger exactly the detectors that a single event would. Coalescing
// keeps every kind that changed since the last TakeEvents call deliverable
// regardless of how slowly the controller loop drains them: a bounded FIFO
// would either drop kinds on overflow or delay a ConfigMap delete behind a
// backlog of unrelated Pod events.
type KubernetesCache struct {
	scopedRetries    map[string]scopedRetry
	scopedLastStatus map[string]sdk.ObservationStatus
	scopedMu         sync.Mutex
	scopedPrevious   map[string]map[string]*unstructured.Unstructured
	scopedStatuses   map[string]string
	scopedTracker    *StateTracker
	podAdmission     bool
	config           KubernetesCacheConfig
	declared         map[string]sdk.WatchKind

	// informerMu guards the live informer generation. Stop and Resync replace
	// it so a redeployed application namespace is observed through a fresh
	// list rather than a store that missed events while access was revoked.
	informerMu sync.RWMutex
	factory    informers.SharedInformerFactory
	informers  map[string]cache.SharedIndexInformer
	parent     context.Context
	cancel     context.CancelFunc

	mu          sync.Mutex
	pending     map[string]sdk.WatchKind
	order       []string
	changes     map[string]ObjectChange
	changeOrder []string
	notify      chan struct{}
}

func NewKubernetesCache(config KubernetesCacheConfig, detectors []sdk.Detector) (*KubernetesCache, error) {
	if err := config.Scope.Within(config.Ceiling); err != nil {
		return nil, err
	}
	if config.Namespace == "" {
		return nil, fmt.Errorf("namespace is required")
	}
	if config.Client == nil {
		return nil, fmt.Errorf("Kubernetes client is required")
	}
	result := &KubernetesCache{
		podAdmission: sdk.ConsumesPodAdmission(detectors), config: config, pending: make(map[string]sdk.WatchKind), changes: make(map[string]ObjectChange), notify: make(chan struct{}, 1),
	}
	declared := make(map[string]sdk.WatchKind)
	for _, detector := range detectors {
		for _, watch := range detector.Spec().Watches {
			if !config.Scope.AllowsWatch(watch, config.Namespace) {
				return nil, fmt.Errorf("watch namespace %q does not match cache namespace %q", watch.Namespace, config.Namespace)
			}
			if watch.APIVersion == traffic.Watch.APIVersion && watch.Kind == traffic.Watch.Kind ||
				watch.APIVersion == servicehealth.ReachabilityWatch.APIVersion && watch.Kind == servicehealth.ReachabilityWatch.Kind {
				// Synthetic-traffic and reachability results arrive through Notify, not an informer.
				continue
			}
			if watch.Namespace != "" && watch.Namespace != config.Namespace || (sdk.ObservationTarget{Kind: watch.Kind}).Cluster() {
				continue
			}
			if watch.APIVersion == metrics.Watch.APIVersion && watch.Kind == metrics.Watch.Kind {
				// Metrics detectors run on their interval; no informer backs the watch.
				continue
			}
			if !inventoryClusterKind(watch.Kind) {
				watch.Namespace = config.Namespace
			}
			declared[watch.APIVersion+"/"+watch.Kind] = watch
		}
	}
	if config.TrackDrift {
		for _, watch := range []sdk.WatchKind{
			{APIVersion: "v1", Kind: "ConfigMap"}, {APIVersion: "v1", Kind: "Service"},
			{APIVersion: "v1", Kind: "Pod"}, {APIVersion: "apps/v1", Kind: "Deployment"},
			{APIVersion: "apps/v1", Kind: "ReplicaSet"}, {APIVersion: "v1", Kind: "Endpoints"},
			{APIVersion: "discovery.k8s.io/v1", Kind: "EndpointSlice"},
			{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy"},
			{APIVersion: "apps/v1", Kind: "StatefulSet"}, {APIVersion: "apps/v1", Kind: "DaemonSet"},
			{APIVersion: "v1", Kind: "ServiceAccount"}, {APIVersion: "v1", Kind: "PersistentVolumeClaim"},
			{APIVersion: "batch/v1", Kind: "Job"}, {APIVersion: "batch/v1", Kind: "CronJob"},
			{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"}, {APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
			{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"}, {APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
		} {
			if !inventoryClusterKind(watch.Kind) {
				watch.Namespace = config.Namespace
			}
			declared[watch.APIVersion+"/"+watch.Kind] = watch
		}
	}
	result.declared = declared
	factory, built, err := result.build()
	if err != nil {
		return nil, err
	}
	result.factory, result.informers = factory, built
	return result, nil
}

func (c *KubernetesCache) build() (informers.SharedInformerFactory, map[string]cache.SharedIndexInformer, error) {
	factory := informers.NewSharedInformerFactoryWithOptions(c.config.Client, 0, informers.WithNamespace(c.config.Namespace))
	// RoleBindings in other namespaces may reference an application-relevant
	// ClusterRole. Observe them for ownership checks, without admitting foreign
	// objects to the application inventory or detector snapshots.
	if c.config.TrackDrift {
		factory.InformerFor(&rbacv1.RoleBinding{}, func(client kubernetes.Interface, resync time.Duration) cache.SharedIndexInformer {
			return rbacinformers.NewRoleBindingInformer(client, metav1.NamespaceAll, resync, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
		})
	}
	built := make(map[string]cache.SharedIndexInformer, len(c.declared))
	for key, watch := range c.declared {
		informer, err := informerFor(factory, key)
		if err != nil {
			return nil, nil, err
		}
		built[key] = informer
		if _, err := informer.AddEventHandler(c.eventHandler(watch)); err != nil {
			return nil, nil, fmt.Errorf("register %s informer handler: %w", key, err)
		}
	}
	return factory, built, nil
}

// Start begins observing the namespace. Calling it on a running cache is a no-op.
func (c *KubernetesCache) Start(ctx context.Context) {
	c.informerMu.Lock()
	defer c.informerMu.Unlock()
	if c.cancel != nil {
		return
	}
	c.parent = ctx
	running, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.factory.Start(running.Done())
}

// Stop ends observation, for example while the application is intentionally
// absent, without discarding pending watch events.
func (c *KubernetesCache) Stop() {
	c.informerMu.Lock()
	defer c.informerMu.Unlock()
	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}
}

// Resync replaces the informers with a fresh generation, starts it, and waits
// for its initial list. Stale objects from before a Stop are never served.
func (c *KubernetesCache) Resync(ctx context.Context) error {
	factory, built, err := c.build()
	if err != nil {
		return err
	}
	c.informerMu.Lock()
	if c.cancel != nil {
		c.cancel()
	}
	parent := c.parent
	if parent == nil {
		parent = ctx
	}
	running, cancel := context.WithCancel(parent)
	c.parent, c.cancel = parent, cancel
	c.factory, c.informers = factory, built
	factory.Start(running.Done())
	c.informerMu.Unlock()
	return c.WaitForSync(ctx)
}

func (c *KubernetesCache) WaitForSync(ctx context.Context) error {
	c.informerMu.RLock()
	syncs := make([]cache.InformerSynced, 0, len(c.informers))
	for _, informer := range c.informers {
		syncs = append(syncs, informer.HasSynced)
	}
	c.informerMu.RUnlock()
	if len(syncs) == 0 {
		return nil
	}
	if !cache.WaitForCacheSync(ctx.Done(), syncs...) {
		if err := ctx.Err(); err != nil {
			return err
		}
		return fmt.Errorf("Kubernetes informer cache failed to sync")
	}
	return nil
}

// Notifications signals that at least one watched kind changed since the last
// TakeEvents call. Multiple changes share one pending signal.
func (c *KubernetesCache) Notifications() <-chan struct{} {
	return c.notify
}

// TakeEvents returns and clears every watched kind that changed since the
// previous call, in first-observed order.
func (c *KubernetesCache) TakeEvents() []sdk.WatchKind {
	c.mu.Lock()
	defer c.mu.Unlock()
	events := make([]sdk.WatchKind, 0, len(c.order))
	for _, key := range c.order {
		events = append(events, c.pending[key])
	}
	c.pending = make(map[string]sdk.WatchKind)
	c.order = nil
	return events
}

func (c *KubernetesCache) TakeObjectChanges() []ObjectChange {
	c.mu.Lock()
	defer c.mu.Unlock()
	changes := make([]ObjectChange, 0, len(c.changeOrder))
	for _, key := range c.changeOrder {
		changes = append(changes, c.changes[key])
	}
	c.changes = make(map[string]ObjectChange)
	c.changeOrder = nil
	return changes
}

// Notify queues a watch event that no informer produces, such as a synthetic
// traffic result, for the observed namespace.
func (c *KubernetesCache) Notify(watch sdk.WatchKind) {
	watch.Namespace = c.config.Namespace
	c.enqueue(watch)
}

func (c *KubernetesCache) enqueue(watch sdk.WatchKind) {
	key := watch.APIVersion + "/" + watch.Kind + "/" + watch.Namespace
	c.mu.Lock()
	if _, exists := c.pending[key]; !exists {
		c.pending[key] = watch
		c.order = append(c.order, key)
	}
	c.mu.Unlock()
	select {
	case c.notify <- struct{}{}:
	default:
	}
}

func (c *KubernetesCache) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	c.scopedMu.Lock()
	defer c.scopedMu.Unlock()
	if c.config.AcceptedScope != nil {
		scope, revision, err := c.config.AcceptedScope(ctx)
		if err != nil {
			return nil, err
		}
		if err = scope.Within(c.config.Ceiling); err != nil {
			return nil, err
		}
		c.reconcileScope(scope, revision)
	}
	scoped, err := c.readScoped(ctx)
	if err != nil {
		return nil, err
	}
	c.observeScoped(scoped)
	snapshot := controllercore.DetectionSnapshot{GenericCapture: c.config.GenericCapture, NamespaceName: c.config.Namespace, ObservedAtTime: time.Now().UTC(), ScopedSnapshot: scoped}
	statefulSets, err := c.config.Client.AppsV1().StatefulSets(c.config.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list statefulsets: %w", err)
	}
	hpas, err := c.config.Client.AutoscalingV2().HorizontalPodAutoscalers(c.config.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list horizontalpodautoscalers: %w", err)
	}
	pdbs, err := c.config.Client.PolicyV1().PodDisruptionBudgets(c.config.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list poddisruptionbudgets: %w", err)
	}
	snapshot.StatefulSetList = append([]appsv1.StatefulSet(nil), statefulSets.Items...)
	snapshot.HPAList = append([]autoscalingv2.HorizontalPodAutoscaler(nil), hpas.Items...)
	snapshot.PDBList = append([]policyv1.PodDisruptionBudget(nil), pdbs.Items...)
	c.informerMu.RLock()
	for key, informer := range c.informers {
		for _, object := range informer.GetStore().List() {
			switch key {
			case "v1/ConfigMap":
				item := object.(*corev1.ConfigMap)
				if item.Name != c.config.StateConfigMapName {
					snapshot.ConfigMapList = append(snapshot.ConfigMapList, *item.DeepCopy())
				}
			case "v1/Service":
				snapshot.ServiceList = append(snapshot.ServiceList, *object.(*corev1.Service).DeepCopy())
			case "v1/Pod":
				snapshot.PodList = append(snapshot.PodList, *object.(*corev1.Pod).DeepCopy())
			case "apps/v1/Deployment":
				snapshot.DeploymentList = append(snapshot.DeploymentList, *object.(*appsv1.Deployment).DeepCopy())
			case "apps/v1/ReplicaSet":
				snapshot.ReplicaSetList = append(snapshot.ReplicaSetList, *object.(*appsv1.ReplicaSet).DeepCopy())
			case "v1/Endpoints":
				snapshot.EndpointList = append(snapshot.EndpointList, *object.(*corev1.Endpoints).DeepCopy())
			case "discovery.k8s.io/v1/EndpointSlice":
				snapshot.EndpointSliceList = append(snapshot.EndpointSliceList, *object.(*discoveryv1.EndpointSlice).DeepCopy())
			case "networking.k8s.io/v1/NetworkPolicy":
				snapshot.NetworkPolicyList = append(snapshot.NetworkPolicyList, *object.(*networkingv1.NetworkPolicy).DeepCopy())
			case "v1/Event":
				snapshot.EventList = append(snapshot.EventList, *object.(*corev1.Event).DeepCopy())
			}
		}
	}
	c.informerMu.RUnlock()
	if c.podAdmission {
		r := controllercore.ObservePodAdmission(ctx, c.config.Client, c.config.Namespace, snapshot.PodList)
		snapshot.Admission = &r
	}
	return snapshot, nil
}

func (c *KubernetesCache) eventHandler(watch sdk.WatchKind) cache.ResourceEventHandler {
	emit := func(previous, object any, initial bool) {
		identity := object
		if identity == nil {
			identity = previous
		}
		if watch.Kind == "RoleBinding" {
			binding := driftObject(identity, watch)
			if binding != nil && binding.GetNamespace() != c.config.Namespace {
				c.enqueue(watch)
				return
			}
		}
		if watch.Kind == "ConfigMap" && objectName(identity) == c.config.StateConfigMapName {
			return
		}
		if c.config.TrackDrift {
			old := driftObject(previous, watch)
			newObject := driftObject(object, watch)
			identity := old
			if newObject != nil {
				identity = newObject
			}
			if identity != nil {
				key := watch.APIVersion + "/" + watch.Kind + "/" + identity.GetNamespace() + "/" + identity.GetName()
				c.mu.Lock()
				if existing, ok := c.changes[key]; ok {
					if existing.Old != nil {
						old = existing.Old
					} else if old != nil {
						old = existing.New
					}
				} else {
					c.changeOrder = append(c.changeOrder, key)
				}
				c.changes[key] = ObjectChange{Old: old, New: newObject, Initial: initial}
				c.mu.Unlock()
			}
		}
		c.enqueue(watch)
	}
	return cache.ResourceEventHandlerDetailedFuncs{
		AddFunc: func(object any, initial bool) { emit(nil, object, initial) },
		UpdateFunc: func(previous, current any) {
			emit(previous, current, false)
		},
		DeleteFunc: func(object any) { emit(object, nil, false) },
	}
}

func driftObject(object any, watch sdk.WatchKind) *unstructured.Unstructured {
	if tombstone, ok := object.(cache.DeletedFinalStateUnknown); ok {
		object = tombstone.Obj
	}
	if object == nil {
		return nil
	}
	content, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(object)
	if err != nil {
		return nil
	}
	result := &unstructured.Unstructured{Object: content}
	result.SetAPIVersion(watch.APIVersion)
	result.SetKind(watch.Kind)
	return result
}

func objectName(object any) string {
	if tombstone, ok := object.(cache.DeletedFinalStateUnknown); ok {
		object = tombstone.Obj
	}
	accessor, err := meta.Accessor(object)
	if err != nil {
		return ""
	}
	return accessor.GetName()
}

func informerFor(factory informers.SharedInformerFactory, key string) (cache.SharedIndexInformer, error) {
	switch key {
	case "v1/ConfigMap":
		return factory.Core().V1().ConfigMaps().Informer(), nil
	case "v1/Service":
		return factory.Core().V1().Services().Informer(), nil
	case "v1/Pod":
		return factory.Core().V1().Pods().Informer(), nil
	case "apps/v1/Deployment":
		return factory.Apps().V1().Deployments().Informer(), nil
	case "apps/v1/ReplicaSet":
		return factory.Apps().V1().ReplicaSets().Informer(), nil
	case "v1/Endpoints":
		return factory.Core().V1().Endpoints().Informer(), nil
	case "discovery.k8s.io/v1/EndpointSlice":
		return factory.Discovery().V1().EndpointSlices().Informer(), nil
	case "networking.k8s.io/v1/NetworkPolicy":
		return factory.Networking().V1().NetworkPolicies().Informer(), nil
	case "apps/v1/StatefulSet":
		return factory.Apps().V1().StatefulSets().Informer(), nil
	case "apps/v1/DaemonSet":
		return factory.Apps().V1().DaemonSets().Informer(), nil
	case "v1/ServiceAccount":
		return factory.Core().V1().ServiceAccounts().Informer(), nil
	case "v1/PersistentVolumeClaim":
		return factory.Core().V1().PersistentVolumeClaims().Informer(), nil
	case "batch/v1/Job":
		return factory.Batch().V1().Jobs().Informer(), nil
	case "batch/v1/CronJob":
		return factory.Batch().V1().CronJobs().Informer(), nil
	case "rbac.authorization.k8s.io/v1/Role":
		return factory.Rbac().V1().Roles().Informer(), nil
	case "rbac.authorization.k8s.io/v1/RoleBinding":
		return factory.Rbac().V1().RoleBindings().Informer(), nil
	case "rbac.authorization.k8s.io/v1/ClusterRole":
		return factory.Rbac().V1().ClusterRoles().Informer(), nil
	case "rbac.authorization.k8s.io/v1/ClusterRoleBinding":
		return factory.Rbac().V1().ClusterRoleBindings().Informer(), nil
	case "v1/Event":
		return factory.Core().V1().Events().Informer(), nil
	default:
		return nil, fmt.Errorf("unsupported informer resource %q", key)
	}
}

// SourceDriftObjects returns the current non-secret informer inventory. It does
// no API reads and never includes resource versions or status in its verdict.
func (c *KubernetesCache) SourceDriftObjects() []*unstructured.Unstructured {
	c.informerMu.RLock()
	defer c.informerMu.RUnlock()
	objects := []*unstructured.Unstructured{}
	// Resolve relevant cluster RBAC from binding subjects before converting any
	// rule bodies. Unrelated cluster authorization is outside this inventory.
	clusterRoles := map[string]bool{}
	clusterBindings := map[string]bool{}
	bound := func(subjects []rbacv1.Subject, namespace string) bool {
		for _, subject := range subjects {
			ns := subject.Namespace
			if ns == "" {
				ns = namespace
			}
			if subject.Kind == "ServiceAccount" && ns == c.config.Namespace {
				return true
			}
		}
		return false
	}
	for _, key := range []string{"rbac.authorization.k8s.io/v1/RoleBinding", "rbac.authorization.k8s.io/v1/ClusterRoleBinding"} {
		informer := c.informers[key]
		if informer == nil {
			continue
		}
		for _, object := range informer.GetStore().List() {
			switch binding := object.(type) {
			case *rbacv1.RoleBinding:
				if bound(binding.Subjects, binding.Namespace) && binding.RoleRef.Kind == "ClusterRole" {
					clusterRoles[binding.RoleRef.Name] = true
				}
			case *rbacv1.ClusterRoleBinding:
				if bound(binding.Subjects, "") {
					clusterBindings[binding.Name] = true
					if binding.RoleRef.Kind == "ClusterRole" {
						clusterRoles[binding.RoleRef.Name] = true
					}
				}
			case *unstructured.Unstructured:
				for ref := range inventoryRBACScope(c.config.Namespace, []*unstructured.Unstructured{binding}) {
					if ref.Kind == "ClusterRole" {
						clusterRoles[ref.Name] = true
					}
					if ref.Kind == "ClusterRoleBinding" {
						clusterBindings[ref.Name] = true
					}
				}
			}
		}
	}
	for key, informer := range c.informers {
		api, kind := splitInformerKey(key)
		for _, object := range informer.GetStore().List() {
			if kind == "RoleBinding" {
				accessor, err := meta.Accessor(object)
				if err != nil || accessor.GetNamespace() != c.config.Namespace {
					continue
				}
			}
			if kind == "ClusterRole" && !clusterRoles[objectName(object)] || kind == "ClusterRoleBinding" && !clusterBindings[objectName(object)] {
				continue
			}
			if observed := driftObject(object, sdk.WatchKind{APIVersion: api, Kind: kind}); observed != nil {
				objects = append(objects, observed)
			}
		}
	}
	return objects
}

// SourceRemovalObjects retains the complete RBAC graph for negative ownership
// proofs. It is not exposed as application drift or as a detector read view.
func (c *KubernetesCache) SourceRemovalObjects() []*unstructured.Unstructured {
	c.informerMu.RLock()
	defer c.informerMu.RUnlock()
	var objects []*unstructured.Unstructured
	for key, informer := range c.informers {
		api, kind := splitInformerKey(key)
		if api != "rbac.authorization.k8s.io/v1" {
			continue
		}
		for _, object := range informer.GetStore().List() {
			if observed := driftObject(object, sdk.WatchKind{APIVersion: api, Kind: kind}); observed != nil {
				objects = append(objects, observed)
			}
		}
	}
	return objects
}
func splitInformerKey(key string) (string, string) {
	index := strings.LastIndex(key, "/")
	return key[:index], key[index+1:]
}
func (c *KubernetesCache) SourceDriftReady() bool {
	c.informerMu.RLock()
	defer c.informerMu.RUnlock()
	for _, informer := range c.informers {
		if !informer.HasSynced() {
			return false
		}
	}
	return true
}

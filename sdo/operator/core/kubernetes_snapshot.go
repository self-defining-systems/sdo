package core

import (
	"context"
	"fmt"
	"os"
	"sort"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/traffic"
)

type KubernetesSnapshotProvider struct {
	Capture      *GenericCapture
	PodAdmission bool
	Scope        sdk.ObservationScope
	Ceiling      sdk.ObservationScope
	Generation   string
	Namespace    string
	Client       kubernetes.Interface
}

func NewKubernetesSnapshotProvider(namespace string) (*KubernetesSnapshotProvider, error) {
	if namespace == "" {
		return nil, fmt.Errorf("namespace is required")
	}
	client, err := NewKubernetesClient()
	if err != nil {
		return nil, err
	}
	capture, err := NewGenericCapture(namespace)
	if err != nil {
		return nil, err
	}
	return &KubernetesSnapshotProvider{Namespace: namespace, Client: client, Capture: capture}, nil
}

// NewKubernetesClient returns a client with its own transport and rate limiter.
// Long-running controllers use a dedicated instance for Lease renewal so
// informer, state-store, dispatcher, and validator traffic cannot starve
// leader-election requests. Its rate limit defaults to
// DefaultKubernetesClientQPS/Burst and honors SDO_KUBE_API_QPS and
// SDO_KUBE_API_BURST.
func NewKubernetesClient() (kubernetes.Interface, error) {
	config, err := kubernetesConfig()
	if err != nil {
		return nil, err
	}
	if err := applyClientRateLimits(config, os.LookupEnv); err != nil {
		return nil, fmt.Errorf("configure kubernetes client rate limit: %w", err)
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes client: %w", err)
	}
	return client, nil
}

// NewKubernetesMetadataClient creates a metadata-only decoder using the same
// Kubernetes configuration and rate limits as the controller's typed clients.
// Callers must set a strict PartialObjectMetadata Accept header per request.
func NewKubernetesMetadataClient() (rest.Interface, error) {
	config, err := kubernetesConfig()
	if err != nil {
		return nil, err
	}
	if err := applyClientRateLimits(config, os.LookupEnv); err != nil {
		return nil, err
	}
	config = metadata.ConfigFor(config)
	config.ContentType = "application/json"
	config.AcceptContentTypes = "application/json;as=PartialObjectMetadata;g=meta.k8s.io;v=v1"
	config.GroupVersion = &schema.GroupVersion{Version: "v1"}
	config.APIPath = "/api"
	return rest.RESTClientFor(config)
}

func (p KubernetesSnapshotProvider) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	if p.Client == nil {
		return DetectionSnapshot{}, fmt.Errorf("kubernetes client is required")
	}
	namespace := p.Namespace
	if namespace == "" {
		return DetectionSnapshot{}, fmt.Errorf("namespace is required")
	}

	configMaps, err := p.Client.CoreV1().ConfigMaps(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return DetectionSnapshot{}, fmt.Errorf("list configmaps: %w", err)
	}
	services, err := p.Client.CoreV1().Services(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return DetectionSnapshot{}, fmt.Errorf("list services: %w", err)
	}
	pods, err := p.Client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return DetectionSnapshot{}, fmt.Errorf("list pods: %w", err)
	}
	deployments, err := p.Client.AppsV1().Deployments(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return DetectionSnapshot{}, fmt.Errorf("list deployments: %w", err)
	}
	replicaSets, err := p.Client.AppsV1().ReplicaSets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return DetectionSnapshot{}, fmt.Errorf("list replicasets: %w", err)
	}
	statefulSets, err := p.Client.AppsV1().StatefulSets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return DetectionSnapshot{}, fmt.Errorf("list statefulsets: %w", err)
	}
	hpas, err := p.Client.AutoscalingV2().HorizontalPodAutoscalers(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return DetectionSnapshot{}, fmt.Errorf("list horizontalpodautoscalers: %w", err)
	}
	pdbs, err := p.Client.PolicyV1().PodDisruptionBudgets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return DetectionSnapshot{}, fmt.Errorf("list poddisruptionbudgets: %w", err)
	}
	endpoints, err := p.Client.CoreV1().Endpoints(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return DetectionSnapshot{}, fmt.Errorf("list endpoints: %w", err)
	}
	endpointSlices, err := p.Client.DiscoveryV1().EndpointSlices(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return DetectionSnapshot{}, fmt.Errorf("list endpointslices: %w", err)
	}
	networkPolicies, err := p.Client.NetworkingV1().NetworkPolicies(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return DetectionSnapshot{}, fmt.Errorf("list networkpolicies: %w", err)
	}
	events, err := p.Client.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return DetectionSnapshot{}, fmt.Errorf("list events: %w", err)
	}

	scoped, err := ObserveScoped(ctx, p.Client, p.Scope, p.Ceiling, p.Generation)
	if err != nil {
		return nil, err
	}
	var admission *sdk.PodAdmissionResult
	if p.PodAdmission {
		result := ObservePodAdmission(ctx, p.Client, namespace, pods.Items)
		admission = &result
	}
	return DetectionSnapshot{
		Admission:         admission,
		GenericCapture:    p.Capture,
		ScopedSnapshot:    scoped,
		NamespaceName:     namespace,
		ConfigMapList:     configMaps.Items,
		ServiceList:       services.Items,
		PodList:           pods.Items,
		DeploymentList:    deployments.Items,
		ReplicaSetList:    replicaSets.Items,
		StatefulSetList:   statefulSets.Items,
		HPAList:           hpas.Items,
		PDBList:           pdbs.Items,
		EndpointList:      endpoints.Items,
		EndpointSliceList: endpointSlices.Items,
		NetworkPolicyList: networkPolicies.Items,
		EventList:         events.Items,
		// Stamped after every list so no listed object postdates it.
		ObservedAtTime: time.Now().UTC(),
	}, nil
}

type DetectionSnapshot struct {
	Admission          *sdk.PodAdmissionResult `json:"pod_admission,omitempty"`
	GenericCapture     *GenericCapture         `json:"-"`
	sdk.ScopedSnapshot `json:"scoped_observation,omitempty"`
	NamespaceName      string
	ConfigMapList      []corev1.ConfigMap
	ServiceList        []corev1.Service
	PodList            []corev1.Pod
	DeploymentList     []appsv1.Deployment
	ReplicaSetList     []appsv1.ReplicaSet
	StatefulSetList    []appsv1.StatefulSet
	HPAList            []autoscalingv2.HorizontalPodAutoscaler
	PDBList            []policyv1.PodDisruptionBudget
	EndpointList       []corev1.Endpoints
	EndpointSliceList  []discoveryv1.EndpointSlice
	NetworkPolicyList  []networkingv1.NetworkPolicy
	EventList          []corev1.Event
	// TrafficWindows holds synthetic-traffic observations by workload name.
	TrafficWindows map[string]traffic.Window
	// ObservedAtTime is when the snapshot was read. Detectors read it through
	// sdk.ObservationTime instead of calling time.Now.
	ObservedAtTime time.Time
}

func (s DetectionSnapshot) StatefulSets() []appsv1.StatefulSet {
	items := make([]appsv1.StatefulSet, len(s.StatefulSetList))
	for index := range s.StatefulSetList {
		items[index] = *s.StatefulSetList[index].DeepCopy()
	}
	sort.Slice(items, func(left, right int) bool {
		return lessSnapshotObject(items[left].Namespace, items[left].Name, items[right].Namespace, items[right].Name)
	})
	return items
}

func (s DetectionSnapshot) HorizontalPodAutoscalers() []autoscalingv2.HorizontalPodAutoscaler {
	items := make([]autoscalingv2.HorizontalPodAutoscaler, len(s.HPAList))
	for index := range s.HPAList {
		items[index] = *s.HPAList[index].DeepCopy()
	}
	sort.Slice(items, func(left, right int) bool {
		return lessSnapshotObject(items[left].Namespace, items[left].Name, items[right].Namespace, items[right].Name)
	})
	return items
}

func (s DetectionSnapshot) PodDisruptionBudgets() []policyv1.PodDisruptionBudget {
	items := make([]policyv1.PodDisruptionBudget, len(s.PDBList))
	for index := range s.PDBList {
		items[index] = *s.PDBList[index].DeepCopy()
	}
	sort.Slice(items, func(left, right int) bool {
		return lessSnapshotObject(items[left].Namespace, items[left].Name, items[right].Namespace, items[right].Name)
	})
	return items
}

// TrafficWindow implements traffic.Source.
// ObservedAt implements sdk.ObservationClock.
func (s DetectionSnapshot) ObservedAt() time.Time {
	return s.ObservedAtTime
}

func (s DetectionSnapshot) TrafficWindow(workload string) (traffic.Window, bool) {
	window, ok := s.TrafficWindows[workload]
	return window, ok
}

func (s DetectionSnapshot) Namespace() string {
	return s.NamespaceName
}

func (s DetectionSnapshot) ConfigMaps() []corev1.ConfigMap {
	configMaps := make([]corev1.ConfigMap, len(s.ConfigMapList))
	for index := range s.ConfigMapList {
		configMaps[index] = *s.ConfigMapList[index].DeepCopy()
	}
	sort.Slice(configMaps, func(left int, right int) bool {
		if configMaps[left].Namespace != configMaps[right].Namespace {
			return configMaps[left].Namespace < configMaps[right].Namespace
		}
		return configMaps[left].Name < configMaps[right].Name
	})
	return configMaps
}

func (s DetectionSnapshot) Services() []corev1.Service {
	items := make([]corev1.Service, len(s.ServiceList))
	for index := range s.ServiceList {
		items[index] = *s.ServiceList[index].DeepCopy()
	}
	sort.Slice(items, func(left int, right int) bool {
		return lessSnapshotObject(items[left].Namespace, items[left].Name, items[right].Namespace, items[right].Name)
	})
	return items
}

func (s DetectionSnapshot) Pods() []corev1.Pod {
	items := make([]corev1.Pod, len(s.PodList))
	for index := range s.PodList {
		items[index] = *s.PodList[index].DeepCopy()
	}
	sort.Slice(items, func(left int, right int) bool {
		return lessSnapshotObject(items[left].Namespace, items[left].Name, items[right].Namespace, items[right].Name)
	})
	return items
}

func (s DetectionSnapshot) Deployments() []appsv1.Deployment {
	items := make([]appsv1.Deployment, len(s.DeploymentList))
	for index := range s.DeploymentList {
		items[index] = *s.DeploymentList[index].DeepCopy()
	}
	sort.Slice(items, func(left int, right int) bool {
		return lessSnapshotObject(items[left].Namespace, items[left].Name, items[right].Namespace, items[right].Name)
	})
	return items
}

func (s DetectionSnapshot) ReplicaSets() []appsv1.ReplicaSet {
	items := make([]appsv1.ReplicaSet, len(s.ReplicaSetList))
	for index := range s.ReplicaSetList {
		items[index] = *s.ReplicaSetList[index].DeepCopy()
	}
	sort.Slice(items, func(left int, right int) bool {
		return lessSnapshotObject(items[left].Namespace, items[left].Name, items[right].Namespace, items[right].Name)
	})
	return items
}

func (s DetectionSnapshot) Endpoints() []corev1.Endpoints {
	items := make([]corev1.Endpoints, len(s.EndpointList))
	for index := range s.EndpointList {
		items[index] = *s.EndpointList[index].DeepCopy()
	}
	sort.Slice(items, func(left int, right int) bool {
		return lessSnapshotObject(items[left].Namespace, items[left].Name, items[right].Namespace, items[right].Name)
	})
	return items
}

func (s DetectionSnapshot) EndpointSlices() []discoveryv1.EndpointSlice {
	items := make([]discoveryv1.EndpointSlice, len(s.EndpointSliceList))
	for index := range s.EndpointSliceList {
		items[index] = *s.EndpointSliceList[index].DeepCopy()
	}
	sort.Slice(items, func(left int, right int) bool {
		return lessSnapshotObject(items[left].Namespace, items[left].Name, items[right].Namespace, items[right].Name)
	})
	return items
}

func (s DetectionSnapshot) NetworkPolicies() []networkingv1.NetworkPolicy {
	items := make([]networkingv1.NetworkPolicy, len(s.NetworkPolicyList))
	for index := range s.NetworkPolicyList {
		items[index] = *s.NetworkPolicyList[index].DeepCopy()
	}
	sort.Slice(items, func(left int, right int) bool {
		return lessSnapshotObject(items[left].Namespace, items[left].Name, items[right].Namespace, items[right].Name)
	})
	return items
}

func (s DetectionSnapshot) Events() []corev1.Event {
	items := make([]corev1.Event, len(s.EventList))
	for index := range s.EventList {
		items[index] = *s.EventList[index].DeepCopy()
	}
	sort.Slice(items, func(left int, right int) bool {
		return lessSnapshotObject(items[left].Namespace, items[left].Name, items[right].Namespace, items[right].Name)
	})
	return items
}

func (s DetectionSnapshot) ReadyEndpointCountForService(namespace string, service string) int {
	ready := 0
	for _, endpoints := range s.Endpoints() {
		if endpoints.Namespace != namespace || endpoints.Name != service {
			continue
		}
		for _, subset := range endpoints.Subsets {
			ready += len(subset.Addresses)
		}
	}
	return ready
}

func (s DetectionSnapshot) PodsForService(namespace string, service string) []corev1.Pod {
	var selector labels.Selector
	for _, candidate := range s.Services() {
		if candidate.Namespace == namespace && candidate.Name == service {
			selector = labels.SelectorFromSet(candidate.Spec.Selector)
			break
		}
	}
	if selector == nil || selector.Empty() {
		return nil
	}

	pods := make([]corev1.Pod, 0)
	for _, pod := range s.Pods() {
		if pod.Namespace == namespace && selector.Matches(labels.Set(pod.Labels)) {
			pods = append(pods, pod)
		}
	}
	return pods
}

func (s DetectionSnapshot) RecentEventsFor(namespace string, kind string, name string) []corev1.Event {
	events := make([]corev1.Event, 0)
	for _, event := range s.Events() {
		ref := event.InvolvedObject
		if ref.Namespace == namespace && ref.Kind == kind && ref.Name == name {
			events = append(events, event)
		}
	}
	return events
}

func lessSnapshotObject(leftNamespace string, leftName string, rightNamespace string, rightName string) bool {
	if leftNamespace != rightNamespace {
		return leftNamespace < rightNamespace
	}
	return leftName < rightName
}

func kubernetesConfig() (*rest.Config, error) {
	config, err := rest.InClusterConfig()
	if err == nil {
		return config, nil
	}

	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfig := os.Getenv("KUBECONFIG"); kubeconfig != "" {
		loadingRules.ExplicitPath = kubeconfig
	} else if home := homedir.HomeDir(); home != "" {
		loadingRules.ExplicitPath = home + "/.kube/config"
	}
	config, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		loadingRules,
		&clientcmd.ConfigOverrides{},
	).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubernetes config: %w", err)
	}
	return config, nil
}

func (s DetectionSnapshot) PodAdmission() (sdk.PodAdmissionResult, bool) {
	if s.Admission == nil {
		return sdk.PodAdmissionResult{}, false
	}
	return *s.Admission, true
}

func (s DetectionSnapshot) CaptureObjects(refs []sdk.ObjectRef) ([]unstructured.Unstructured, error) {
	if s.GenericCapture != nil {
		return s.GenericCapture.CaptureObjects(refs)
	}
	return sdk.TypedObjects(s), nil
}
func (s DetectionSnapshot) Object(ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if s.GenericCapture != nil {
		return s.GenericCapture.Object(ref)
	}
	return sdk.FindObject(sdk.TypedObjects(s), ref)
}

// DiffAgainstSource requires the runtime's accepted-source read view.
func (s DetectionSnapshot) DiffAgainstSource(ref sdk.ObjectRef) (sdk.SourceDiff, error) {
	return sdk.SourceDiff{}, fmt.Errorf("accepted source diff unavailable for %s/%s", ref.Kind, ref.Name)
}

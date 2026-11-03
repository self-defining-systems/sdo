//go:build integration && controller_e2e

package e2e

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const nodeName = "e2e-node"

// Kubelet stands in for everything envtest lacks: one Node, the Deployment,
// ReplicaSet, Endpoints and Job controllers, and a kubelet that runs worker and
// controller containers as local bubblewrap processes. Application pods are not
// executed; their phase follows a small model of the toy application.
type Kubelet struct {
	t       testing.TB
	cluster *Cluster
	Images  *Images
	Run     *Runtime
	net     *network

	port    int
	address string
	counter atomic.Int64
	ipMu    sync.Mutex
	ips     map[types.UID]string
	logsMu  sync.Mutex
	appLogs map[string]*LogBuffer

	// JobBackoffBase is the Job controller's delay after the first failed Pod; it doubles per failure up to 6 minutes. The
	// real value is 10 s; the harness shortens it so restarts do not dominate a run.
	JobBackoffBase time.Duration
	cancel         context.CancelFunc
	done           chan struct{}
	// Errors counts reconcile errors by text; the harness fails a run on any that is not a conflict.
	Errors sync.Map
}

// StartKubelet creates the Node and starts the reconcile loop and the HTTPS endpoint the API server proxies to.
func StartKubelet(t testing.TB, cluster *Cluster, images *Images, runDir string) *Kubelet {
	t.Helper()
	k := &Kubelet{JobBackoffBase: 10 * time.Second, t: t, cluster: cluster, Images: images, ips: map[types.UID]string{}, appLogs: map[string]*LogBuffer{}, done: make(chan struct{})}
	k.net = newNetwork(filepath.Join(runDir, "hosts"))
	if err := os.WriteFile(k.net.hostsPath, []byte("127.0.0.1 localhost\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	k.Run = newRuntime(t, cluster, images, runDir, k)
	k.port = freePort(t)
	// The API server refuses to proxy to loopback addresses, so the Node needs a routable one.
	k.address = hostAddress()
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", k.address, k.port))
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.LoadX509KeyPair(filepath.Join(cluster.Dir, "tls.crt"), filepath.Join(cluster.Dir, "tls.key"))
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: k.handler(), TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}}}
	go func() { _ = server.ServeTLS(listener, "", "") }()
	ctx, cancel := context.WithCancel(context.Background())
	k.cancel = cancel
	k.createNode(ctx)
	go k.loop(ctx)
	t.Cleanup(func() {
		cancel()
		<-k.done
		_ = server.Close()
		k.Run.killAll()
		k.net.close()
	})
	return k
}

func (k *Kubelet) createNode(ctx context.Context) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName, Labels: map[string]string{"kubernetes.io/hostname": nodeName}}}
	created, err := k.cluster.Client.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{})
	if err != nil {
		k.t.Fatalf("create node: %v", err)
	}
	created.Status = corev1.NodeStatus{
		Addresses:       []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: k.address}, {Type: corev1.NodeHostName, Address: nodeName}},
		DaemonEndpoints: corev1.NodeDaemonEndpoints{KubeletEndpoint: corev1.DaemonEndpoint{Port: int32(k.port)}},
		Conditions:      []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue, Reason: "KubeletReady", LastHeartbeatTime: metav1.Now(), LastTransitionTime: metav1.Now()}},
		Capacity:        corev1.ResourceList{corev1.ResourceCPU: mustQuantity("16"), corev1.ResourceMemory: mustQuantity("64Gi"), corev1.ResourcePods: mustQuantity("500")},
		Allocatable:     corev1.ResourceList{corev1.ResourceCPU: mustQuantity("16"), corev1.ResourceMemory: mustQuantity("64Gi"), corev1.ResourcePods: mustQuantity("500")},
	}
	if _, err := k.cluster.Client.CoreV1().Nodes().UpdateStatus(ctx, created, metav1.UpdateOptions{}); err != nil {
		k.t.Fatalf("node status: %v", err)
	}
}

func (k *Kubelet) loop(ctx context.Context) {
	defer close(k.done)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for name, step := range map[string]func(context.Context) error{
			"deployments": k.reconcileDeployments, "pods": k.reconcilePods, "jobs": k.reconcileJobs,
			"endpoints": k.reconcileEndpoints, "network": k.reconcileNetwork, "namespaces": k.reconcileNamespaces, "claims": k.reconcileClaims,
		} {
			if err := step(ctx); err != nil && ctx.Err() == nil && !apierrors.IsConflict(err) && !apierrors.IsNotFound(err) && !apierrors.IsAlreadyExists(err) {
				k.Errors.Store(name+": "+err.Error(), true)
			}
		}
	}
}

func (k *Kubelet) nextName(prefix string) string {
	return fmt.Sprintf("%s-%05x", prefix, k.counter.Add(1)+0x10000)
}

func (k *Kubelet) podIP(uid types.UID) string {
	k.ipMu.Lock()
	defer k.ipMu.Unlock()
	if ip, ok := k.ips[uid]; ok {
		return ip
	}
	ip := "127.1." + loopback(len(k.ips)+1)
	k.ips[uid] = ip
	return ip
}

func hashOf(v any) string {
	data, _ := json.Marshal(v)
	h := fnv.New32a()
	_, _ = h.Write(data)
	return strconv.FormatUint(uint64(h.Sum32()), 36)
}

func ownedBy(object metav1.Object, kind string, uid types.UID) bool {
	for _, ref := range object.GetOwnerReferences() {
		if ref.Kind == kind && ref.UID == uid {
			return true
		}
	}
	return false
}

func ownerKind(object metav1.Object) string {
	for _, ref := range object.GetOwnerReferences() {
		if ref.Controller != nil && *ref.Controller {
			return ref.Kind
		}
	}
	return ""
}

func controllerRef(kind, apiVersion, name string, uid types.UID) []metav1.OwnerReference {
	yes := true
	return []metav1.OwnerReference{{APIVersion: apiVersion, Kind: kind, Name: name, UID: uid, Controller: &yes, BlockOwnerDeletion: &yes}}
}

// reconcileClaims marks PersistentVolumeClaims bound: there is no volume controller.
func (k *Kubelet) reconcileClaims(ctx context.Context) error {
	claims, err := k.cluster.Client.CoreV1().PersistentVolumeClaims("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	for i := range claims.Items {
		claim := claims.Items[i]
		if claim.Status.Phase == corev1.ClaimBound {
			continue
		}
		claim.Status.Phase = corev1.ClaimBound
		claim.Status.AccessModes = claim.Spec.AccessModes
		claim.Status.Capacity = claim.Spec.Resources.Requests
		if _, err := k.cluster.Client.CoreV1().PersistentVolumeClaims(claim.Namespace).UpdateStatus(ctx, &claim, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}
	return nil
}

// reconcileDeployments is the Deployment and ReplicaSet controller for Recreate and RollingUpdate.
func (k *Kubelet) reconcileDeployments(ctx context.Context) error {
	client := k.cluster.Client
	deployments, err := client.AppsV1().Deployments("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	live := map[types.UID]bool{}
	for i := range deployments.Items {
		deployment := &deployments.Items[i]
		live[deployment.UID] = true
		if err := k.reconcileDeployment(ctx, deployment); err != nil {
			return err
		}
	}
	// Garbage collection: ReplicaSets whose Deployment is gone.
	sets, err := client.AppsV1().ReplicaSets("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	for _, set := range sets.Items {
		if ownerKind(&set) == "Deployment" && !live[set.OwnerReferences[0].UID] {
			_ = client.AppsV1().ReplicaSets(set.Namespace).Delete(ctx, set.Name, metav1.DeleteOptions{})
		}
	}
	return nil
}

func (k *Kubelet) reconcileDeployment(ctx context.Context, deployment *appsv1.Deployment) error {
	client := k.cluster.Client
	replicas := int32(1)
	if deployment.Spec.Replicas != nil {
		replicas = *deployment.Spec.Replicas
	}
	hash := hashOf(deployment.Spec.Template)
	current := deployment.Name + "-" + hash
	sets, err := client.AppsV1().ReplicaSets(deployment.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	pods, err := client.CoreV1().Pods(deployment.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	var newSet *appsv1.ReplicaSet
	var oldSets []*appsv1.ReplicaSet
	for i := range sets.Items {
		set := &sets.Items[i]
		if !ownedBy(set, "Deployment", deployment.UID) {
			continue
		}
		if set.Name == current {
			newSet = set
		} else {
			oldSets = append(oldSets, set)
		}
	}
	podsOf := func(set *appsv1.ReplicaSet) (all, ready []*corev1.Pod) {
		for i := range pods.Items {
			pod := &pods.Items[i]
			if ownedBy(pod, "ReplicaSet", set.UID) {
				all = append(all, pod)
				if pod.DeletionTimestamp == nil && podReady(pod) {
					ready = append(ready, pod)
				}
			}
		}
		return all, ready
	}
	if newSet == nil {
		template := deployment.Spec.Template.DeepCopy()
		if template.Labels == nil {
			template.Labels = map[string]string{}
		}
		template.Labels["pod-template-hash"] = hash
		selector := metav1.SetAsLabelSelector(map[string]string{"pod-template-hash": hash})
		if deployment.Spec.Selector != nil {
			selector = deployment.Spec.Selector.DeepCopy()
			selector.MatchLabels = mergeMaps(selector.MatchLabels, map[string]string{"pod-template-hash": hash})
		}
		zero := int32(0)
		newSet, err = client.AppsV1().ReplicaSets(deployment.Namespace).Create(ctx, &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{Name: current, Namespace: deployment.Namespace, Labels: template.Labels, OwnerReferences: controllerRef("Deployment", "apps/v1", deployment.Name, deployment.UID)},
			Spec:       appsv1.ReplicaSetSpec{Replicas: &zero, Selector: selector, Template: *template},
		}, metav1.CreateOptions{})
		if err != nil {
			return err
		}
	}
	_, newReady := podsOf(newSet)
	oldTotal := 0
	for _, old := range oldSets {
		all, _ := podsOf(old)
		oldTotal += len(all)
		if old.Spec.Replicas == nil || *old.Spec.Replicas != 0 {
			zero := int32(0)
			old.Spec.Replicas = &zero
			if _, err := client.AppsV1().ReplicaSets(old.Namespace).Update(ctx, old, metav1.UpdateOptions{}); err != nil {
				return err
			}
		}
		recreate := deployment.Spec.Strategy.Type == appsv1.RecreateDeploymentStrategyType
		if recreate || int32(len(newReady)) >= replicas {
			for _, pod := range all {
				if pod.DeletionTimestamp == nil {
					_ = client.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{GracePeriodSeconds: new(int64)})
				}
			}
		}
	}
	// New pods start once the strategy allows them.
	want := replicas
	if deployment.Spec.Strategy.Type == appsv1.RecreateDeploymentStrategyType && oldTotal > 0 {
		want = 0
	}
	if newSet.Spec.Replicas == nil || *newSet.Spec.Replicas != want {
		newSet.Spec.Replicas = &want
		if newSet, err = client.AppsV1().ReplicaSets(newSet.Namespace).Update(ctx, newSet, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}
	all, ready := podsOf(newSet)
	active := 0
	for _, pod := range all {
		if pod.DeletionTimestamp == nil {
			active++
		}
	}
	for ; int32(active) < want; active++ {
		template := newSet.Spec.Template
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: k.nextName(newSet.Name), Namespace: newSet.Namespace, Labels: template.Labels, Annotations: template.Annotations, OwnerReferences: controllerRef("ReplicaSet", "apps/v1", newSet.Name, newSet.UID)}, Spec: *template.Spec.DeepCopy()}
		if _, err := client.CoreV1().Pods(newSet.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
			return err
		}
	}
	if int32(active) > want {
		extra := active - int(want)
		for _, pod := range all {
			if extra == 0 {
				break
			}
			if pod.DeletionTimestamp == nil {
				_ = client.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{GracePeriodSeconds: new(int64)})
				extra--
			}
		}
	}
	// Status.
	oldAll := 0
	for _, old := range oldSets {
		all, _ := podsOf(old)
		oldAll += len(all)
		status := appsv1.ReplicaSetStatus{Replicas: int32(len(all)), ObservedGeneration: old.Generation}
		if old.Status.Replicas != status.Replicas {
			old.Status = status
			_, _ = client.AppsV1().ReplicaSets(old.Namespace).UpdateStatus(ctx, old, metav1.UpdateOptions{})
		}
	}
	setStatus := appsv1.ReplicaSetStatus{Replicas: int32(len(all)), ReadyReplicas: int32(len(ready)), AvailableReplicas: int32(len(ready)), FullyLabeledReplicas: int32(len(all)), ObservedGeneration: newSet.Generation}
	if newSet.Status.Replicas != setStatus.Replicas || newSet.Status.ReadyReplicas != setStatus.ReadyReplicas || newSet.Status.ObservedGeneration != setStatus.ObservedGeneration {
		newSet.Status = setStatus
		if _, err := client.AppsV1().ReplicaSets(newSet.Namespace).UpdateStatus(ctx, newSet, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}
	status := appsv1.DeploymentStatus{ObservedGeneration: deployment.Generation, Replicas: int32(len(all) + oldAll), UpdatedReplicas: int32(len(all)), ReadyReplicas: int32(len(ready)), AvailableReplicas: int32(len(ready))}
	if status.Replicas > 0 && int32(len(ready)) == 0 {
		status.UnavailableReplicas = status.Replicas
	}
	availability := corev1.ConditionFalse
	if int32(len(ready)) >= replicas {
		availability = corev1.ConditionTrue
	}
	status.Conditions = []appsv1.DeploymentCondition{{Type: appsv1.DeploymentAvailable, Status: availability, Reason: "MinimumReplicasAvailable", LastUpdateTime: metav1.Now(), LastTransitionTime: metav1.Now()}}
	if sameJSON(deployment.Status.Replicas, status.Replicas) && deployment.Status.ReadyReplicas == status.ReadyReplicas && deployment.Status.UpdatedReplicas == status.UpdatedReplicas && deployment.Status.ObservedGeneration == status.ObservedGeneration && deployment.Status.UnavailableReplicas == status.UnavailableReplicas {
		return nil
	}
	deployment.Status = status
	_, err = client.AppsV1().Deployments(deployment.Namespace).UpdateStatus(ctx, deployment, metav1.UpdateOptions{})
	return err
}

func sameJSON(a, b any) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}

func mergeMaps(a, b map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

func podReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// reconcileEndpoints keeps Endpoints and EndpointSlices of every selector Service current.
func (k *Kubelet) reconcileEndpoints(ctx context.Context) error {
	client := k.cluster.Client
	services, err := client.CoreV1().Services("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	pods, err := client.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	for i := range services.Items {
		service := &services.Items[i]
		if len(service.Spec.Selector) == 0 {
			continue
		}
		selector := labels.SelectorFromSet(service.Spec.Selector)
		var ready, notReady []corev1.EndpointAddress
		portsByName := map[string]int32{}
		for p := range pods.Items {
			pod := &pods.Items[p]
			if pod.Namespace != service.Namespace || pod.DeletionTimestamp != nil || pod.Status.PodIP == "" || !selector.Matches(labels.Set(pod.Labels)) {
				continue
			}
			address := corev1.EndpointAddress{IP: endpointAddr(pod.Status.PodIP), NodeName: &pod.Spec.NodeName, TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID}}
			if podReady(pod) {
				ready = append(ready, address)
			} else {
				notReady = append(notReady, address)
			}
			for _, c := range pod.Spec.Containers {
				for _, port := range c.Ports {
					if port.Name != "" {
						portsByName[port.Name] = port.ContainerPort
					}
				}
			}
		}
		var endpointPorts []corev1.EndpointPort
		var slicePorts []discoveryv1.EndpointPort
		for _, port := range service.Spec.Ports {
			target := port.Port
			switch port.TargetPort.Type {
			case intstr.Int:
				if port.TargetPort.IntVal != 0 {
					target = port.TargetPort.IntVal
				}
			case intstr.String:
				resolved, ok := portsByName[port.TargetPort.StrVal]
				if !ok {
					continue
				}
				target = resolved
			}
			protocol := port.Protocol
			if protocol == "" {
				protocol = corev1.ProtocolTCP
			}
			endpointPorts = append(endpointPorts, corev1.EndpointPort{Name: port.Name, Port: target, Protocol: protocol})
			name, proto, number := port.Name, protocol, target
			slicePorts = append(slicePorts, discoveryv1.EndpointPort{Name: &name, Protocol: &proto, Port: &number})
		}
		sort.Slice(ready, func(a, b int) bool { return ready[a].IP < ready[b].IP })
		sort.Slice(notReady, func(a, b int) bool { return notReady[a].IP < notReady[b].IP })
		subsets := []corev1.EndpointSubset{}
		if len(ready)+len(notReady) > 0 && len(endpointPorts) > 0 {
			subsets = append(subsets, corev1.EndpointSubset{Addresses: ready, NotReadyAddresses: notReady, Ports: endpointPorts})
		}
		if err := k.applyEndpoints(ctx, service, subsets); err != nil {
			return err
		}
		var endpoints []discoveryv1.Endpoint
		yes, no := true, false
		for _, a := range ready {
			endpoints = append(endpoints, discoveryv1.Endpoint{Addresses: []string{a.IP}, Conditions: discoveryv1.EndpointConditions{Ready: &yes, Serving: &yes, Terminating: &no}, NodeName: a.NodeName, TargetRef: a.TargetRef})
		}
		for _, a := range notReady {
			endpoints = append(endpoints, discoveryv1.Endpoint{Addresses: []string{a.IP}, Conditions: discoveryv1.EndpointConditions{Ready: &no, Serving: &no, Terminating: &no}, NodeName: a.NodeName, TargetRef: a.TargetRef})
		}
		if err := k.applySlice(ctx, service, endpoints, slicePorts); err != nil {
			return err
		}
	}
	return nil
}

func (k *Kubelet) applyEndpoints(ctx context.Context, service *corev1.Service, subsets []corev1.EndpointSubset) error {
	api := k.cluster.Client.CoreV1().Endpoints(service.Namespace)
	existing, err := api.Get(ctx, service.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = api.Create(ctx, &corev1.Endpoints{ObjectMeta: metav1.ObjectMeta{Name: service.Name, Namespace: service.Namespace, Labels: service.Labels}, Subsets: subsets}, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	if sameJSON(existing.Subsets, subsets) {
		return nil
	}
	existing.Subsets = subsets
	_, err = api.Update(ctx, existing, metav1.UpdateOptions{})
	return err
}

func (k *Kubelet) applySlice(ctx context.Context, service *corev1.Service, endpoints []discoveryv1.Endpoint, ports []discoveryv1.EndpointPort) error {
	api := k.cluster.Client.DiscoveryV1().EndpointSlices(service.Namespace)
	name := service.Name + "-e2e"
	existing, err := api.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = api.Create(ctx, &discoveryv1.EndpointSlice{
			ObjectMeta:  metav1.ObjectMeta{Name: name, Namespace: service.Namespace, Labels: map[string]string{discoveryv1.LabelServiceName: service.Name, discoveryv1.LabelManagedBy: "endpointslice-controller.k8s.io"}},
			AddressType: discoveryv1.AddressTypeIPv4, Endpoints: endpoints, Ports: ports,
		}, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	if sameJSON(existing.Endpoints, endpoints) && sameJSON(existing.Ports, ports) {
		return nil
	}
	existing.Endpoints, existing.Ports = endpoints, ports
	_, err = api.Update(ctx, existing, metav1.UpdateOptions{})
	return err
}

// reconcileJobs is the Job controller: pods from templates, backoff, deadline and completion.
func (k *Kubelet) reconcileJobs(ctx context.Context) error {
	client := k.cluster.Client
	jobs, err := client.BatchV1().Jobs("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	pods, err := client.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	live := map[types.UID]bool{}
	for i := range jobs.Items {
		job := &jobs.Items[i]
		if job.DeletionTimestamp != nil {
			// envtest has no garbage collector. Foreground deletion must retire
			// dependents and release its finalizer, never schedule another worker.
			dependents := false
			for _, pod := range pods.Items {
				if pod.Namespace == job.Namespace && ownedBy(&pod, "Job", job.UID) {
					dependents = true
					if pod.DeletionTimestamp == nil {
						if err := client.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
							return err
						}
					}
				}
			}
			if !dependents && removeForegroundFinalizer(job) {
				if _, err := client.BatchV1().Jobs(job.Namespace).Update(ctx, job, metav1.UpdateOptions{}); err != nil && !apierrors.IsNotFound(err) {
					return err
				}
			}
			continue
		}
		live[job.UID] = true
		if err := k.reconcileJob(ctx, job, pods.Items); err != nil {
			return err
		}
	}
	// Garbage collection: pods whose Job is gone (the API server has no collector).
	for i := range pods.Items {
		pod := &pods.Items[i]
		if ownerKind(pod) == "Job" && !live[pod.OwnerReferences[0].UID] && pod.DeletionTimestamp == nil {
			_ = client.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{GracePeriodSeconds: new(int64)})
		}
	}
	return nil
}

func hasCondition(job *batchv1.Job, kind batchv1.JobConditionType) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == kind && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func (k *Kubelet) reconcileJob(ctx context.Context, job *batchv1.Job, allPods []corev1.Pod) error {
	client := k.cluster.Client
	var mine []*corev1.Pod
	for i := range allPods {
		if allPods[i].Namespace == job.Namespace && ownedBy(&allPods[i], "Job", job.UID) {
			mine = append(mine, &allPods[i])
		}
	}
	var active, succeeded, failed int32
	for _, pod := range mine {
		switch pod.Status.Phase {
		case corev1.PodSucceeded:
			succeeded++
		case corev1.PodFailed:
			failed++
		default:
			if pod.DeletionTimestamp == nil {
				active++
			}
		}
	}
	if hasCondition(job, batchv1.JobComplete) || hasCondition(job, batchv1.JobFailed) {
		return nil
	}
	now := metav1.Now()
	status := job.Status.DeepCopy()
	if status.StartTime == nil {
		status.StartTime = &now
	}
	var finish batchv1.JobConditionType
	reason, message := "", ""
	backoff := int32(6)
	if job.Spec.BackoffLimit != nil {
		backoff = *job.Spec.BackoffLimit
	}
	switch {
	case succeeded >= 1:
		finish, reason = batchv1.JobComplete, "CompletionsReached"
	case failed > backoff:
		finish, reason, message = batchv1.JobFailed, "BackoffLimitExceeded", "Job has reached the specified backoff limit"
	case job.Spec.ActiveDeadlineSeconds != nil && now.Sub(status.StartTime.Time) > time.Duration(*job.Spec.ActiveDeadlineSeconds)*time.Second:
		finish, reason, message = batchv1.JobFailed, "DeadlineExceeded", "Job was active longer than specified deadline"
	}
	if finish == "" && active == 0 && failed > 0 && !k.backoffElapsed(mine, failed) {
		return nil
	}
	if finish == "" && active == 0 {
		template := job.Spec.Template
		podLabels := mergeMaps(template.Labels, map[string]string{"job-name": job.Name, "batch.kubernetes.io/job-name": job.Name, "controller-uid": string(job.UID), "batch.kubernetes.io/controller-uid": string(job.UID)})
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: k.nextName(job.Name), Namespace: job.Namespace, Labels: podLabels, Annotations: template.Annotations, Finalizers: nil, OwnerReferences: controllerRef("Job", "batch/v1", job.Name, job.UID)}, Spec: *template.Spec.DeepCopy()}
		if _, err := client.CoreV1().Pods(job.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
			return err
		}
		active++
	}
	if finish != "" {
		for _, pod := range mine {
			if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed && pod.DeletionTimestamp == nil {
				_ = client.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{GracePeriodSeconds: new(int64)})
			}
		}
		active = 0
	}
	ready := int32(0)
	status.Active, status.Succeeded, status.Failed, status.Ready = active, succeeded, failed, &ready
	status.Terminating = new(int32)
	if finish != "" {
		condition := batchv1.JobCondition{Type: finish, Status: corev1.ConditionTrue, Reason: reason, Message: message, LastProbeTime: now, LastTransitionTime: now}
		if finish == batchv1.JobComplete {
			status.CompletionTime = &now
			status.Conditions = append(status.Conditions,
				batchv1.JobCondition{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue, Reason: reason, LastProbeTime: now, LastTransitionTime: now}, condition)
		} else {
			status.Conditions = append(status.Conditions,
				batchv1.JobCondition{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Reason: reason, Message: message, LastProbeTime: now, LastTransitionTime: now}, condition)
		}
	}
	if sameJSON(job.Status, status) {
		return nil
	}
	job.Status = *status
	_, err := client.BatchV1().Jobs(job.Namespace).UpdateStatus(ctx, job, metav1.UpdateOptions{})
	return err
}

func mustQuantity(s string) resource.Quantity { return resource.MustParse(s) }

// hostAddress is a non-loopback IPv4 address of this host (the API server will not proxy to loopback).
func hostAddress() string {
	addresses, _ := net.InterfaceAddrs()
	for _, a := range addresses {
		if ipNet, ok := a.(*net.IPNet); ok && !ipNet.IP.IsLoopback() && ipNet.IP.To4() != nil && !ipNet.IP.IsLinkLocalUnicast() {
			return ipNet.IP.String()
		}
	}
	return "127.0.0.1"
}

// reconcileNamespaces plays the ServiceAccount and root-CA publisher controllers: every namespace gets a
// "default" ServiceAccount and the kube-root-ca.crt ConfigMap, which workloads and detectors expect.
func (k *Kubelet) reconcileNamespaces(ctx context.Context) error {
	client := k.cluster.Client
	namespaces, err := client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	ca, _ := os.ReadFile(k.cluster.CAPath)
	for _, ns := range namespaces.Items {
		if ns.DeletionTimestamp != nil {
			continue
		}
		if _, err := client.CoreV1().ServiceAccounts(ns.Name).Create(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: ns.Name}}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
		if _, err := client.CoreV1().ConfigMaps(ns.Name).Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "kube-root-ca.crt", Namespace: ns.Name}, Data: map[string]string{"ca.crt": string(ca)}}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
	}
	return nil
}

// backoffElapsed implements the Job controller's exponential backoff: base * 2^(failures-1), at most 6 minutes,
// counted from the newest failed Pod's finish time.
func (k *Kubelet) backoffElapsed(pods []*corev1.Pod, failed int32) bool {
	delay := k.JobBackoffBase << min(failed-1, 10)
	delay = min(delay, 6*time.Minute)
	var newest time.Time
	for _, pod := range pods {
		for _, status := range pod.Status.ContainerStatuses {
			if t := status.State.Terminated; t != nil && t.FinishedAt.Time.After(newest) {
				newest = t.FinishedAt.Time
			}
		}
	}
	return newest.IsZero() || time.Since(newest) >= delay
}

// Only the GC-owned finalizer belongs to this harness controller.
func removeForegroundFinalizer(object metav1.Object) bool {
	old := object.GetFinalizers()
	var kept []string
	removed := false
	for _, finalizer := range old {
		if finalizer == metav1.FinalizerDeleteDependents {
			removed = true
		} else {
			kept = append(kept, finalizer)
		}
	}
	if removed {
		object.SetFinalizers(kept)
	}
	return removed
}

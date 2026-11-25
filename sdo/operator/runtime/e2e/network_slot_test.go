//go:build integration && controller_e2e

package e2e

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// Two worlds on one host must never share a loopback address: the second world's
// listeners used to fail silently, and its probes reached the first world's
// applications or were refused (a spurious second incident in scenario 1).
func TestConcurrentWorldsClaimDisjointLoopbackRanges(t *testing.T) {
	first, firstPods, firstServices, err := claimSlot(loopbackSlotPort + 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Close() }()
	second, secondPods, secondServices, err := claimSlot(loopbackSlotPort + 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Close() }()
	ranges := map[int]bool{firstPods: true, firstServices: true, secondPods: true, secondServices: true}
	if len(ranges) != 4 {
		t.Fatalf("worlds share loopback ranges: first %d/%d, second %d/%d", firstPods, firstServices, secondPods, secondServices)
	}
}

func TestPodAddressesAreBindableAndPerSlot(t *testing.T) {
	l, err := net.Listen("tcp", net.JoinHostPort(podLoopback(1), "0"))
	if err != nil {
		t.Fatalf("this process's Pod address must be bindable: %v", err)
	}
	_ = l.Close()
	if podLoopback(1) == serviceLoopback(1) {
		t.Fatal("Pod and Service ranges must differ")
	}
}

func TestAListenFailureIsAReconcileErrorNotASilentSkip(t *testing.T) {
	ip := podLoopback(3)
	taken, err := net.Listen("tcp", net.JoinHostPort(ip, "0"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = taken.Close() }()
	port := int32(taken.Addr().(*net.TCPAddr).Port)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "app", UID: "pod", OwnerReferences: controllerRef("ReplicaSet", "apps/v1", "app", "rs")},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Ports: []corev1.ContainerPort{{ContainerPort: port}}}}},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning, PodIP: ip},
	}
	k := &Kubelet{cluster: &Cluster{Client: fake.NewSimpleClientset(pod)}, net: newNetwork(filepath.Join(t.TempDir(), "hosts"))}
	t.Cleanup(k.net.close)
	if err := k.reconcileNetwork(context.Background()); err == nil {
		t.Fatal("a Pod whose address is already bound by another world must fail the reconcile, not silently run without a listener")
	}
}

// Pods share the host's loopback, so a fixed pod-local port (the API-history sidecar's
// 127.0.0.1:18080) is one port for every world on the host: with four worlds running at
// once the sidecars of three failed to bind and their incidents never saw API history.
// Worlds therefore run one at a time per host.
func TestWorldsOnOneHostRunOneAtATime(t *testing.T) {
	const port = loopbackSlotPort + 2000
	release, err := lockHost(context.Background(), port)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if second, err := lockHost(ctx, port); err == nil {
		second()
		t.Fatal("a second world must wait while the first holds the host")
	}
	release()
	second, err := lockHost(context.Background(), port)
	if err != nil {
		t.Fatalf("the host is free once the first world finishes: %v", err)
	}
	second()
}

func TestAPIProxyAddressIsLocalToEachPod(t *testing.T) {
	const proxy = "http://127.0.0.1:18080"
	first := map[string]string{"SDO_API_PROXY_URL": proxy}
	second := map[string]string{"SDO_API_PROXY_URL": proxy}
	sidecar := map[string]string{}
	setPodAPIProxyEnv(first, podLoopback(8))
	setPodAPIProxyEnv(second, podLoopback(9))
	setPodAPIProxyEnv(sidecar, podLoopback(8))
	if first["SDO_API_PROXY_URL"] != "http://"+sidecar["SDO_API_PROXY_ADDRESS"] {
		t.Fatalf("agent proxy %q does not match its sidecar %q", first["SDO_API_PROXY_URL"], sidecar["SDO_API_PROXY_ADDRESS"])
	}
	if first["SDO_API_PROXY_URL"] == second["SDO_API_PROXY_URL"] {
		t.Fatalf("agents in different Pods share proxy %q", first["SDO_API_PROXY_URL"])
	}
}

func TestProjectedScriptIdentityVolume(t *testing.T) {
	const namespace = "app"
	runtime := &Runtime{cluster: &Cluster{Token: "synthetic-token", Client: fake.NewSimpleClientset(
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "root-ca"}, Data: map[string]string{"ca.crt": "synthetic-ca", "unused": "ignored"}},
	)}}
	mode := int32(0600)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace},
		Spec: corev1.PodSpec{AutomountServiceAccountToken: new(bool), Volumes: []corev1.Volume{
			{Name: "identity", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
				DefaultMode: &mode,
				Sources: []corev1.VolumeProjection{
					{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token"}},
					{ConfigMap: &corev1.ConfigMapProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "root-ca"}, Items: []corev1.KeyToPath{{Key: "ca.crt", Path: "trust/ca.crt"}}}},
				},
			}}},
		}},
	}
	volumes, err := runtime.prepareVolumes(context.Background(), pod, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{"token": "synthetic-token", "trust/ca.crt": "synthetic-ca"} {
		path := filepath.Join(volumes["identity"], name)
		data, err := os.ReadFile(path)
		if err != nil || string(data) != want {
			t.Fatalf("%s = %q, %v; want %q", name, data, err, want)
		}
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatalf("file permissions for %s: %v, %v", name, info, err)
		}
	}
	if _, ok := volumes["__serviceaccount"]; ok {
		t.Fatal("explicit projected credentials must not enable automatic mounting")
	}
}

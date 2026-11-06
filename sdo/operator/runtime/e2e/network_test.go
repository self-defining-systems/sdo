//go:build integration && controller_e2e

package e2e

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// network is the smallest model of cluster networking the controller's probes
// need. Every application Pod gets a loopback twin (127.<slot>.x.y) and a real
// listener on each declared container port; every Service gets a loopback
// address (127.<slot+1>.x.y) whose listeners forward to a ready Pod's target port, or
// refuse the connection when the selected upstream cannot accept TCP.
// Names resolve through a hosts file the sandboxes bind over /etc/hosts.
type network struct {
	mu        sync.Mutex
	podSocks  map[types.UID][]net.Listener
	svcSocks  map[string]*forwarder
	svcIPs    map[string]string
	hostsPath string
	hosts     string
}

type forwarder struct {
	listener net.Listener
	target   atomic.Value // string; empty means no ready endpoint
}

func newNetwork(hostsPath string) *network {
	return &network{podSocks: map[types.UID][]net.Listener{}, svcSocks: map[string]*forwarder{}, svcIPs: map[string]string{}, hostsPath: hostsPath}
}

// endpointAddr is the address a Pod carries in Endpoints and EndpointSlices. Pods listen on loopback addresses
// (127.<slot>.x.y, which is also their status.podIP, so direct dials from agents work), but the API server rejects
// loopback addresses in Endpoints, so those objects carry the Pod's 10.244.x.y twin; targetRef names the Pod.
func endpointAddr(podIP string) string {
	parts := strings.Split(podIP, ".")
	return "10.244." + parts[2] + "." + parts[3]
}

func loopback(n int) string { return fmt.Sprintf("%d.%d", n>>8&255, n&255) }

// Every world on a host binds real listeners on these loopback addresses, so two
// worlds (parallel test processes, or agents sharing a host) must not share them:
// the second world's listeners failed silently and its probes reached the other
// world's applications or were refused. A process claims one slot, a pair of
// 127.x/16 ranges (Pods, Services), by holding a TCP port that only that slot's
// owner can bind; the kernel releases it when the process exits.
const (
	loopbackSlots    = 64
	loopbackSlotPort = 43100
)

var (
	loopbackOnce sync.Once
	loopbackPods int // second octet of Pod addresses
	loopbackSvcs int // second octet of Service addresses
	loopbackHold net.Listener
)

// claimSlot takes the first free slot at or after port base and returns the
// listener that holds it with the Pod and Service octets it owns.
func claimSlot(base int) (hold net.Listener, pods, services int, err error) {
	for slot := range loopbackSlots {
		l, listenErr := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", base+slot))
		if listenErr == nil {
			return l, 16 + 2*slot, 17 + 2*slot, nil
		}
	}
	return nil, 0, 0, fmt.Errorf("all %d loopback slots (127.0.0.1:%d..) are held by other controller end-to-end runs", loopbackSlots, base)
}

func claimLoopbackSlot() {
	loopbackOnce.Do(func() {
		var err error
		if loopbackHold, loopbackPods, loopbackSvcs, err = claimSlot(loopbackSlotPort); err != nil {
			panic(err)
		}
	})
}

// podLoopback is the n-th Pod address of this process's slot.
func podLoopback(n int) string {
	claimLoopbackSlot()
	return fmt.Sprintf("127.%d.%s", loopbackPods, loopback(n))
}

func serviceLoopback(n int) string {
	claimLoopbackSlot()
	return fmt.Sprintf("127.%d.%s", loopbackSvcs, loopback(n))
}

func (n *network) serviceIP(key string) string {
	if ip, ok := n.svcIPs[key]; ok {
		return ip
	}
	ip := serviceLoopback(len(n.svcIPs) + 1)
	n.svcIPs[key] = ip
	return ip
}

// reconcileNetwork brings listeners, forwarders and the hosts file in line with the API state.
func (k *Kubelet) reconcileNetwork(ctx context.Context) error {
	pods, err := k.cluster.Client.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	services, err := k.cluster.Client.CoreV1().Services("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	n := k.net
	n.mu.Lock()
	defer n.mu.Unlock()
	live := map[types.UID]bool{}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if ownerKind(pod) != "ReplicaSet" || pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning || pod.Status.PodIP == "" {
			continue
		}
		live[pod.UID] = true
		if _, ok := n.podSocks[pod.UID]; ok {
			continue
		}
		var socks []net.Listener
		bound := map[int32]bool{}
		for _, c := range pod.Spec.Containers {
			for _, port := range c.Ports {
				if bound[port.ContainerPort] {
					continue // a second container declaring the same port shares the first listener
				}
				l, err := net.Listen("tcp", net.JoinHostPort(pod.Status.PodIP, fmt.Sprint(port.ContainerPort)))
				if err != nil {
					for _, open := range socks {
						_ = open.Close()
					}
					return fmt.Errorf("listen for Pod %s/%s: %w", pod.Namespace, pod.Name, err)
				}
				bound[port.ContainerPort] = true
				go func() { _ = http.Serve(l, appHandler(c.Name)) }()
				socks = append(socks, l)
			}
		}
		n.podSocks[pod.UID] = socks
	}
	for uid, socks := range n.podSocks {
		if !live[uid] {
			for _, l := range socks {
				_ = l.Close()
			}
			delete(n.podSocks, uid)
		}
	}
	var hosts []string
	wanted := map[string]bool{}
	for i := range services.Items {
		service := &services.Items[i]
		if service.Spec.ClusterIP == "" || service.Spec.ClusterIP == "None" {
			continue
		}
		serviceKey := service.Namespace + "/" + service.Name
		ip := n.serviceIP(serviceKey)
		for _, name := range []string{service.Name + "." + service.Namespace + ".svc.cluster.local", service.Name + "." + service.Namespace + ".svc", service.Name + "." + service.Namespace} {
			hosts = append(hosts, ip+" "+name)
		}
		selector := labels.SelectorFromSet(service.Spec.Selector)
		for _, port := range service.Spec.Ports {
			key := fmt.Sprintf("%s:%d", serviceKey, port.Port)
			target := ""
			if len(service.Spec.Selector) > 0 {
				for p := range pods.Items {
					pod := &pods.Items[p]
					if pod.Namespace != service.Namespace || pod.DeletionTimestamp != nil || !podReady(pod) || !selector.Matches(labels.Set(pod.Labels)) {
						continue
					}
					number := port.Port
					if port.TargetPort.Type == intstr.Int && port.TargetPort.IntVal != 0 {
						number = port.TargetPort.IntVal
					} else if port.TargetPort.Type == intstr.String {
						for _, c := range pod.Spec.Containers {
							for _, cp := range c.Ports {
								if cp.Name == port.TargetPort.StrVal {
									number = cp.ContainerPort
								}
							}
						}
					}
					target = net.JoinHostPort(pod.Status.PodIP, fmt.Sprint(number))
					break
				}
			}
			// Accepting here and resetting after an upstream dial falsely makes
			// every TCP health probe succeed, even for a broken targetPort.
			// Only expose the virtual Service while its endpoint accepts TCP.
			upstream, err := net.DialTimeout("tcp", target, 100*time.Millisecond)
			if err != nil {
				continue
			}
			_ = upstream.Close()
			wanted[key] = true
			f, ok := n.svcSocks[key]
			if !ok {
				l, err := net.Listen("tcp", net.JoinHostPort(ip, fmt.Sprint(port.Port)))
				if err != nil {
					return fmt.Errorf("listen for Service %s port %d: %w", serviceKey, port.Port, err)
				}
				f = &forwarder{listener: l}
				n.svcSocks[key] = f
				go f.serve()
			}
			f.target.Store(target)
		}
	}
	for key, f := range n.svcSocks {
		if !wanted[key] {
			_ = f.listener.Close()
			delete(n.svcSocks, key)
		}
	}
	sort.Strings(hosts)
	content := "127.0.0.1 localhost\n" + strings.Join(hosts, "\n") + "\n"
	if content != n.hosts {
		n.hosts = content
		return os.WriteFile(n.hostsPath, []byte(content), 0o644) // in place: the sandboxes bind this inode
	}
	return nil
}

func (f *forwarder) serve() {
	for {
		client, err := f.listener.Accept()
		if err != nil {
			return
		}
		go func() {
			target, _ := f.target.Load().(string)
			if target == "" {
				reset(client)
				return
			}
			upstream, err := net.DialTimeout("tcp", target, 2*time.Second)
			if err != nil {
				reset(client)
				return
			}
			go func() { _, _ = io.Copy(upstream, client); _ = upstream.Close() }()
			_, _ = io.Copy(client, upstream)
			_ = client.Close()
		}()
	}
}

func reset(c net.Conn) {
	if tcp, ok := c.(*net.TCPConn); ok {
		_ = tcp.SetLinger(0)
	}
	_ = c.Close()
}

func appHandler(service string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) { fmt.Fprintln(w, "ready") })
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		fmt.Fprintln(w, service)
	})
	return mux
}

func (n *network) close() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, socks := range n.podSocks {
		for _, l := range socks {
			_ = l.Close()
		}
	}
	for _, f := range n.svcSocks {
		_ = f.listener.Close()
	}
}

var _ = filepath.Join
